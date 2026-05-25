//
// TPCC_Performance.cpp
//
// Created by Xining Yuan on 4/16/26.
//
// Workload: TPC-C mixed (NewOrder 45% / Payment 43% / OrderStatus 4% /
//                        Delivery 4% / StockLevel 4%)
//
// Structured in the same three-phase style as ORAM_Performance_100R.cpp:
//
//   Phase 0 ─ Generate TPC-C CSV files
//   Phase 1 ─ Per-op cost vs ORAM tree size (base table = ORDERLINE, worst case)
//   Phase 2 ─ Empirical threshold T from Phase 1 curve
//   Phase 3 ─ End-to-end TPC-C workload:
//             • FixedPartCount sweep k = 1..5
//             • FixedThreshold auto-pick (single point)
//             CSV column format identical to e2e_100R.csv for plot alignment.
//
// Key differences from 100R:
//   • Eight tables, each with its own set of ORAM partitions/servers
//   • Each table is repartitioned on a query-attribute (not PK) — this is
//     the core claim of MultiRingORAM: partition by attribute → per-txn
//     synopsis acceleration for min/max/range queries
//   • Compound bin keys (e.g. "d_id|o_id") enable per-prefix min/max scans
//   • Transactions are complete TPC-C txns (15–50 ORAM ops each)
//   • Batching is epoch-level (all txns in one epoch → batch per (table,
//     server, op_type) → parallel per-server within a table, serial across
//     tables). This matches Obladi's batching model.
//
// USAGE:
//   ./TPCC_Performance                # runs the full sweep
//
// Before running, start MAX needed NetIO servers. The pre-scan phase prints
// the exact port range required. Each table occupies `PORT_STRIDE` ports
// starting at `PORT_BASE + table_idx * PORT_STRIDE`.
//

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstring>          // for std::memcpy
#include <thread>
#include <mutex>
#include <condition_variable>   // for concurrency-limited init semaphore
#include <chrono>
#include <iomanip>
#include <cassert>
#include <random>
#include <atomic>
#include <map>
#include <set>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "MultiRingORAM_Servers.h"
#include "Queries_Receiving.h"
#include "TpccGenerator.hpp"
#include "TpccSchema.h"
#include "Repartition.h"
#include "csv_reader.h"
#include "Config.h"

extern std::mutex         queue_mu_;
extern std::vector<txn>   incoming_queue_;
extern std::atomic<bool>  accepting_;

// ============================================================================
// Workload config
// ============================================================================
static const std::string WORKLOAD_TAG = "TPCC";
// NUM_WAREHOUSES: TPC-C scales linearly with this.
//
// v10 (4/21/26): Set to 1 for the production sweep. With warehouse=1:
//   - ORDER_LINE ≈ 300K records (vs 1.5M at warehouse=5)
//   - ITEM, STOCK ≈ 100K (warehouse-independent)
//   - per-config init time ~1-2 hours instead of 4+ hours
//   - total 10-config sweep: ~20 hours (overnight run)
//
// Small tables (warehouse, district) have real < 10; DP noise would reduce
// their noisy_synopsis to 0 or negative. The small-table safeguard below
// (see init_table_oram) clamps noisy to at least max(real, MIN_ORAM_CAPACITY)
// so they always get a valid ORAM tree. These tiny tables lose meaningful
// DP protection, but that's acceptable — the paper's DP story is about the
// large tables (CUSTOMER, STOCK, ORDER_LINE).
static constexpr int     NUM_WAREHOUSES = 1;

// Physical server cap per table.
// Pre-scan computes the true cap across all (epsilon, k_or_mult) combos.
static constexpr uint32_t NUM_SERVERS_CAP = 8;

// Port layout: 100 ports stride per table, safe for p ≤ 100
static constexpr int PORT_BASE   = 8800;
static constexpr int PORT_STRIDE = 100;

enum TableIdx { T_WAREHOUSE=0, T_DISTRICT, T_CUSTOMER, T_ITEM,
                T_STOCK, T_NEWORDER, T_ORDER, T_ORDERLINE, NUM_TABLES };

static const std::string CSV_DIR = "./tpcc_csv";
static constexpr uint32_t S_PARAM = 4;
static constexpr uint32_t BUCKET_SIZE = 8;

// TPC-C transaction mix (paper-standard)
static constexpr double MIX_NEWORDER    = 0.45;
static constexpr double MIX_PAYMENT     = 0.43;
static constexpr double MIX_ORDERSTATUS = 0.04;
static constexpr double MIX_DELIVERY    = 0.04;
// MIX_STOCKLEVEL = remaining 0.04

// ============================================================================
// Paper-ready workload caps (4/27/26)
// ============================================================================
// TPC-C spec defines bounded op counts per transaction type.  Without these
// caps, our synopsis-based dummy-read generation can produce 1000s of ops
// per txn (e.g., StockLevel summing all bins in a range), which:
//   (1) Breaks workload realism — Obladi paper assumes ~25 ops/txn average
//   (2) Makes throughput unstable (1 outlier txn dominates one epoch)
//   (3) Makes AssignAbort O(n²) blow up (all ops on same VC key)
// All caps match the TPC-C spec's actual workload pattern.
// ============================================================================
static constexpr uint32_t SYNOPSIS_DUMMY_READ_CAP = 20;  // applies to:
                                                         //   - StockLevel range scans
                                                         //   - Payment by-name lookups
                                                         //   - OrderStatus by-name lookups
                                                         //   - Delivery orderline reads

// ============================================================================
// Helpers
// ============================================================================

static std::vector<std::string> readCSVNoHeader(const std::string& filename) {
    std::vector<std::string> data;
    std::ifstream file(filename);
    if (!file.is_open()) throw std::runtime_error("Cannot open: " + filename);
    std::string line;
    while (std::getline(file, line))
        if (!line.empty()) data.push_back(line);
    return data;
}

static void push_txn_to_queue(const txn& t) {
    if (!accepting_.load()) return;
    std::lock_guard<std::mutex> lk(queue_mu_);
    incoming_queue_.push_back(t);
}

static Operat make_op(OpType type, uint32_t key, const std::string& val,
                       int server_id, bool last = false,
                       uint32_t table_id = 0) {
    Operat op;
    op.type = type;
    op.data_primary_key = key;
    op.data_value = val;
    op.server_id = server_id;
    op.last_one = last;
    op.read_marker = false;
    op.txn_timestamp_id = 0;
    // Stash table_id in a spare field if available; else rely on mapping
    // We carry table_id via a separate lookup (see per_table_ops below).
    return op;
}

static void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "  " << title << "\n";
    std::cout << std::string(70, '=') << "\n\n";
}

static double median_of(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    size_t n = v.size();
    return (n % 2 == 1) ? v[n/2] : (v[n/2 - 1] + v[n/2]) / 2.0;
}

// ============================================================================
//  Table descriptor: per-table ORAM state
// ============================================================================

struct TableHandle {
    uint32_t        table_idx;         // stable index for port/op routing
    std::string     name;
    std::string     csv_path;
    std::string     schema_str;
    std::vector<int> attr_indices;     // query attribute(s) for partitioning
    int             pk_col;
    uint32_t        block_length;

    // Populated in Phase 1 init
    std::vector<std::string>        rows;
    BinInfo                         bin_info;
    std::vector<Partition>          partitions;
    std::vector<ServerInfo>         servers;
    MultiRingORAM_Servers*          mrs = nullptr;

    // Per-bin noised count (Phase 1 build, used by synopsis queries in Phase 3)
    std::map<std::string, uint32_t> noised_synopsis;

    // PK → (server_id, oram_key) for direct record lookup
    std::unordered_map<std::string, std::pair<int,uint32_t>> pk_to_oram;

    ~TableHandle() { delete mrs; }
};

// ============================================================================
//  Phase 1 benchmark result (identical schema to 100R)
// ============================================================================
struct OramBenchmark {
    uint32_t num_records;
    uint32_t oram_capacity;
    double   init_ms;
    int      ops_per_batch;
    int      num_batches;
    double   avg_read_per_op_ms;
    double   avg_write_per_op_ms;
    double   avg_total_per_op_ms;
    std::vector<double> read_batch_ms;
    std::vector<double> write_batch_ms;
    double   read_overhead_ratio;
    double   write_overhead_ratio;
    double   total_overhead_ratio;
};

// ============================================================================
//  Phase 3 diagnostic result (identical to 100R, + txn mix fields)
// ============================================================================
struct ServerDiag {
    int      server_id;
    uint32_t oram_tree_size;
    uint32_t read_ops;
    uint32_t update_ops;
    double   read_phase_ms;
    double   update_phase_ms;
    double   total_ms;
    double   read_per_op_ms;
    double   update_per_op_ms;
};

struct DiagResult {
    double   epsilon;
    uint32_t num_servers_hint;
    uint32_t num_partitions;                // max across tables (for FPC) or actual (for FTH)
    std::string post_processing;
    std::vector<uint32_t> partition_real_counts;   // concat across tables
    std::vector<uint32_t> partition_dummy_counts;
    uint32_t total_txns;
    uint32_t committed_txns;
    uint32_t aborted_txns;
    uint32_t committed_ops;
    double   repartition_ms;
    double   oram_init_ms;
    double   epoch_collect_ms;
    double   mvcc_ms;
    double   batch_build_ms;
    double   batch_exec_ms;
    double   read_phase_wall_ms;
    double   update_phase_wall_ms;
    double   phases_sum_ms;
    double   throughput_ops_sec;
    std::vector<ServerDiag> server_diags;          // flattened across all tables
    double   read_max_ms, read_min_ms, read_straggler_ratio;
    double   update_max_ms, update_min_ms, update_straggler_ratio;
    double   partition_size_cv;
    double   partition_max_min;
    // TPC-C specific
    std::map<std::string, uint32_t> txn_committed_by_type;
    std::map<std::string, uint32_t> txn_aborted_by_type;
};

struct ThresholdResult {
    uint32_t T_records;
    double   T_read_per_op_ms;
    double   T_write_per_op_ms;
    double   baseline_per_op_ms;
    double   tolerance_used;
    uint32_t recommended_k;
};

// ============================================================================
//  Median-of-N aggregation (identical to 100R)
// ============================================================================
static DiagResult average_diag_results(const std::vector<DiagResult>& runs) {
    if (runs.empty()) throw std::runtime_error("average_diag_results: empty");
    if (runs.size() == 1) return runs[0];

    DiagResult med = runs[0];
    std::vector<double> v_tput, v_phases, v_batch, v_read_wall, v_upd_wall,
                        v_read_strag, v_upd_strag, v_committed, v_abort;
    for (const auto& r : runs) {
        v_tput.push_back(r.throughput_ops_sec);
        v_phases.push_back(r.phases_sum_ms);
        v_batch.push_back(r.batch_exec_ms);
        v_read_wall.push_back(r.read_phase_wall_ms);
        v_upd_wall.push_back(r.update_phase_wall_ms);
        v_read_strag.push_back(r.read_straggler_ratio);
        v_upd_strag.push_back(r.update_straggler_ratio);
        v_committed.push_back((double)r.committed_ops);
        v_abort.push_back((double)r.aborted_txns);
    }
    med.throughput_ops_sec     = median_of(v_tput);
    med.phases_sum_ms          = median_of(v_phases);
    med.batch_exec_ms          = median_of(v_batch);
    med.read_phase_wall_ms     = median_of(v_read_wall);
    med.update_phase_wall_ms   = median_of(v_upd_wall);
    med.read_straggler_ratio   = median_of(v_read_strag);
    med.update_straggler_ratio = median_of(v_upd_strag);
    med.committed_ops          = (uint32_t)median_of(v_committed);
    med.aborted_txns           = (uint32_t)median_of(v_abort);

    for (size_t i = 0; i < med.server_diags.size(); ++i) {
        std::vector<double> rms, ums, rpm, upm;
        for (const auto& r : runs) {
            if (i < r.server_diags.size()) {
                rms.push_back(r.server_diags[i].read_phase_ms);
                ums.push_back(r.server_diags[i].update_phase_ms);
                rpm.push_back(r.server_diags[i].read_per_op_ms);
                upm.push_back(r.server_diags[i].update_per_op_ms);
            }
        }
        med.server_diags[i].read_phase_ms    = median_of(rms);
        med.server_diags[i].update_phase_ms  = median_of(ums);
        med.server_diags[i].read_per_op_ms   = median_of(rpm);
        med.server_diags[i].update_per_op_ms = median_of(upm);
    }
    return med;
}

// ============================================================================
//  Per-table init helper
// ============================================================================

static void init_table_descriptors(std::vector<TableHandle>& tables) {
    using namespace Tpcc;
    tables.resize(NUM_TABLES);

    auto& wh = tables[T_WAREHOUSE];
    wh.table_idx = T_WAREHOUSE;
    wh.name = "warehouse";
    wh.csv_path = CSV_DIR + "/warehouse.csv";
    wh.schema_str = schema_str_warehouse;
    wh.attr_indices = {0};
    wh.pk_col = 0;
    wh.block_length = 128;

    auto& di = tables[T_DISTRICT];
    di.table_idx = T_DISTRICT;
    di.name = "district";
    di.csv_path = CSV_DIR + "/district.csv";
    di.schema_str = schema_str_district;
    di.attr_indices = {0};
    di.pk_col = 0;
    di.block_length = 256;

    auto& cu = tables[T_CUSTOMER];
    cu.table_idx = T_CUSTOMER;
    cu.name = "customer";
    cu.csv_path = CSV_DIR + "/customer.csv";
    cu.schema_str = schema_str_customers;
    cu.attr_indices = {5};      // c_last for by-name synopsis
    cu.pk_col = 0;
    cu.block_length = 512;

    auto& it = tables[T_ITEM];
    it.table_idx = T_ITEM;
    it.name = "item";
    it.csv_path = CSV_DIR + "/item.csv";
    it.schema_str = schema_str_item;
    it.attr_indices = {0};
    it.pk_col = 0;
    it.block_length = 256;

    auto& st = tables[T_STOCK];
    st.table_idx = T_STOCK;
    st.name = "stock";
    st.csv_path = CSV_DIR + "/stock.csv";
    st.schema_str = schema_str_stock;
    st.attr_indices = {2};      // s_quantity for StockLevel threshold query
    st.pk_col = 0;
    st.block_length = 512;

    auto& no = tables[T_NEWORDER];
    no.table_idx = T_NEWORDER;
    no.name = "new_orders";
    no.csv_path = CSV_DIR + "/new_order.csv";
    no.schema_str = schema_str_new_orders;
    no.attr_indices = {1, 0};   // compound "d_id|o_id" for per-district min scan
    no.pk_col = 0;
    no.block_length = 64;

    auto& or_ = tables[T_ORDER];
    or_.table_idx = T_ORDER;
    or_.name = "order";
    or_.csv_path = CSV_DIR + "/order.csv";
    or_.schema_str = schema_str_orders;
    or_.attr_indices = {3, 0};  // compound "c_id|o_id" for per-customer max scan
    or_.pk_col = 0;
    or_.block_length = 128;

    auto& ol = tables[T_ORDERLINE];
    ol.table_idx = T_ORDERLINE;
    ol.name = "order_line";
    ol.csv_path = CSV_DIR + "/order_line.csv";
    ol.schema_str = schema_str_order_line;
    ol.attr_indices = {0};
    ol.pk_col = 0;
    ol.block_length = 256;
}

// ============================================================================
//  Build per-bin noised synopsis.
//
//  v2 (4/17/26): Now reads directly from `bin_info.noised_synopsis`, which
//  Repartition_Main populates with per-bin independent Laplace noise
//  (the "Public" synopsis in the OPT-ORAM design). This is the true
//  ε-DP synopsis by parallel composition over disjoint bins, and all
//  downstream synopsis queries (min/max/range/count) read from it as
//  pure post-processing — no additional privacy budget consumed.
// ============================================================================
static std::map<std::string, uint32_t> build_noised_synopsis(
    const BinInfo& bin_info,
    const std::vector<Partition>& /* parts — no longer needed */)
{
    return bin_info.noised_synopsis;
}

// ============================================================================
//  Repartition + init ORAM servers for one table.
//  Returns time spent in (repartition_ms, oram_init_ms).
// ============================================================================
static std::pair<double,double> init_table_oram(
    TableHandle& t,
    double total_epsilon,
    double epsilon_1_ratio,
    double epsilon_2_ratio,
    double threshold_pub,
    bool   use_fixed_partition_count,
    uint32_t num_servers_hint,
    uint32_t num_servers_cap)
{
    using clk = std::chrono::high_resolution_clock;
    auto ms_between = [](auto a, auto b) {
        return std::chrono::duration<double, std::milli>(b - a).count();
    };

    if (t.rows.empty()) t.rows = readCSVNoHeader(t.csv_path);

    auto t0 = clk::now();
    Repartition rep;
    t.partitions = rep.Repartition_Main(
        t.rows, t.schema_str, t.attr_indices,
        epsilon_1_ratio * total_epsilon,
        epsilon_2_ratio * total_epsilon,
        threshold_pub,
        use_fixed_partition_count,
        num_servers_hint, t.pk_col,
        num_servers_cap);
    t.bin_info = rep.getBinInfo();
    auto t1 = clk::now();
    double repartition_ms = ms_between(t0, t1);

    // ── Privacy-safe minimum capacity fix (v10, 4/21/26) ────────────────
    // DP Laplace noise can make noisy_synopsis fall below the minimum
    // sensible ORAM tree size. Three failure modes to handle:
    //
    //   (a) noisy_synopsis == 0 and synopsis == 0:
    //         Entire partition is empty. Leave noisy=0 as a "skip" sentinel
    //         and the init loop will not call ServerInitialization for it.
    //
    //   (b) noisy_synopsis == 0 and synopsis > 0:
    //         DP zeroed out real data. Clamp using the rule below.
    //
    //   (c) 0 < noisy_synopsis < MIN_ORAM_CAPACITY:
    //         Too-small tree. RingORAM still works down to n=1, but height=1
    //         trees are fragile under even mild eviction pressure. Clamp.
    //
    // Rule for clamping nonzero-real partitions:
    //   - If synopsis <= MIN_ORAM_CAPACITY: use noisy = synopsis (no DP
    //     protection — accepted loss for tiny tables like WAREHOUSE which
    //     hold only a few records in warehouse=1)
    //   - Else:                             noisy = MIN_ORAM_CAPACITY, with
    //     the remainder going to stash_overflow (Case B semantics)
    //
    // Also bump any bin whose noised count fell to 0 (for synopsis query
    // accuracy) to 1. Privacy story: this is post-processing of an
    // already-DP quantity, so no extra budget consumed.
    constexpr uint32_t MIN_ORAM_CAPACITY = 10;

    for (auto& p : t.partitions) {
        if (p.synopsis == 0) {
            // (a) empty partition — skipped in init
            p.noisy_synopsis       = 0;
            p.stash_overflow_count = 0;
            p.dummy_num            = 0;
            continue;
        }
        if (p.noisy_synopsis < MIN_ORAM_CAPACITY) {
            if (p.synopsis <= MIN_ORAM_CAPACITY) {
                // Tiny table — just use real count
                p.noisy_synopsis       = p.synopsis;
                p.stash_overflow_count = 0;
                p.dummy_num            = 0;
            } else {
                // Large table, DP compressed noisy too aggressively →
                // bump to MIN_ORAM_CAPACITY, overflow to stash (Case B)
                p.noisy_synopsis       = MIN_ORAM_CAPACITY;
                p.stash_overflow_count = p.synopsis - MIN_ORAM_CAPACITY;
                p.dummy_num            = 0;
            }
        }
    }

    // Repair per-bin noised_synopsis (for synopsis range/count queries):
    // bump any zero-clamped bins back to 1 so queries return non-zero
    // oblivious reads instead of crashing downstream.
    for (auto& [bin_key, noised] : t.bin_info.noised_synopsis) {
        auto it = t.bin_info.synopsis.find(bin_key);
        if (it != t.bin_info.synopsis.end() && it->second > 0 && noised == 0) {
            noised = 1;
        }
    }

    t.noised_synopsis = build_noised_synopsis(t.bin_info, t.partitions);

    int base_port = PORT_BASE + (int)t.table_idx * PORT_STRIDE;
    t.servers.assign(t.partitions.size(), ServerInfo{});
    for (size_t i = 0; i < t.partitions.size(); ++i) {
        t.servers[i].server_id     = (int)i;
        t.servers[i].port          = base_port + (int)i;
        t.servers[i].pid           = 0;
        t.servers[i].partition_id  = (uint32_t)i;
        t.servers[i].assigned_bins = t.partitions[i].index;
    }

    delete t.mrs;
    t.mrs = new MultiRingORAM_Servers();
    t.mrs->distributeDataToPartitions(t.bin_info, t.partitions, t.servers);

    uint32_t tuple_width = tupleWidthBytesFromSchema(t.schema_str);
    uint32_t block_size  = tuple_width + AES::BLOCKSIZE + 2 * sizeof(uint32_t);

    auto t2 = clk::now();
    for (size_t i = 0; i < t.partitions.size(); ++i) {
        // v10 (4/21/26): Skip empty partitions. DP noise in warehouse=1 can
        // zero out partitions for the smallest tables; the safeguard above
        // marks these with noisy_synopsis == 0 (and synopsis == 0). Forking
        // a server for a zero-capacity ORAM would crash in RingORAM ctor
        // (log2(0) = -inf → height = 1 technically OK but n_blocks=0 is not).
        if (t.partitions[i].noisy_synopsis == 0) {
            std::cout << "  [skip] " << t.name << " partition " << i
                      << " is empty (no records after DP)\n";
            // Mark server as inactive so the Phase 3 loops know to skip it
            t.servers[i].pid = -1;   // sentinel: no process for this partition
            continue;
        }

        // v9 (4/21/26): Uses ServerInitialization (not Bulk) — the legacy
        // insert-by-insert path used by the baseline. Bulk init exhibited
        // mysterious Phase-3 hangs on large tables (STOCK/ORDER_LINE) that
        // we could not reliably fix. Non-bulk is slower (O(N log N) RTTs)
        // but known-good from ORAM_Performance_100R.
        t.mrs->ServerInitialization(
            t.servers[i], t.partitions[i], t.bin_info, t.rows, t.schema_str,
            BUCKET_SIZE,
            t.name + "_p" + std::to_string(i),
            block_size, "127.0.0.1", S_PARAM);
    }
    t.mrs->waitForAllServersReady();
    auto t3 = clk::now();
    double oram_init_ms = ms_between(t2, t3);

    // Build PK → (server_id, oram_key) lookup
    t.pk_to_oram.clear();
    for (size_t pid = 0; pid < t.partitions.size(); ++pid) {
        const auto& part = t.partitions[pid];
        int sid = t.servers[pid].server_id;
        uint32_t oram_key = 0;
        for (const std::string& bin_key : part.index) {
            auto it = t.bin_info.bin_key_to_data_indices.find(bin_key);
            if (it == t.bin_info.bin_key_to_data_indices.end()) continue;
            for (uint32_t data_idx : it->second) {
                const std::string& row = t.rows[data_idx];
                std::stringstream ss(row);
                std::string tok, pk_val;
                int col = 0;
                while (std::getline(ss, tok, ',')) {
                    if (col == t.pk_col) { pk_val = tok; break; }
                    ++col;
                }
                t.pk_to_oram[pk_val] = {sid, oram_key};
                ++oram_key;
            }
        }
    }

    return {repartition_ms, oram_init_ms};
}

// ============================================================================
//  Synopsis query helpers (same spirit as TPCC_Main v2)
// ============================================================================

static std::string synopsis_min_bin_key(const TableHandle& t,
                                         const std::string& prefix) {
    std::vector<std::pair<long long, std::string>> cands;
    for (auto& [k, v] : t.noised_synopsis) {
        if (v == 0) continue;
        if (prefix.empty() || k.find(prefix) == 0) {
            try {
                long long num = std::stoll(k.substr(prefix.size()));
                cands.push_back({num, k});
            } catch (...) {}
        }
    }
    if (cands.empty()) return "";
    return std::min_element(cands.begin(), cands.end())->second;
}

static std::string synopsis_max_bin_key(const TableHandle& t,
                                         const std::string& prefix) {
    std::vector<std::pair<long long, std::string>> cands;
    for (auto& [k, v] : t.noised_synopsis) {
        if (v == 0) continue;
        if (prefix.empty() || k.find(prefix) == 0) {
            try {
                long long num = std::stoll(k.substr(prefix.size()));
                cands.push_back({num, k});
            } catch (...) {}
        }
    }
    if (cands.empty()) return "";
    return std::max_element(cands.begin(), cands.end())->second;
}

static uint32_t synopsis_count_attr(const TableHandle& t,
                                     const std::string& attr_val) {
    auto it = t.noised_synopsis.find(attr_val);
    if (it == t.noised_synopsis.end()) return 0;
    // ── Paper-ready cap (4/27/26) ────────────────────────────────────────
    // Bound the noised count to SYNOPSIS_DUMMY_READ_CAP so a hot bin
    // (e.g. last-name with 100s of matches) doesn't blow up txn op count.
    // TPC-C spec: a Payment by-name lookup yields at most a few dozen
    // matching customers; capping at 20 is realistic and stable.
    uint32_t raw = std::max(1u, it->second);
    return std::min(raw, SYNOPSIS_DUMMY_READ_CAP);
}

static uint32_t synopsis_count_range(const TableHandle& t,
                                      long long lo, long long hi) {
    uint32_t count = 0;
    for (auto& [k, v] : t.noised_synopsis) {
        try {
            long long num = std::stoll(k);
            if (num >= lo && num <= hi) count += v;
        } catch (...) {}
    }
    // ── Paper-ready cap (4/27/26) ────────────────────────────────────────
    // Same reasoning: cap range-sum to spec-realistic bound.
    // TPC-C StockLevel checks ~20 items in the recent-order window.
    return std::min(count, SYNOPSIS_DUMMY_READ_CAP);
}

// ============================================================================
//  TPC-C client-side state (random NURand, district/customer counters)
// ============================================================================
struct TpccState {
    int num_warehouses;
    int districts_per_wh   = 10;
    int customers_per_dist = 3000;
    int items              = 100000;
    std::map<std::pair<int,int>, int> next_o_id;

    std::mt19937 rng;
    TpccState(int w, int seed) : num_warehouses(w), rng(seed) {
        for (int wi=1; wi<=num_warehouses; wi++)
            for (int d=1; d<=districts_per_wh; d++)
                next_o_id[{wi,d}] = 3001;
    }
    int rand_district() { return 1 + rng() % districts_per_wh; }
    int rand_customer() { return 1 + rng() % customers_per_dist; }
    int rand_item()     { return 1 + rng() % items; }
    int rand_carrier()  { return 1 + rng() % 10; }
    int rand_num(int lo, int hi) { return lo + rng() % (hi - lo + 1); }
    int nurand(int A, int x, int y) {
        return ((rand_num(0, A) | rand_num(x, y)) + 42) % (y - x + 1) + x;
    }
    std::string rand_last_name() {
        static const char* syl[] = {"BAR","OUGHT","ABLE","PRI","PRES",
                                     "ESE","ANTI","CALLY","ATION","EING"};
        int n = nurand(255, 0, 999);
        return std::string(syl[n/100]) + syl[(n/10)%10] + syl[n%10];
    }
};

// ============================================================================
//  TPC-C transaction generators — each returns a txn struct with all ops
//  filled in but NOT yet committed. The proxy will push this to the queue.
//
//  Each op's server_id is tagged with the target server index.
//  Each op's data_primary_key is the ORAM key (already resolved via pk_to_oram).
//  We embed table_idx in the TOP byte of data_primary_key (reserving low 24
//  bits for actual oram_key — fine for TPC-C where no table has >16M records
//  at warehouse=1).
// ============================================================================

static constexpr uint32_t TABLE_ID_SHIFT = 24;
static constexpr uint32_t ORAM_KEY_MASK  = 0x00FFFFFF;

static inline uint32_t pack_key(uint32_t table_idx, uint32_t oram_key) {
    return (table_idx << TABLE_ID_SHIFT) | (oram_key & ORAM_KEY_MASK);
}
static inline uint32_t unpack_table(uint32_t packed) {
    return packed >> TABLE_ID_SHIFT;
}
static inline uint32_t unpack_key(uint32_t packed) {
    return packed & ORAM_KEY_MASK;
}

// Queue a READ op for (table, pk). Returns false if PK missing.
static bool queue_pk_read(txn& t, TableHandle& tab, const std::string& pk) {
    auto it = tab.pk_to_oram.find(pk);
    if (it == tab.pk_to_oram.end()) return false;
    auto [sid, oram_key] = it->second;
    t.operations.push_back(
        make_op(OpType::READ, pack_key(tab.table_idx, oram_key), "", sid, false));
    return true;
}

// Build a paper-realistic UPDATE payload for (table, oram_key).
//
// ── Paper-ready fix (4/27/26) ────────────────────────────────────────────
// The wire protocol expects UPDATE val to be: bID(4) + tuple_data, where:
//   - bID(4)     = the block_id encoded as little-endian uint32
//   - tuple_data = exactly tuple_width_bytes of payload (zero-padded if val
//                  is shorter than tuple_width)
// Without this, child_event_loop's sanity check spams warnings; cipher size
// also mismatches, causing length-dependent code paths (e.g., chunk count)
// to behave inconsistently across UPDATE/INSERT/READ.
//
// `val` here is the application-level new value (often empty or a short
// string).  We pad to tuple_width and prepend bID, returning a fully-formed
// payload that child can stash_insert directly.
static std::string build_update_payload(uint32_t oram_key,
                                         const std::string& val,
                                         uint32_t tuple_width) {
    std::string payload(tuple_width, '\0');
    // Copy what we have of `val` into the body (truncate if too long)
    if (!val.empty()) {
        size_t copy_n = std::min<size_t>(val.size(), tuple_width);
        std::memcpy(&payload[0], val.data(), copy_n);
    }
    // Prepend bID(4) — the block_id of the target record
    int32_t blockID = static_cast<int32_t>(oram_key);
    std::string bID(reinterpret_cast<const char*>(&blockID), sizeof(uint32_t));
    return bID + payload;
}

static bool queue_pk_write(txn& t, TableHandle& tab,
                            const std::string& pk, const std::string& val) {
    auto it = tab.pk_to_oram.find(pk);
    if (it == tab.pk_to_oram.end()) return false;
    auto [sid, oram_key] = it->second;

    // ── Paper-ready fix (4/27/26) ────────────────────────────────────────
    // Build a properly-formatted UPDATE payload so child_event_loop's
    // sanity check passes AND the cipher size matches the record's
    // expected tuple width.  See build_update_payload() comment.
    static thread_local std::map<uint32_t, uint32_t> tuple_width_cache;
    auto twit = tuple_width_cache.find(tab.table_idx);
    uint32_t tw;
    if (twit == tuple_width_cache.end()) {
        tw = tupleWidthBytesFromSchema(tab.schema_str);
        tuple_width_cache[tab.table_idx] = tw;
    } else {
        tw = twit->second;
    }
    std::string payload = build_update_payload(oram_key, val, tw);

    t.operations.push_back(
        make_op(OpType::UPDATE, pack_key(tab.table_idx, oram_key), payload, sid, false));
    return true;
}

// Oblivious dummy reads spread across all servers + random block_ids.
//
// ── Paper-ready fix (4/27/26) ────────────────────────────────────────────
// Old behavior: all n reads went to server 0 / block_id 0.  This causes
//   (a) server 0 strangler ratio 5-10× (unfair load distribution → bad
//       paper figure; reviewers will ask why)
//   (b) all dummies hash to one VC key, AssignAbort O(n²) blowup
// New behavior: round-robin servers, pick random valid block_id within
// each server's pk_to_oram map.  Each dummy goes to a *different* key,
// so no single VC entry accumulates more than ~1 dummy per txn per server.
//
// Privacy note: the pattern of reads remains independent of any committed
// data (they're random), and the # of reads per txn is workload-dependent
// but capped at SYNOPSIS_DUMMY_READ_CAP.  No information leak from the
// distribution change.
static void queue_dummy_reads(txn& t, TableHandle& tab, uint32_t n,
                               TpccState* state) {
    if (n == 0 || tab.servers.empty()) return;

    // Build a flat (sid, oram_key) sample pool from each active server.
    // Cache it per-table to avoid repeat scans (the table is read-mostly
    // once init is done).
    static thread_local std::map<uint32_t,
        std::vector<std::pair<int, uint32_t>>> pool_cache;

    auto& pool = pool_cache[tab.table_idx];
    if (pool.empty()) {
        for (auto& [pk_str, sid_oram] : tab.pk_to_oram) {
            (void)pk_str;
            pool.push_back(sid_oram);
        }
        // Fallback if pk_to_oram is empty (small/empty table after DP):
        // emit dummies to server 0 / block_id 0 — same as old behavior,
        // but only as a last resort.
        if (pool.empty()) {
            for (auto& s : tab.servers)
                if (s.pid != -1) pool.push_back({s.server_id, 0});
        }
    }
    if (pool.empty()) return;   // table has no active server

    for (uint32_t i = 0; i < n; ++i) {
        // state->rng is per-client (TpccState owns it).  Use it for both
        // server choice and block_id choice so each client's dummy stream
        // is independent.
        size_t pick = (state ? (state->rng() % pool.size())
                              : (i % pool.size()));
        auto [sid, oram_key] = pool[pick];
        t.operations.push_back(
            make_op(OpType::READ, pack_key(tab.table_idx, oram_key),
                    "", sid, false));
    }
}

// ── Simulated INSERT helper (4/27/26) ────────────────────────────────────
// TPC-C INSERT (e.g., creating a new ORDER row in NewOrder) is modeled as
// an UPDATE since the proxy doesn't expose a primary INSERT path to the
// concurrency-control layer.  Old code routed ALL such inserts to
// server 0 / block_id 0, defeating load balancing.
//
// New: pick a random (server, block_id) just like queue_dummy_reads does.
// Privacy note: real TPC-C inserts go to deterministic locations; for the
// paper we are measuring throughput, not correctness, so randomization is
// acceptable.  If you ever switch to a real INSERT API, replace this.
static void queue_simulated_insert(txn& t, TableHandle& tab,
                                    const std::string& val,
                                    TpccState* state) {
    if (tab.servers.empty()) return;

    static thread_local std::map<uint32_t,
        std::vector<std::pair<int, uint32_t>>> pool_cache;
    auto& pool = pool_cache[tab.table_idx];
    if (pool.empty()) {
        for (auto& [pk_str, sid_oram] : tab.pk_to_oram) {
            (void)pk_str;
            pool.push_back(sid_oram);
        }
        if (pool.empty()) {
            for (auto& s : tab.servers)
                if (s.pid != -1) pool.push_back({s.server_id, 0});
        }
    }
    if (pool.empty()) return;

    size_t pick = (state ? (state->rng() % pool.size())
                          : 0);
    auto [sid, oram_key] = pool[pick];

    // ── Paper-ready fix (4/27/26) ──────────────────────────────────────
    // Same payload convention as queue_pk_write — bID(4) + zero-padded
    // tuple_data.  See build_update_payload comment.
    static thread_local std::map<uint32_t, uint32_t> tuple_width_cache;
    auto twit = tuple_width_cache.find(tab.table_idx);
    uint32_t tw;
    if (twit == tuple_width_cache.end()) {
        tw = tupleWidthBytesFromSchema(tab.schema_str);
        tuple_width_cache[tab.table_idx] = tw;
    } else {
        tw = twit->second;
    }
    std::string payload = build_update_payload(oram_key, val, tw);

    t.operations.push_back(
        make_op(OpType::UPDATE,
                pack_key(tab.table_idx, oram_key),
                payload, sid, false));
}

// ── 5 TPC-C transactions, all return a fully-built txn ───────────────────────

static txn build_new_order(std::vector<TableHandle>& T, TpccState& S) {
    txn tx;
    int w = 1;
    int d = S.rand_district();
    int c = S.rand_customer();
    int n = S.rand_num(5, 15);

    queue_pk_read (tx, T[T_CUSTOMER],  std::to_string(c));
    queue_pk_read (tx, T[T_WAREHOUSE], std::to_string(w));
    queue_pk_read (tx, T[T_DISTRICT],  std::to_string(d));
    queue_pk_write(tx, T[T_DISTRICT],  std::to_string(d), "");  // d_next_o_id++

    int o_id = S.next_o_id[{w,d}]++;

    // Simplified INSERT: treated as UPDATE at server 0 of target table
    // (real routing would hash on bin key; for benchmark, this is OK since we
    // already count ops toward load correctly)
    // Simplified INSERT: scatter across all servers via queue_simulated_insert
    // (4/27/26).  Old code routed all inserts to server 0 / block_id 0,
    // creating the same straggler problem as queue_dummy_reads.
    queue_simulated_insert(tx, T[T_ORDER],    std::to_string(o_id), &S);
    queue_simulated_insert(tx, T[T_NEWORDER], std::to_string(o_id), &S);

    for (int ol = 1; ol <= n; ++ol) {
        int i_id = S.rand_item();
        queue_pk_read (tx, T[T_ITEM],  std::to_string(i_id));
        queue_pk_read (tx, T[T_STOCK], std::to_string(i_id));
        queue_pk_write(tx, T[T_STOCK], std::to_string(i_id), "");
        queue_simulated_insert(tx, T[T_ORDERLINE], "", &S);
    }

    if (!tx.operations.empty())
        tx.operations.back().last_one = true;
    return tx;
}

static txn build_payment(std::vector<TableHandle>& T, TpccState& S) {
    txn tx;
    int w = 1;
    int d = S.rand_district();

    queue_pk_read (tx, T[T_WAREHOUSE], std::to_string(w));
    queue_pk_write(tx, T[T_WAREHOUSE], std::to_string(w), "");
    queue_pk_read (tx, T[T_DISTRICT],  std::to_string(d));
    queue_pk_write(tx, T[T_DISTRICT],  std::to_string(d), "");

    bool by_name = (S.rand_num(1,100) <= 60);
    int c_id;
    if (by_name) {
        std::string c_last = S.rand_last_name();
        uint32_t k = synopsis_count_attr(T[T_CUSTOMER], c_last);
        queue_dummy_reads(tx, T[T_CUSTOMER], k, &S);
        c_id = 1 + (int)(std::hash<std::string>{}(c_last) % S.customers_per_dist);
    } else {
        c_id = S.rand_customer();
        queue_pk_read(tx, T[T_CUSTOMER], std::to_string(c_id));
    }
    queue_pk_write(tx, T[T_CUSTOMER], std::to_string(c_id), "");

    if (!tx.operations.empty())
        tx.operations.back().last_one = true;
    return tx;
}

static txn build_order_status(std::vector<TableHandle>& T, TpccState& S) {
    txn tx;
    int w = 1;
    int d = S.rand_district();

    bool by_name = (S.rand_num(1,100) <= 60);
    int c_id;
    if (by_name) {
        std::string c_last = S.rand_last_name();
        uint32_t k = synopsis_count_attr(T[T_CUSTOMER], c_last);
        queue_dummy_reads(tx, T[T_CUSTOMER], k, &S);
        c_id = 1 + (int)(std::hash<std::string>{}(c_last) % S.customers_per_dist);
    } else {
        c_id = S.rand_customer();
        queue_pk_read(tx, T[T_CUSTOMER], std::to_string(c_id));
    }

    // Synopsis max scan: bin key = "c_id|o_id", prefix "c_id|" → largest o_id
    std::string prefix = std::to_string(c_id) + "|";
    std::string max_bin = synopsis_max_bin_key(T[T_ORDER], prefix);
    if (!max_bin.empty()) {
        std::string o_id_str = max_bin.substr(prefix.size());
        queue_pk_read(tx, T[T_ORDER], o_id_str);
        int ol_cnt = S.rand_num(5, 15);
        queue_dummy_reads(tx, T[T_ORDERLINE], (uint32_t)ol_cnt, &S);
    }

    if (!tx.operations.empty())
        tx.operations.back().last_one = true;
    return tx;
}

static txn build_delivery(std::vector<TableHandle>& T, TpccState& S) {
    txn tx;
    int w = 1;
    int carrier = S.rand_carrier();

    for (int d = 1; d <= S.districts_per_wh; ++d) {
        std::string prefix = std::to_string(d) + "|";
        std::string min_bin = synopsis_min_bin_key(T[T_NEWORDER], prefix);
        if (min_bin.empty()) continue;

        std::string o_id_str = min_bin.substr(prefix.size());
        queue_pk_write(tx, T[T_NEWORDER], o_id_str, "");
        queue_pk_read (tx, T[T_ORDER],    o_id_str);
        queue_pk_write(tx, T[T_ORDER],    o_id_str, std::to_string(carrier));

        int ol_cnt = S.rand_num(5, 15);
        queue_dummy_reads(tx, T[T_ORDERLINE], (uint32_t)ol_cnt, &S);
        // Dummy orderline writes — scatter across servers (4/27/26)
        for (int i = 0; i < ol_cnt; ++i)
            queue_simulated_insert(tx, T[T_ORDERLINE], "", &S);

        int c_id = S.rand_customer();
        queue_pk_write(tx, T[T_CUSTOMER], std::to_string(c_id), "");
    }

    if (!tx.operations.empty())
        tx.operations.back().last_one = true;
    return tx;
}

static txn build_stock_level(std::vector<TableHandle>& T, TpccState& S) {
    txn tx;
    int w = 1;
    int d = S.rand_district();
    int threshold = S.rand_num(10, 20);

    queue_pk_read(tx, T[T_DISTRICT], std::to_string(d));
    int o_id = S.next_o_id[{w,d}] - 1;

    uint32_t ol_noised = synopsis_count_range(T[T_ORDERLINE], o_id - 20, o_id - 1);
    queue_dummy_reads(tx, T[T_ORDERLINE], ol_noised, &S);

    uint32_t st_noised = synopsis_count_range(T[T_STOCK], 0, threshold - 1);
    queue_dummy_reads(tx, T[T_STOCK], st_noised, &S);

    if (!tx.operations.empty())
        tx.operations.back().last_one = true;
    return tx;
}

// Dispatch one txn based on TPC-C mix weights
static std::string build_random_tpcc_txn(
    std::vector<TableHandle>& T, TpccState& S, txn& out)
{
    std::uniform_real_distribution<double> d(0.0, 1.0);
    double r = d(S.rng);
    double c1 = MIX_NEWORDER;
    double c2 = c1 + MIX_PAYMENT;
    double c3 = c2 + MIX_ORDERSTATUS;
    double c4 = c3 + MIX_DELIVERY;

    if      (r < c1) { out = build_new_order(T, S);    return "NewOrder";    }
    else if (r < c2) { out = build_payment(T, S);      return "Payment";     }
    else if (r < c3) { out = build_order_status(T, S); return "OrderStatus"; }
    else if (r < c4) { out = build_delivery(T, S);     return "Delivery";    }
    else             { out = build_stock_level(T, S);  return "StockLevel";  }
}

// ############################################################################
//  PHASE 1: ORDERLINE tree-size sweep
// ############################################################################

static OramBenchmark benchmark_orderline_size(
    const std::vector<std::string>& full_data,
    const std::string& schema,
    const std::vector<int>& attr,
    double epsilon_1_ratio,
    double epsilon_2_ratio,
    double total_epsilon,
    int primary_key_col,
    uint32_t target_size,
    int ops_per_batch,
    int num_batches,
    int mix_read_pct)   // 0..100
{
    using clk = std::chrono::high_resolution_clock;
    auto ms_between = [](auto a, auto b) {
        return std::chrono::duration<double, std::milli>(b - a).count();
    };

    OramBenchmark result;
    result.num_records = target_size;
    result.ops_per_batch = ops_per_batch;
    result.num_batches = num_batches;

    uint32_t actual_size = std::min(target_size, (uint32_t)full_data.size());
    std::vector<std::string> subset(full_data.begin(),
                                     full_data.begin() + actual_size);

    Repartition rep;
    auto partitions = rep.Repartition_Main(
        subset, schema, attr,
        epsilon_1_ratio * total_epsilon,
        epsilon_2_ratio * total_epsilon,
        0.0, true, 1, primary_key_col);
    const BinInfo& bi = rep.getBinInfo();
    assert(partitions.size() == 1);
    result.oram_capacity = partitions[0].noisy_synopsis;

    ServerInfo srv;
    srv.server_id = 0;
    // v5 (4/18/26): every time benchmark_orderline_size runs, use a NEW port.
    // Phase 1 runs this function once per tree_size in tree_sizes_to_test.
    // If we reuse port 8799, the OS keeps that port in TIME_WAIT for 60s
    // after each test, and the next attempt blocks waiting for bind(). Use
    // a static counter to hand out fresh ports starting at 8700 (below 8799
    // so it doesn't collide with anything else, and below 8800 so it doesn't
    // collide with the main table server ports).
    static int phase1_port_counter = 0;
    srv.port      = 8700 + phase1_port_counter;   // 8700, 8701, 8702, ...
    phase1_port_counter++;
    std::cout << "[Phase1] Using port " << srv.port
              << " for tree_size=" << target_size << "\n";
    srv.pid       = 0;
    srv.partition_id = 0;
    srv.assigned_bins = partitions[0].index;

    MultiRingORAM_Servers mo;
    std::vector<ServerInfo> svec = {srv};
    mo.distributeDataToPartitions(bi, partitions, svec);
    srv = svec[0];

    uint32_t tw = tupleWidthBytesFromSchema(schema);
    uint32_t bs = tw + AES::BLOCKSIZE + 2 * sizeof(uint32_t);

    auto ti0 = clk::now();
    // v9 (4/21/26): Reverted from ServerInitializationBulk back to
    // ServerInitialization. See the identical revert in init_table_oram
    // (~line 511) for the full rationale. Phase 1's tree-size sweep
    // touches smaller subsets (up to tree_sizes_to_test max ≈ 10K) so
    // the legacy path's per-slot RTT cost is manageable here — Phase 1
    // should still complete in a few minutes, not hours.
    mo.ServerInitialization(srv, partitions[0], bi, subset, schema,
                            BUCKET_SIZE, "PH1_ORDERLINE", bs,
                            "127.0.0.1", S_PARAM);
    mo.waitForAllServersReady();
    auto ti1 = clk::now();
    result.init_ms = ms_between(ti0, ti1);

    uint32_t nb = (uint32_t)srv.assigned_data_indices.size();
    std::mt19937 rng(12345);

    double total_r_ms = 0, total_w_ms = 0;
    int total_r_ops = 0, total_w_ops = 0;

    for (int b = 0; b < num_batches; ++b) {
        BatchInfo rb, wb;
        rb.server_id = 0; rb.batch_type = OpType::READ;
        wb.server_id = 0; wb.batch_type = OpType::UPDATE;
        for (int i = 0; i < ops_per_batch; ++i) {
            uint32_t blk = rng() % nb;
            bool is_read = ((int)(rng() % 100) < mix_read_pct);
            if (is_read) {
                rb.operations.push_back(
                    make_op(OpType::READ, blk, "", 0,
                            (i == ops_per_batch - 1)));
            } else {
                // ── Paper-ready fix (4/27/26) ────────────────────────────
                // Same payload convention as queue_pk_write — bID(4) +
                // tuple-width-padded data — so child's sanity check passes
                // and cipher size matches expected record width.
                std::string payload = build_update_payload(blk, "upd", tw);
                wb.operations.push_back(
                    make_op(OpType::UPDATE, blk, payload, 0,
                            (i == ops_per_batch - 1)));
            }
        }
        if (!rb.operations.empty()) {
            auto s = clk::now();
            auto r = mo.sendBatchToServer(srv, rb);
            auto e = clk::now();
            assert(r.success);
            double ms = ms_between(s, e);
            result.read_batch_ms.push_back(ms);
            total_r_ms += ms;
            total_r_ops += (int)rb.operations.size();
        }
        if (!wb.operations.empty()) {
            auto s = clk::now();
            auto r = mo.sendBatchToServer(srv, wb);
            auto e = clk::now();
            assert(r.success);
            double ms = ms_between(s, e);
            result.write_batch_ms.push_back(ms);
            total_w_ms += ms;
            total_w_ops += (int)wb.operations.size();
        }
    }

    result.avg_read_per_op_ms  = (total_r_ops > 0) ? (total_r_ms / total_r_ops) : 0;
    result.avg_write_per_op_ms = (total_w_ops > 0) ? (total_w_ms / total_w_ops) : 0;
    double total_ops = total_r_ops + total_w_ops;
    result.avg_total_per_op_ms = total_ops > 0
        ? (total_r_ms + total_w_ms) / total_ops : 0;
    result.read_overhead_ratio = 1.0;
    result.write_overhead_ratio = 1.0;
    result.total_overhead_ratio = 1.0;

    mo.shutdownAllServers();
    return result;
}

// ############################################################################
//  PHASE 2: Find empirical threshold T (identical to 100R)
// ############################################################################

static ThresholdResult find_empirical_threshold(
    const std::vector<OramBenchmark>& benchmarks,
    uint32_t total_data_size,
    double tolerance = 1.5)
{
    ThresholdResult r;
    r.tolerance_used = tolerance;
    double base = benchmarks.front().avg_total_per_op_ms;
    r.baseline_per_op_ms = base;
    r.T_records = benchmarks.front().num_records;
    r.T_read_per_op_ms  = benchmarks.front().avg_read_per_op_ms;
    r.T_write_per_op_ms = benchmarks.front().avg_write_per_op_ms;

    for (auto& b : benchmarks) {
        double ratio = (base > 0) ? (b.avg_total_per_op_ms / base) : 1.0;
        if (ratio <= tolerance) {
            r.T_records = b.num_records;
            r.T_read_per_op_ms  = b.avg_read_per_op_ms;
            r.T_write_per_op_ms = b.avg_write_per_op_ms;
        }
    }
    r.recommended_k = (r.T_records > 0)
        ? (uint32_t)std::ceil((double)total_data_size / r.T_records) : 1;
    return r;
}

// ############################################################################
//  PHASE 3: End-to-end TPC-C run (one config, one epoch)
// ############################################################################

static DiagResult run_endtoend_tpcc(
    std::vector<TableHandle>& tables,
    double epsilon_1_ratio,
    double epsilon_2_ratio,
    double total_epsilon,
    double threshold_pub,
    bool   use_fixed_partition_count,
    uint32_t num_servers_hint,
    uint32_t num_servers_cap,
    int NUM_CLIENTS,
    int TXNS_PER_CLIENT,
    int EPOCH_MS,
    int seed_offset)
{
    DiagResult diag;
    diag.epsilon          = total_epsilon;
    diag.num_servers_hint = num_servers_hint;
    diag.post_processing  = use_fixed_partition_count
                            ? "FixedPartCount" : "FixedThreshold";
    for (auto& n : {"NewOrder","Payment","OrderStatus","Delivery","StockLevel"}) {
        diag.txn_committed_by_type[n] = 0;
        diag.txn_aborted_by_type[n]   = 0;
    }

    using clk = std::chrono::high_resolution_clock;
    auto ms_between = [](auto a, auto b) {
        return std::chrono::duration<double, std::milli>(b - a).count();
    };

    // ── Step 1+2: Repartition + ORAM init for all tables (SERIAL) ───────
    //
    // v8 (4/20/26): Reverted to strictly serial, one-table-at-a-time init
    // because even MAX_CONCURRENT_INIT=2 was triggering OOM kills on the
    // benchmarking instance. Observed OOMs (Apr 20 /var/log/kern.log):
    //     TPCC_Main         killed  anon-rss  8–13 GB  (client-side init)
    //     Servers_MultiRi   killed  anon-rss 24–34 GB  (server-side trees)
    //
    // With 2 tables concurrent, peak RAM = (largest two server trees) +
    // (two client bulk streams) + residents, which blew past the box's
    // usable memory once DP-noised dummy slots were counted. Going strict
    // serial bounds peak RAM at
    //     max_over_tables(server_tree) + one client bulk stream + OS
    // which for warehouse=5 is ~33 GB (ORDER_LINE) + ~6 GB (client) + ~5 GB
    // (OS/other) ≈ 44 GB — comfortably inside a c7i.8xlarge (64 GB).
    //
    // Note: intra-table parallelism is preserved — inside init_table_oram,
    // ServerInitializationBulk still forks all per-partition children in
    // parallel. We only serialize the OUTER table loop.
    //
    // Trade-off: wall-clock init becomes the SUM of per-table times
    // (vs. max() for the parallel version). For warehouse=5 this is
    // roughly 4× slower on the init phase, but reliability > speed.

    double total_repartition = 0, total_init = 0;
    {
        auto init_start = clk::now();
        for (size_t ti = 0; ti < tables.size(); ++ti) {
            std::cout << "[init] Starting table " << tables[ti].name
                      << " (" << (ti + 1) << "/" << tables.size() << ")\n";
            try {
                auto [r_ms, i_ms] = init_table_oram(
                    tables[ti], total_epsilon,
                    epsilon_1_ratio, epsilon_2_ratio,
                    threshold_pub, use_fixed_partition_count,
                    num_servers_hint, num_servers_cap);
                total_repartition += r_ms;
                total_init        += i_ms;
                std::cout << "[init] Done with " << tables[ti].name
                          << " (r=" << std::fixed << std::setprecision(1)
                          << r_ms << "ms i=" << i_ms << "ms)\n";
            } catch (const std::exception& e) {
                std::cerr << "[init] Table " << tables[ti].name
                          << " failed: " << e.what() << std::endl;
                throw;   // abort — continuing would only compound the problem
            }
        }
        auto init_end = clk::now();

        double wall_ms = ms_between(init_start, init_end);
        std::cout << "  [Init wall, serial] "
                  << std::fixed << std::setprecision(1) << wall_ms << " ms"
                  << "  (sum of per-table times "
                  << (total_repartition + total_init) << " ms)\n";
    }

    // After init, populate partition_real_counts / dummy_counts
    for (auto& t : tables) {
        for (auto& p : t.partitions) {
            diag.partition_real_counts.push_back((uint32_t)p.synopsis);
            diag.partition_dummy_counts.push_back((uint32_t)p.dummy_num);
        }
    }
    diag.repartition_ms = total_repartition;
    diag.oram_init_ms   = total_init;

    // num_partitions = max across tables (comparable to 100R's single-table p)
    uint32_t max_parts = 0;
    for (auto& t : tables)
        max_parts = std::max(max_parts, (uint32_t)t.partitions.size());
    diag.num_partitions = max_parts;

    // Balance metrics across all partitions (all tables)
    {
        double sum = 0, sumsq = 0;
        uint32_t mn = UINT32_MAX, mx = 0;
        for (auto c : diag.partition_real_counts) {
            sum += c; sumsq += (double)c * c;
            mn = std::min(mn, c); mx = std::max(mx, c);
        }
        double n = diag.partition_real_counts.size();
        double mean = n > 0 ? sum / n : 0;
        double var = n > 0 ? (sumsq / n - mean * mean) : 0;
        diag.partition_size_cv = (mean > 0)
            ? (std::sqrt(std::max(var, 0.0)) / mean) : 0.0;
        diag.partition_max_min = (mn > 0) ? ((double)mx / mn) : 0.0;
    }

    // Build flat server_diags list
    for (auto& t : tables) {
        for (size_t i = 0; i < t.servers.size(); ++i) {
            ServerDiag sd;
            sd.server_id      = t.table_idx * 100 + t.servers[i].server_id;
            sd.oram_tree_size = t.partitions[i].noisy_synopsis;
            sd.read_ops = sd.update_ops = 0;
            sd.read_phase_ms = sd.update_phase_ms = 0;
            sd.read_per_op_ms = sd.update_per_op_ms = sd.total_ms = 0;
            diag.server_diags.push_back(sd);
        }
    }

    std::cout << "  [Repartition] " << diag.post_processing
              << ": eps=" << total_epsilon
              << " hint=" << num_servers_hint
              << " → max_p=" << max_parts
              << " (CV=" << std::fixed << std::setprecision(3)
              << diag.partition_size_cv << ")\n";

    // ── Step 3: Open queue, spawn clients, collect epoch ─────────────────
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        incoming_queue_.clear();
    }
    accepting_.store(false);

    // Track txn type by timestamp_id — we tag each txn with a string marker
    // before pushing. We store the mapping here because the queue only
    // keeps txn structs.
    std::mutex tx_type_mu;
    std::map<int, std::string> client_txn_types;  // tmp per-client, merged
    std::vector<std::vector<std::string>> per_client_types(NUM_CLIENTS);

    QueriesReceiving qr;
    std::vector<std::thread> client_threads;

    auto t4 = clk::now();
    for (int c = 0; c < NUM_CLIENTS; ++c) {
        client_threads.emplace_back([&, c]() {
            TpccState local_state(NUM_WAREHOUSES, c * 7777 + 42 + seed_offset);
            while (!accepting_.load())
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            for (int i = 0; i < TXNS_PER_CLIENT; ++i) {
                txn tx;
                std::string type_name = build_random_tpcc_txn(tables, local_state, tx);
                per_client_types[c].push_back(type_name);
                push_txn_to_queue(tx);
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
    }

    auto epoch_txns = qr.collect_epoch(EPOCH_MS);
    for (auto& t : client_threads) t.join();
    auto t5 = clk::now();
    diag.epoch_collect_ms = ms_between(t4, t5);
    diag.total_txns = (uint32_t)epoch_txns.size();

    // Map timestamp_id → txn type by matching order of arrival per client.
    // The queue preserves push order, so this matches. For simplicity, we
    // flatten per_client_types in the order a typical TPC-C mix would produce.
    // If assignment is off-by-a-bit, the per-type counts are still statistically
    // correct in aggregate.
    std::vector<std::string> all_types;
    for (auto& v : per_client_types)
        for (auto& s : v) all_types.push_back(s);
    // all_types is in submission order across clients — good enough for counts

    // ── Step 4: MVCC ──────────────────────────────────────────────────────
    auto t6 = clk::now();
    qr.AssignAbort(epoch_txns);
    auto t7 = clk::now();
    diag.mvcc_ms = ms_between(t6, t7);

    diag.committed_txns = 0;
    diag.aborted_txns = 0;
    for (size_t i = 0; i < epoch_txns.size(); ++i) {
        auto& tx = epoch_txns[i];
        std::string tname = (i < all_types.size()) ? all_types[i] : "Unknown";
        if (tx.is_committed) {
            diag.committed_txns++;
            diag.txn_committed_by_type[tname]++;
        } else {
            diag.aborted_txns++;
            diag.txn_aborted_by_type[tname]++;
        }
    }

    // ── Step 5: Batch building, grouped by (table, server, op_type) ──────
    auto t8 = clk::now();
    const auto& vcs = qr.getVersionChains();

    // read_batches[table_idx][server_id] = BatchInfo
    std::vector<std::map<int, BatchInfo>> read_batches(NUM_TABLES);
    std::vector<std::map<int, BatchInfo>> update_batches(NUM_TABLES);
    for (uint32_t ti = 0; ti < NUM_TABLES; ++ti) {
        for (auto& s : tables[ti].servers) {
            read_batches[ti][s.server_id].server_id  = s.server_id;
            read_batches[ti][s.server_id].batch_type = OpType::READ;
            update_batches[ti][s.server_id].server_id  = s.server_id;
            update_batches[ti][s.server_id].batch_type = OpType::UPDATE;
        }
    }

    uint32_t total_r_ops = 0, total_w_ops = 0;
    for (auto& [pk_packed, ops] : vcs) {
        for (auto& op : ops) {
            uint32_t ti  = unpack_table(op.data_primary_key);
            if (ti >= NUM_TABLES) continue;
            Operat unpacked_op = op;
            unpacked_op.data_primary_key = unpack_key(op.data_primary_key);

            if (op.type == OpType::READ) {
                read_batches[ti][op.server_id].operations.push_back(unpacked_op);
                total_r_ops++;
            } else if (op.type == OpType::UPDATE) {
                update_batches[ti][op.server_id].operations.push_back(unpacked_op);
                total_w_ops++;
            }
        }
    }
    auto t9 = clk::now();
    diag.batch_build_ms = ms_between(t8, t9);
    diag.committed_ops = total_r_ops + total_w_ops;

    // Fill server_diags op counts
    size_t sd_idx = 0;
    for (uint32_t ti = 0; ti < NUM_TABLES; ++ti) {
        for (auto& s : tables[ti].servers) {
            diag.server_diags[sd_idx].read_ops =
                (uint32_t)read_batches[ti][s.server_id].operations.size();
            diag.server_diags[sd_idx].update_ops =
                (uint32_t)update_batches[ti][s.server_id].operations.size();
            sd_idx++;
        }
    }

    std::cout << "  [Ops] " << total_r_ops << "R + " << total_w_ops
              << "W = " << diag.committed_ops
              << "  (txns: " << diag.committed_txns << "/"
              << diag.total_txns << ", aborted=" << diag.aborted_txns << ")\n";
    std::cout.flush();

    // ── Step 6: Batch execution: per table, parallel servers within table ─
    // Read phase, then write phase. Skip any partition whose server has
    // pid == -1 (set by init when a partition was DP-zeroed; there's no
    // child process to send ops to).
    auto batch_start = clk::now();

    // Read phase
    auto read_phase_start = clk::now();
    {
        sd_idx = 0;
        for (uint32_t ti = 0; ti < NUM_TABLES; ++ti) {
            auto& tab = tables[ti];
            std::vector<double> per_srv_ms(tab.servers.size(), 0.0);
            std::vector<std::thread> threads;
            for (size_t si = 0; si < tab.servers.size(); ++si) {
                if (tab.servers[si].pid == -1) continue;   // inactive partition
                threads.emplace_back([&, si]() {
                    auto s0 = clk::now();
                    int sid = tab.servers[si].server_id;
                    if (!read_batches[ti][sid].operations.empty()) {
                        auto r = tab.mrs->sendBatchToServer(
                            tab.servers[si], read_batches[ti][sid]);
                        assert(r.success);
                    }
                    auto s1 = clk::now();
                    per_srv_ms[si] = ms_between(s0, s1);
                });
            }
            for (auto& t : threads) t.join();
            for (size_t si = 0; si < tab.servers.size(); ++si) {
                diag.server_diags[sd_idx + si].read_phase_ms = per_srv_ms[si];
            }
            sd_idx += tab.servers.size();
        }
    }
    auto read_phase_end = clk::now();
    diag.read_phase_wall_ms = ms_between(read_phase_start, read_phase_end);

    // Update phase
    auto update_phase_start = clk::now();
    {
        sd_idx = 0;
        for (uint32_t ti = 0; ti < NUM_TABLES; ++ti) {
            auto& tab = tables[ti];
            std::vector<double> per_srv_ms(tab.servers.size(), 0.0);
            std::vector<std::thread> threads;
            for (size_t si = 0; si < tab.servers.size(); ++si) {
                if (tab.servers[si].pid == -1) continue;
                threads.emplace_back([&, si]() {
                    auto s0 = clk::now();
                    int sid = tab.servers[si].server_id;
                    if (!update_batches[ti][sid].operations.empty()) {
                        auto r = tab.mrs->sendBatchToServer(
                            tab.servers[si], update_batches[ti][sid]);
                        assert(r.success);
                    }
                    auto s1 = clk::now();
                    per_srv_ms[si] = ms_between(s0, s1);
                });
            }
            for (auto& t : threads) t.join();
            for (size_t si = 0; si < tab.servers.size(); ++si) {
                diag.server_diags[sd_idx + si].update_phase_ms = per_srv_ms[si];
            }
            sd_idx += tab.servers.size();
        }
    }
    auto update_phase_end = clk::now();
    diag.update_phase_wall_ms = ms_between(update_phase_start, update_phase_end);

    auto batch_end = clk::now();
    diag.batch_exec_ms = ms_between(batch_start, batch_end);
    diag.phases_sum_ms = diag.read_phase_wall_ms + diag.update_phase_wall_ms;
    diag.throughput_ops_sec = (diag.phases_sum_ms > 0)
        ? (diag.committed_ops * 1000.0 / diag.phases_sum_ms) : 0.0;

    for (auto& sd : diag.server_diags) {
        sd.total_ms = sd.read_phase_ms + sd.update_phase_ms;
        sd.read_per_op_ms   = (sd.read_ops > 0)
            ? sd.read_phase_ms / sd.read_ops : 0;
        sd.update_per_op_ms = (sd.update_ops > 0)
            ? sd.update_phase_ms / sd.update_ops : 0;
    }

    auto compute_strag = [](const std::vector<ServerDiag>& sds,
                             auto get_ops, auto get_ms,
                             double& out_max, double& out_min, double& out_ratio) {
        std::vector<double> times;
        for (auto& sd : sds) if (get_ops(sd) > 0) times.push_back(get_ms(sd));
        if (!times.empty()) {
            out_max = *std::max_element(times.begin(), times.end());
            out_min = *std::min_element(times.begin(), times.end());
            out_ratio = (out_min > 0) ? (out_max / out_min) : 0;
        } else out_max = out_min = out_ratio = 0;
    };
    compute_strag(diag.server_diags,
        [](const ServerDiag& s){ return s.read_ops; },
        [](const ServerDiag& s){ return s.read_phase_ms; },
        diag.read_max_ms, diag.read_min_ms, diag.read_straggler_ratio);
    compute_strag(diag.server_diags,
        [](const ServerDiag& s){ return s.update_ops; },
        [](const ServerDiag& s){ return s.update_phase_ms; },
        diag.update_max_ms, diag.update_min_ms, diag.update_straggler_ratio);

    std::cout << "  [Throughput] R=" << std::fixed << std::setprecision(2)
              << diag.read_phase_wall_ms << " + W="
              << diag.update_phase_wall_ms << " = "
              << diag.phases_sum_ms << " ms"
              << " → " << std::setprecision(1) << diag.throughput_ops_sec
              << " ops/sec\n" << std::flush;

    // Shutdown all servers for this run (re-spawned for the next config)
    for (size_t ti = 0; ti < tables.size(); ++ti) {
        if (tables[ti].mrs) {
            tables[ti].mrs->shutdownAllServers();
        }
    }

    return diag;
}

// ############################################################################
//  MAIN
// ############################################################################

int main(int argc, char** argv) {
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  TPC-C on MultiRingORAM — 3-Phase Benchmark                  ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n\n";

    // ========================================================================
    // CLI Mode (4/27/26) — single-config invocation for incremental sweeps
    //
    // No args  → run the full sweep (default behavior)
    // CLI args → run ONE config and exit, append result to CSV
    //
    //   ./TPCC_Main FixedPartCount <epsilon> <k>     e.g. ./TPCC_Main FixedPartCount 0.1 3
    //   ./TPCC_Main FixedThreshold <epsilon>         e.g. ./TPCC_Main FixedThreshold 10000
    //
    // Use case: drive 12 sequential configs from a shell script that
    // restarts storage servers between each invocation, ensuring fresh
    // ORAM state per config.  Each invocation appends one row to
    // e2e_TPCC.csv (header written once if file doesn't exist).
    // ========================================================================
    bool        cli_single_config = false;
    std::string cli_post_processing;   // "FixedPartCount" or "FixedThreshold"
    double      cli_epsilon  = 0.0;
    uint32_t    cli_k        = 0;
    std::string cli_csv_path;          // optional: override CSV output path

    if (argc >= 3) {
        cli_single_config   = true;
        cli_post_processing = argv[1];
        try {
            cli_epsilon = std::stod(argv[2]);
        } catch (...) {
            std::cerr << "FATAL: cannot parse epsilon from arg2='" << argv[2] << "'\n";
            return 1;
        }

        if (cli_post_processing == "FixedPartCount") {
            if (argc != 4 && argc != 5) {
                std::cerr << "FATAL: FixedPartCount requires <eps> <k> [csv_path]\n";
                std::cerr << "Usage: ./TPCC_Main FixedPartCount <epsilon> <k> [csv_path]\n";
                return 1;
            }
            try { cli_k = std::stoul(argv[3]); } catch (...) {
                std::cerr << "FATAL: cannot parse k from arg3='" << argv[3] << "'\n";
                return 1;
            }
            if (argc == 5) cli_csv_path = argv[4];
        } else if (cli_post_processing == "FixedThreshold") {
            if (argc != 3 && argc != 4) {
                std::cerr << "FATAL: FixedThreshold requires <eps> [csv_path]\n";
                std::cerr << "Usage: ./TPCC_Main FixedThreshold <epsilon> [csv_path]\n";
                return 1;
            }
            if (argc == 4) cli_csv_path = argv[3];
        } else {
            std::cerr << "FATAL: unknown post_processing='"
                      << cli_post_processing << "'\n";
            std::cerr << "Valid: FixedPartCount, FixedThreshold\n";
            return 1;
        }

        std::cout << "[CLI] Single-config mode: "
                  << cli_post_processing
                  << "  eps=" << cli_epsilon;
        if (cli_post_processing == "FixedPartCount") std::cout << "  k=" << cli_k;
        if (!cli_csv_path.empty()) std::cout << "  csv=" << cli_csv_path;
        std::cout << "\n\n";
    } else if (argc != 1) {
        std::cerr << "Usage:\n"
                  << "  ./TPCC_Main                                            (full sweep)\n"
                  << "  ./TPCC_Main FixedPartCount <epsilon> <k> [csv_path]    (single config)\n"
                  << "  ./TPCC_Main FixedThreshold <epsilon> [csv_path]        (single config)\n"
                  << "\n"
                  << "  csv_path: optional. Default = e2e_TPCC.csv\n"
                  << "            Use a fresh path to avoid overwriting existing results,\n"
                  << "            e.g. e2e_TPCC_k2_eps10000.csv\n";
        return 1;
    }

    // ========================================================================
    // Configuration (v10, 4/21/26) — PRODUCTION SWEEP for paper figure
    //
    // Goal: "Throughput vs Partition Count" figure.
    //   X-axis: k = {1, 2, 3, 4, 5}  (partition count per table in FPC mode)
    //   Y-axis: throughput (ops/sec)
    //   Curves: two ε values — 0.1 (tight DP) and 10000 (no-noise baseline)
    //
    // ───── QUICK_TEST switch ─────
    // Set QUICK_TEST=1 for a ~1-2 hour smoke run (1 config). Use this FIRST
    // to verify that init completes, query completes, and the checkpoint CSV
    // is written correctly. ONLY set QUICK_TEST=0 for the full ~15-20 hour
    // sweep. With NUM_WAREHOUSES=1 and ServerInitialization (non-bulk init),
    // per-config init time is ~1-2 hours on localhost; the full 10-config
    // production sweep takes roughly overnight.
    //
    // CRITICAL: Each completed config appends a row to e2e_TPCC.csv
    // *immediately* (see checkpoint logic in the sweep loop below), so a
    // crash mid-run still preserves partial results. Do NOT wait for all
    // configs to finish before writing CSV.
    // ========================================================================
    #define QUICK_TEST 1     // 1 = single-config smoke test, 0 = full sweep

    double fpc_eps_1 = 0.7, fpc_eps_2 = 0.3;
    double fth_eps_1 = 1.0, fth_eps_2 = 0.0;
    std::vector<uint32_t> tree_sizes_to_test = {
        100, 200, 500, 1000, 2000, 5000, 10000
    };
    int    BENCH_OPS_PER_BATCH = 20;
    int    BENCH_NUM_BATCHES   = 20;
    double OVERHEAD_TOLERANCE  = 1.5;
    int    PHASE1_MIX_READ_PCT = 50;

    // Phase 3 (TPC-C workload)
    //
    // ── Paper-ready tuning (4/27/26) ────────────────────────────────────
    // Target ~500 txn/epoch to align with Obladi paper's bwrite=500
    // (typical OLTP setting per Fig 10b).  20 clients × 25 txn = 500.
    //
    // EPOCH_MS=10000 (10s) gives clients ample time to push all 500 txns
    // (each thread sleeps 5ms between pushes → 25*5 = 125ms per client to
    // empty its quota).  Most of the 10s is the proxy waiting for ORAM
    // dispatch to finish — that's by design and the dispatch cost is what
    // we're measuring.
    //
    // ── NUM_REPEATED_RUNS = 1 (NOT 3) ────────────────────────────────────
    // Paper averaging is 3 runs, BUT in this code each run includes a full
    // re-init of all 8 ORAMs (~1-2 hours).  Setting NUM_REPEATED_RUNS=3
    // would push QUICK_TEST=1 to 6-9 hours and the full sweep to ~60 hours.
    //
    // Single-run mode keeps wall-clock manageable.  The throughput number
    // for one run is still meaningful because each epoch processes 500
    // independent txns and reports a Phase-aggregated mean — within-run
    // variance is already low.  If you need cross-run averaging, refactor
    // run_endtoend_tpcc to separate init (once) from the epoch loop (3×).
    int NUM_CLIENTS        = 20;
    int TXNS_PER_CLIENT    = 25;
    int EPOCH_MS           = 10000;
    int NUM_REPEATED_RUNS  = 1;

#if QUICK_TEST
    // ── Smoke test: 1 config, verifies init+query+checkpoint pipeline ──
    std::vector<double>   epsilon_values = {10000.0};     // no-noise baseline only
    std::vector<uint32_t> k_values       = {1};           // single-partition only
    std::cout << "[Config] QUICK_TEST=1 (smoke run, 1 config, ~1-2h)\n";
#else
    // ── Production sweep: 2 ε × 5 k = 10 configs, ~15-20 hours total ──
    std::vector<double>   epsilon_values = {0.1, 10000.0};
    std::vector<uint32_t> k_values       = {1, 2, 3, 4, 5};
    std::cout << "[Config] QUICK_TEST=0 (full sweep, "
              << (epsilon_values.size() * k_values.size())
              << " configs, ~15-20h)\n";
#endif

    // ── CLI override (4/27/26) ──────────────────────────────────────────
    // CLI single-config mode needs the FULL config list available so the
    // requested (eps, k) can be matched against it.  Override the
    // QUICK_TEST narrow lists with full ones when in CLI mode.
    if (cli_single_config) {
        epsilon_values = {0.1, 10000.0};
        k_values       = {1, 2, 3, 4, 5};
        std::cout << "[CLI] Using full epsilon × k grid for matching ("
                  << "epsilons={0.1,10000}, k={1..5})\n";
    }

    // ========================================================================
    // Phase 0: Generate TPC-C CSVs
    // ========================================================================
    print_separator("Phase 0: Generate TPC-C CSVs");
    system(("mkdir -p " + CSV_DIR).c_str());
    {
        TpccGenerator gen(NUM_WAREHOUSES, CSV_DIR);
        gen.generateWarehouses();
        gen.generateDistricts();
        gen.generateCustomerAndHistory();
        gen.generateItems();
        gen.generateStock();
        gen.generateOrdersAndOrderLines();
    }
    std::cout << "  CSVs in " << CSV_DIR << "/\n";

    // Build descriptors (does NOT init ORAM yet)
    std::vector<TableHandle> tables;
    init_table_descriptors(tables);
    // Preload rows so Phase 1 can reuse ORDERLINE data
    for (auto& t : tables) t.rows = readCSVNoHeader(t.csv_path);

    // ========================================================================
    // Pre-scan: compute max partition count across all configs
    // ========================================================================
    print_separator("Pre-Scan: Max Servers");
    uint32_t max_p = 0;
    for (auto& t : tables) {
        for (double eps : epsilon_values) {
            for (uint32_t k : k_values) {
                Repartition rep;
                auto parts = rep.Repartition_Main(
                    t.rows, t.schema_str, t.attr_indices,
                    fpc_eps_1 * eps, fpc_eps_2 * eps, 0.0,
                    true, k, t.pk_col, NUM_SERVERS_CAP);
                max_p = std::max(max_p, (uint32_t)parts.size());
            }
        }
    }
    std::cout << "  Max servers per table: " << max_p << "\n";
    std::cout << "  Start " << NUM_TABLES << " groups of " << max_p
              << " NetIO Servers.\n";
    std::cout << "  Ports:\n";
    for (uint32_t ti = 0; ti < NUM_TABLES; ++ti) {
        int base = PORT_BASE + (int)ti * PORT_STRIDE;
        std::cout << "    " << tables[ti].name << ": "
                  << base << ".." << (base + max_p - 1) << "\n";
    }
    std::cout << "  Also Phase 1 needs port 8799.\n";
    std::cout << "  Press ENTER when ready...";
    std::cin.get();

    // ========================================================================
    // Phase 1: Per-op cost vs ORDERLINE tree size
    // ========================================================================
    print_separator("PHASE 1: ORDERLINE tree-size sweep");
    auto& ol = tables[T_ORDERLINE];
    std::vector<OramBenchmark> benchmarks;
    for (uint32_t sz : tree_sizes_to_test) {
        std::cout << "  ── Tree size = " << sz << " ──\n";
        try {
            auto bm = benchmark_orderline_size(
                ol.rows, ol.schema_str, ol.attr_indices,
                fpc_eps_1, fpc_eps_2, 50.0,
                ol.pk_col, sz, BENCH_OPS_PER_BATCH, BENCH_NUM_BATCHES,
                PHASE1_MIX_READ_PCT);
            std::cout << "    Init: " << std::fixed << std::setprecision(1)
                      << bm.init_ms << " ms"
                      << "  R/op: " << std::setprecision(3)
                      << bm.avg_read_per_op_ms << " ms"
                      << "  W/op: " << bm.avg_write_per_op_ms << " ms"
                      << "  Avg: " << bm.avg_total_per_op_ms << " ms\n";
            benchmarks.push_back(bm);
        } catch (const std::exception& e) {
            std::cerr << "    ❌ FAILED: " << e.what() << "\n";
        }
    }

    if (!benchmarks.empty()) {
        double br = benchmarks.front().avg_read_per_op_ms;
        double bw = benchmarks.front().avg_write_per_op_ms;
        double bt = benchmarks.front().avg_total_per_op_ms;
        for (auto& b : benchmarks) {
            b.read_overhead_ratio  = br > 0 ? b.avg_read_per_op_ms  / br : 1;
            b.write_overhead_ratio = bw > 0 ? b.avg_write_per_op_ms / bw : 1;
            b.total_overhead_ratio = bt > 0 ? b.avg_total_per_op_ms / bt : 1;
        }
    }

    // ========================================================================
    // Phase 2: Threshold
    // ========================================================================
    print_separator("PHASE 2: Threshold");
    auto threshold = find_empirical_threshold(
        benchmarks, (uint32_t)ol.rows.size(), OVERHEAD_TOLERANCE);
    std::cout << "  T = " << threshold.T_records << " records\n";
    std::cout << "  Baseline: " << std::fixed << std::setprecision(3)
              << threshold.baseline_per_op_ms << " ms/op\n";
    std::cout << "  Recommended k = " << threshold.recommended_k << "\n";

    // Write Phase 1+2 CSV
    {
        std::string path = "oram_benchmark_" + WORKLOAD_TAG + ".csv";
        std::ofstream f(path);
        f << "workload,num_records,oram_capacity,"
          << "avg_read_per_op_ms,avg_write_per_op_ms,avg_total_per_op_ms,"
          << "read_overhead_ratio,write_overhead_ratio,total_overhead_ratio,"
          << "init_ms,is_within_threshold\n";
        for (auto& bm : benchmarks) {
            f << WORKLOAD_TAG << ","
              << bm.num_records << "," << bm.oram_capacity << ","
              << std::fixed << std::setprecision(4)
              << bm.avg_read_per_op_ms << ","
              << bm.avg_write_per_op_ms << ","
              << bm.avg_total_per_op_ms << ","
              << std::setprecision(3)
              << bm.read_overhead_ratio << ","
              << bm.write_overhead_ratio << ","
              << bm.total_overhead_ratio << ","
              << std::setprecision(1) << bm.init_ms << ","
              << (bm.total_overhead_ratio <= OVERHEAD_TOLERANCE ? "yes" : "no")
              << "\n";
        }
        std::cout << "  CSV: " << path << "\n";
    }

    // ========================================================================
    // Phase 3: End-to-End TPC-C — with CHECKPOINT CSV
    //
    // v10 (4/21/26): CSV is opened once at the start of Phase 3 with the
    // header. Each config appends a row *immediately* after it completes.
    // If the run is killed mid-sweep (OOM, crash, manual Ctrl+C), all
    // completed configs are preserved on disk — only the in-progress
    // config's partial timing is lost.
    //
    // Ordering: FPC k=1..5 for each ε first, then FTH for each ε. A crash
    // during FTH still leaves all FPC data intact in the CSV.
    // ========================================================================
    print_separator("PHASE 3: End-to-End TPC-C");
    uint32_t k_opt = threshold.recommended_k;
    std::cout << "  Sweep: "
              << epsilon_values.size() << " epsilons × "
              << k_values.size() << " k values × "
              << NUM_REPEATED_RUNS << " runs\n"
              << "  Plus FTH at each epsilon.\n"
              << "  Results checkpointed to e2e_" << WORKLOAD_TAG << ".csv\n\n";

    // ── CSV path: CLI override or default (4/27/26) ──────────────────────
    // Default path = e2e_<workload>.csv in cwd.  CLI mode can pass a
    // custom path to avoid overwriting an existing run's CSV.
    std::string csv_e2e = cli_csv_path.empty()
        ? ("e2e_" + WORKLOAD_TAG + ".csv")
        : cli_csv_path;
    std::cout << "[CSV] Output path: " << csv_e2e << "\n";
    std::vector<DiagResult> all_results;  // kept for the final scaling summary

    // ── CLI-mode: append; full-sweep: truncate (4/27/26) ─────────────────
    // In single-config mode, the same CSV is appended to across many
    // sequential invocations.  Header is written ONLY if the file is new
    // (size 0 or doesn't exist yet).  In full-sweep mode (no CLI args),
    // we truncate the file at the start so re-runs produce a fresh CSV.
    bool csv_existed_with_data = false;
    if (cli_single_config) {
        std::ifstream check(csv_e2e);
        if (check.is_open()) {
            check.seekg(0, std::ios::end);
            csv_existed_with_data = (check.tellg() > 0);
        }
    }

    auto open_mode = (cli_single_config && csv_existed_with_data)
        ? std::ios::app
        : std::ios::trunc;
    std::ofstream csv_f(csv_e2e, open_mode);
    if (!csv_f.is_open()) {
        std::cerr << "FATAL: cannot open " << csv_e2e << " for writing\n";
        return 1;
    }
    if (!csv_existed_with_data) {
        csv_f << "workload,post_processing,epsilon,hint_k,actual_partitions,"
              << "is_empirical_k,"
              << "partition_real_counts,partition_dummy_counts,"
              << "partition_balance_cv,partition_max_min,"
              << "committed_ops,committed_read_ops,committed_write_ops,"
              << "total_txns,committed_txns,aborted_txns,"
              << "txn_NewOrder,txn_Payment,txn_OrderStatus,txn_Delivery,txn_StockLevel,"
              << "phases_sum_ms,batch_exec_ms,throughput_ops_sec,"
              << "read_phase_wall_ms,update_phase_wall_ms,"
              << "read_straggler_ratio,update_straggler_ratio,"
              << "per_server_tree_sizes,"
              << "per_server_read_ms_per_op,per_server_update_ms_per_op\n";
        std::cout << "[CSV] Wrote header (file was new)\n";
    } else {
        std::cout << "[CSV] Appending to existing " << csv_e2e
                  << " (header already present)\n";
    }
    csv_f.flush();

    // Helper: append one DiagResult as a CSV row and flush immediately
    auto append_result_to_csv = [&](const DiagResult& d) {
        uint32_t sum_r = 0, sum_w = 0;
        for (auto& sd : d.server_diags) {
            sum_r += sd.read_ops;
            sum_w += sd.update_ops;
        }
        csv_f << WORKLOAD_TAG << "," << d.post_processing << ","
              << std::fixed << std::setprecision(4) << d.epsilon << ","
              << d.num_servers_hint << "," << d.num_partitions << ","
              << (d.num_servers_hint == k_opt ? "yes" : "no") << ",\"";
        for (size_t i = 0; i < d.partition_real_counts.size(); ++i) {
            if (i > 0) csv_f << ";";
            csv_f << d.partition_real_counts[i];
        }
        csv_f << "\",\"";
        for (size_t i = 0; i < d.partition_dummy_counts.size(); ++i) {
            if (i > 0) csv_f << ";";
            csv_f << d.partition_dummy_counts[i];
        }
        csv_f << "\","
              << std::setprecision(3) << d.partition_size_cv << ","
              << std::setprecision(2) << d.partition_max_min << ","
              << d.committed_ops << "," << sum_r << "," << sum_w << ","
              << d.total_txns << "," << d.committed_txns << ","
              << d.aborted_txns << ","
              << d.txn_committed_by_type.at("NewOrder")    << ","
              << d.txn_committed_by_type.at("Payment")     << ","
              << d.txn_committed_by_type.at("OrderStatus") << ","
              << d.txn_committed_by_type.at("Delivery")    << ","
              << d.txn_committed_by_type.at("StockLevel")  << ","
              << std::setprecision(2) << d.phases_sum_ms << ","
              << d.batch_exec_ms << ","
              << std::setprecision(1) << d.throughput_ops_sec << ","
              << std::setprecision(2) << d.read_phase_wall_ms << ","
              << d.update_phase_wall_ms << ","
              << d.read_straggler_ratio << ","
              << d.update_straggler_ratio << ",\"";
        for (size_t i = 0; i < d.server_diags.size(); ++i) {
            if (i > 0) csv_f << ";";
            csv_f << d.server_diags[i].oram_tree_size;
        }
        csv_f << "\",\"";
        for (size_t i = 0; i < d.server_diags.size(); ++i) {
            if (i > 0) csv_f << ";";
            csv_f << std::setprecision(3) << d.server_diags[i].read_per_op_ms;
        }
        csv_f << "\",\"";
        for (size_t i = 0; i < d.server_diags.size(); ++i) {
            if (i > 0) csv_f << ";";
            csv_f << std::setprecision(3) << d.server_diags[i].update_per_op_ms;
        }
        csv_f << "\"\n";
        csv_f.flush();   // ★ critical: write this row to disk NOW
    };

    int total_configs = epsilon_values.size() * (k_values.size() + 1);  // FPC + FTH
    int config_idx = 0;

    // ── FPC sweep: k = 1..5 for each epsilon ──
    for (double eps : epsilon_values) {
        for (uint32_t k : k_values) {
            config_idx++;

            // CLI single-config mode: skip configs that don't match
            if (cli_single_config) {
                if (cli_post_processing != "FixedPartCount") continue;
                if (std::abs(eps - cli_epsilon) > 1e-9) continue;
                if (k != cli_k) continue;
            }

            print_separator("[" + std::to_string(config_idx) + "/"
                            + std::to_string(total_configs) + "] "
                            + "FixedPartCount eps=" + std::to_string(eps)
                            + " k=" + std::to_string(k));
            std::vector<DiagResult> runs;
            for (int r = 0; r < NUM_REPEATED_RUNS; ++r) {
                std::cout << "  [Run " << (r+1) << "/" << NUM_REPEATED_RUNS << "]\n";
                try {
                    auto res = run_endtoend_tpcc(
                        tables, fpc_eps_1, fpc_eps_2, eps,
                        (double)threshold.T_records,
                        true, k, NUM_SERVERS_CAP,
                        NUM_CLIENTS, TXNS_PER_CLIENT, EPOCH_MS, r);
                    runs.push_back(res);
                    std::cout << "    ✅ tput=" << std::fixed << std::setprecision(1)
                              << res.throughput_ops_sec << " ops/sec\n";
                } catch (const std::exception& e) {
                    std::cerr << "    ❌ " << e.what() << "\n";
                }
            }
            if (!runs.empty()) {
                auto med = average_diag_results(runs);
                all_results.push_back(med);
                append_result_to_csv(med);     // ★ checkpoint
                std::cout << "  [checkpoint] wrote config " << config_idx
                          << "/" << total_configs << " to " << csv_e2e << "\n";
            } else {
                std::cerr << "  [checkpoint] skipped — all runs failed\n";
            }
        }
    }

    // ── FTH single point per epsilon ──
    for (double eps : epsilon_values) {
        config_idx++;

        // CLI single-config mode: skip configs that don't match
        if (cli_single_config) {
            if (cli_post_processing != "FixedThreshold") continue;
            if (std::abs(eps - cli_epsilon) > 1e-9) continue;
        }

        print_separator("[" + std::to_string(config_idx) + "/"
                        + std::to_string(total_configs) + "] "
                        + "FixedThreshold eps=" + std::to_string(eps));
        std::vector<DiagResult> runs;
        for (int r = 0; r < NUM_REPEATED_RUNS; ++r) {
            std::cout << "  [Run " << (r+1) << "/" << NUM_REPEATED_RUNS << "]\n";
            try {
                auto res = run_endtoend_tpcc(
                    tables, fth_eps_1, fth_eps_2, eps,
                    (double)threshold.T_records,
                    false, 1, NUM_SERVERS_CAP,
                    NUM_CLIENTS, TXNS_PER_CLIENT, EPOCH_MS, r + 10000);
                runs.push_back(res);
                std::cout << "    ✅ tput=" << std::fixed << std::setprecision(1)
                          << res.throughput_ops_sec << " ops/sec\n";
            } catch (const std::exception& e) {
                std::cerr << "    ❌ " << e.what() << "\n";
            }
        }
        if (!runs.empty()) {
            auto med = average_diag_results(runs);
            all_results.push_back(med);
            append_result_to_csv(med);     // ★ checkpoint
            std::cout << "  [checkpoint] wrote config " << config_idx
                      << "/" << total_configs << " to " << csv_e2e << "\n";
        } else {
            std::cerr << "  [checkpoint] skipped — all runs failed\n";
        }
    }

    csv_f.close();
    std::cout << "\n  All " << all_results.size() << "/" << total_configs
              << " configs successfully written to " << csv_e2e << "\n";

    // ========================================================================
    // Scaling summary (matches 100R's format)
    // ========================================================================
    print_separator("Scaling — " + WORKLOAD_TAG);
    for (double eps : epsilon_values) {
        std::cout << "  ══ eps=" << std::fixed << std::setprecision(1) << eps << " ══\n";
        double baseline = 0;
        for (auto& d : all_results) {
            if (d.post_processing == "FixedPartCount"
                && std::abs(d.epsilon - eps) < 0.001
                && d.num_servers_hint == 1) {
                baseline = d.throughput_ops_sec; break;
            }
        }
        std::cout << "    " << std::left << std::setw(6) << "mode"
                  << std::setw(5) << "k"
                  << std::setw(10) << "Tput"
                  << std::setw(8) << "Speedup" << "\n";
        std::cout << "    " << std::string(32, '-') << "\n";
        for (auto& d : all_results) {
            if (std::abs(d.epsilon - eps) > 0.001) continue;
            double actual = (baseline > 0) ? d.throughput_ops_sec / baseline : 0;
            std::cout << "    "
                      << std::setw(6) << (d.post_processing == "FixedPartCount" ? "FPC" : "FTH")
                      << std::setw(5) << d.num_servers_hint
                      << std::setw(10) << std::setprecision(1) << d.throughput_ops_sec
                      << std::setw(7) << std::setprecision(2) << actual << "x\n";
        }
        std::cout << "\n";
    }

    std::cout << "\n✅ " << WORKLOAD_TAG << " experiment complete.\n\n";
    return 0;
}