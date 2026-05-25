//
// Created by Xining Yuan on 4/10/26.
//
//
// ORAM_Performance_100R.cpp
//
// Created by Xining Yuan on 3/1/26.
//
// Workload: 100% Read  (Read=100%, Write=0%)
//
// Phase 1: Benchmark ORAM per-op cost at varying tree sizes
// Phase 2: Find threshold T (knee in latency curve)
// Phase 3: End-to-end with empirical T
//

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
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
#include <signal.h>      // kill(), SIGTERM
#include <sys/wait.h>    // waitpid()

#include "MultiRingORAM_Servers.h"
#include "Queries_Receiving.h"

extern std::mutex         queue_mu_;
extern std::vector<txn>   incoming_queue_;
extern std::atomic<bool>  accepting_;

// ============================================================================
// Workload config
// ============================================================================
static constexpr int    READ_PCT  = 100;
static constexpr int    WRITE_PCT = 0;
static const std::string WORKLOAD_TAG = "100R_Skewed";

// Physical server cap: FPC never produces more partitions than this.
// Update if you add/remove EC2 server instances.
static constexpr uint32_t NUM_SERVERS = 4;

// Zipf skew exponent for access pattern.  α=1.0 is standard Zipf (moderate skew).
// Higher α → heavier tail (more accesses concentrate on a few hot blocks).
// α=0 → uniform (equivalent to original experiments).
static constexpr double SKEW_ALPHA = 1.0;

// ============================================================================
// Helpers
// ============================================================================

static std::vector<std::string> readCSV(const std::string& filename) {
    std::vector<std::string> data;
    std::ifstream file(filename);
    if (!file.is_open()) throw std::runtime_error("Cannot open: " + filename);
    std::string line;
    std::getline(file, line);
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
                       int server_id, bool last = false) {
    Operat op;
    op.type = type;
    op.data_primary_key = key;
    op.data_value = val;
    op.server_id = server_id;
    op.last_one = last;
    op.read_marker = false;
    op.txn_timestamp_id = 0;
    return op;
}

static void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "  " << title << "\n";
    std::cout << std::string(70, '=') << "\n\n";
}

// ============================================================================
// Phase 1 result
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
// Phase 3 result
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
    uint32_t num_partitions;
    std::string post_processing;
    std::vector<uint32_t> partition_real_counts;
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
    std::vector<ServerDiag> server_diags;
    double   read_max_ms, read_min_ms, read_straggler_ratio;
    double   update_max_ms, update_min_ms, update_straggler_ratio;
    double   partition_size_cv;
    double   partition_max_min;
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
//  Helper: take the MEDIAN of multiple DiagResult runs
//  Structural fields (partition layout, epsilon, etc.) come from the first run.
//  All timing / throughput fields are the median across all runs.
// ============================================================================
static double median_of(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    size_t n = v.size();
    return (n % 2 == 1) ? v[n/2] : (v[n/2 - 1] + v[n/2]) / 2.0;
}

static DiagResult average_diag_results(const std::vector<DiagResult>& runs) {
    if (runs.empty()) throw std::runtime_error("median_diag_results: empty runs");
    if (runs.size() == 1) return runs[0];

    DiagResult med = runs[0];   // copy structural fields from first run

    std::vector<double> v_tput, v_phases, v_batch, v_read_wall, v_upd_wall,
                        v_read_strag, v_upd_strag, v_committed;
    for (const auto& r : runs) {
        v_tput.push_back(r.throughput_ops_sec);
        v_phases.push_back(r.phases_sum_ms);
        v_batch.push_back(r.batch_exec_ms);
        v_read_wall.push_back(r.read_phase_wall_ms);
        v_upd_wall.push_back(r.update_phase_wall_ms);
        v_read_strag.push_back(r.read_straggler_ratio);
        v_upd_strag.push_back(r.update_straggler_ratio);
        v_committed.push_back(static_cast<double>(r.committed_ops));
    }
    med.throughput_ops_sec     = median_of(v_tput);
    med.phases_sum_ms          = median_of(v_phases);
    med.batch_exec_ms          = median_of(v_batch);
    med.read_phase_wall_ms     = median_of(v_read_wall);
    med.update_phase_wall_ms   = median_of(v_upd_wall);
    med.read_straggler_ratio   = median_of(v_read_strag);
    med.update_straggler_ratio = median_of(v_upd_strag);
    med.committed_ops          = static_cast<uint32_t>(median_of(v_committed));

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


// ############################################################################
//  PHASE 1: ORAM Size Benchmark — Workload: 100% Read
// ############################################################################

static OramBenchmark benchmark_single_oram_size(
    const std::vector<std::string>& full_data,
    const std::string& schema,
    const std::vector<int>& attr,
    double epsilon_1_ratio,
    double epsilon_2_ratio,
    double total_epsilon,
    int primary_key_col,
    uint32_t target_size,
    int ops_per_batch,
    int num_batches)
{
    using clk = std::chrono::high_resolution_clock;
    auto ms_between = [](auto a, auto b) {
        return std::chrono::duration<double, std::milli>(b - a).count();
    };

    OramBenchmark result;
    result.num_records    = target_size;
    result.ops_per_batch  = ops_per_batch;
    result.num_batches    = num_batches;

    uint32_t actual_size = std::min(target_size,
                                     static_cast<uint32_t>(full_data.size()));
    std::vector<std::string> subset(full_data.begin(),
                                     full_data.begin() + actual_size);

    Repartition repartitioner;
    auto partitions = repartitioner.Repartition_Main(
        subset, schema, attr,
        epsilon_1_ratio * total_epsilon,
        epsilon_2_ratio * total_epsilon,
        0.0,  // threshold_pub unused for single-partition benchmark
        true, 1, primary_key_col
    );
    const BinInfo& bin_info = repartitioner.getBinInfo();
    assert(partitions.size() == 1 && "Expected single partition for benchmark");

    // ORAM tree is always sized to noisy_synopsis (privacy invariant).
    result.oram_capacity = partitions[0].noisy_synopsis;

    ServerInfo server;
    server.server_id    = 0;
    server.port         = 8899;  // separate port for Phase 1
    server.pid          = 0;
    server.partition_id = 0;
    server.assigned_bins = partitions[0].index;

    MultiRingORAM_Servers multi_oram;
    std::vector<ServerInfo> servers_vec = {server};
    multi_oram.distributeDataToPartitions(bin_info, partitions, servers_vec);
    server = servers_vec[0];

    uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    uint32_t block_size  = tuple_width + AES::BLOCKSIZE + 2 * sizeof(uint32_t);
    uint32_t bucket_size = 8;
    uint32_t S_param     = 4;

    // Fork a server process (child creates NetIOConnector internally).
    auto t_init_start = clk::now();
    multi_oram.ServerInitialization(
        server, partitions[0], bin_info, subset, schema,
        bucket_size, "RingORAM_Bench",
        block_size, "127.0.0.1", S_param);
    multi_oram.waitForAllServersReady();
    auto t_init_end = clk::now();
    result.init_ms = ms_between(t_init_start, t_init_end);
    uint32_t num_blocks = static_cast<uint32_t>(
        server.assigned_data_indices.size());

    std::mt19937 rng(12345);

    // Pre-compute Zipf weight vector for Phase 1 (single server, num_blocks items).
    // rank 0 = hottest block: P(rank k) ∝ 1/(k+1)^SKEW_ALPHA
    std::vector<double> ph1_zipf_w(num_blocks);
    for (uint32_t i = 0; i < num_blocks; ++i)
        ph1_zipf_w[i] = 1.0 / std::pow(static_cast<double>(i + 1), SKEW_ALPHA);
    std::discrete_distribution<uint32_t> ph1_zipf(ph1_zipf_w.begin(), ph1_zipf_w.end());

    for (int batch = 0; batch < num_batches; ++batch) {
        // 100% READ workload
        BatchInfo read_batch;
        read_batch.server_id  = 0;
        read_batch.batch_type = OpType::READ;
        for (int i = 0; i < ops_per_batch; ++i) {
            uint32_t block_id = ph1_zipf(rng);
            read_batch.operations.push_back(
                make_op(OpType::READ, block_id, "", 0, (i == ops_per_batch - 1)));
        }

        auto r_start = clk::now();
        auto r_result = multi_oram.sendBatchToServer(server, read_batch);
        auto r_end = clk::now();
        assert(r_result.success && "READ benchmark batch failed");
        result.read_batch_ms.push_back(ms_between(r_start, r_end));
        result.write_batch_ms.push_back(0.0);  // no writes
    }

    // ── Compute averages ──

    double total_read_ms = 0;
    for (auto v : result.read_batch_ms)  total_read_ms += v;
    double total_ops = ops_per_batch * num_batches;
    result.avg_read_per_op_ms  = total_read_ms / total_ops;
    result.avg_write_per_op_ms = 0.0;
    result.avg_total_per_op_ms = result.avg_read_per_op_ms;

    result.read_overhead_ratio  = 1.0;
    result.write_overhead_ratio = 1.0;
    result.total_overhead_ratio = 1.0;

    multi_oram.shutdownAllServers();
    return result;
}


// ############################################################################
//  PHASE 2: Find Empirical Threshold
// ############################################################################

static ThresholdResult find_empirical_threshold(
    const std::vector<OramBenchmark>& benchmarks,
    uint32_t total_data_size,
    double tolerance = 1.5)
{
    ThresholdResult result;
    result.tolerance_used = tolerance;

    double baseline_read  = benchmarks.front().avg_read_per_op_ms;
    double baseline_write = benchmarks.front().avg_write_per_op_ms;
    double baseline_total = benchmarks.front().avg_total_per_op_ms;
    result.baseline_per_op_ms = baseline_total;

    result.T_records        = benchmarks.front().num_records;
    result.T_read_per_op_ms = baseline_read;
    result.T_write_per_op_ms = baseline_write;

    for (auto& bm : benchmarks) {
        double ratio = (baseline_total > 0)
            ? (bm.avg_total_per_op_ms / baseline_total) : 1.0;
        if (ratio <= tolerance) {
            result.T_records         = bm.num_records;
            result.T_read_per_op_ms  = bm.avg_read_per_op_ms;
            result.T_write_per_op_ms = bm.avg_write_per_op_ms;
        }
    }

    result.recommended_k = (result.T_records > 0)
        ? static_cast<uint32_t>(std::ceil(
              static_cast<double>(total_data_size) / result.T_records))
        : 1;

    return result;
}


// ############################################################################
//  Helper: get a data subset of exactly `target_size` rows.
//  If target_size > data.size(), the original dataset is tiled (cycled)
//  so that the ORAM benchmark always has a full, evenly-sized workload.
// ############################################################################
static std::vector<std::string> get_data_subset(
    const std::vector<std::string>& data,
    uint32_t target_size)
{
    if (target_size == 0 || data.empty()) return {};
    std::vector<std::string> subset;
    subset.reserve(target_size);
    for (uint32_t i = 0; i < target_size; ++i)
        subset.push_back(data[i % data.size()]);  // cycle if needed
    return subset;
}

// ############################################################################
//  PHASE 3: End-to-End — Workload: 100% Read
// ############################################################################

static DiagResult run_endtoend_test(
    const std::vector<std::string>& data,
    const std::string& schema,
    const std::vector<int>& attr,
    double epsilon_1_ratio,
    double epsilon_2_ratio,
    double total_epsilon,
    double threshold_pub,
    bool   use_fixed_partition_count,
    uint32_t num_servers_hint,
    int primary_key_col,
    uint32_t num_servers,        // hard cap passed to Repartition_Main
    int NUM_CLIENTS,
    int TXNS_PER_CLIENT,
    int OPS_PER_TXN,
    int EPOCH_MS)
{
    DiagResult diag;
    diag.epsilon           = total_epsilon;
    diag.num_servers_hint  = num_servers_hint;
    diag.post_processing   = use_fixed_partition_count
                             ? "FixedPartCount" : "FixedThreshold";

    using clk = std::chrono::high_resolution_clock;
    auto ms_between = [](auto a, auto b) {
        return std::chrono::duration<double, std::milli>(b - a).count();
    };

    // ── Step 1: Repartition ──
    auto t0 = clk::now();
    Repartition repartitioner;
    auto partitions = repartitioner.Repartition_Main(
        data, schema, attr,
        epsilon_1_ratio * total_epsilon,
        epsilon_2_ratio * total_epsilon,
        threshold_pub,
        use_fixed_partition_count,
        num_servers_hint, primary_key_col,
        num_servers          // hard cap: never exceed physical servers
    );
    const BinInfo& bin_info = repartitioner.getBinInfo();
    auto t1 = clk::now();
    diag.repartition_ms = ms_between(t0, t1);
    diag.num_partitions = static_cast<uint32_t>(partitions.size());

    for (size_t i = 0; i < partitions.size(); ++i) {
        diag.partition_real_counts.push_back(
            static_cast<uint32_t>(partitions[i].synopsis));
        diag.partition_dummy_counts.push_back(
            static_cast<uint32_t>(partitions[i].dummy_num));
    }

    // Balance metrics
    {
        double sum = 0, sumsq = 0;
        uint32_t mn = UINT32_MAX, mx = 0;
        for (auto c : diag.partition_real_counts) {
            sum += c; sumsq += (double)c * c;
            mn = std::min(mn, c); mx = std::max(mx, c);
        }
        double n = diag.partition_real_counts.size();
        double mean = sum / n;
        double var = sumsq / n - mean * mean;
        diag.partition_size_cv = (mean > 0)
            ? (std::sqrt(std::max(var, 0.0)) / mean) : 0.0;
        diag.partition_max_min = (mn > 0) ? ((double)mx / mn) : 0.0;
    }

    std::cout << "  [Repartition] " << diag.post_processing
              << ": eps=" << total_epsilon
              << " hint=" << num_servers_hint
              << " → " << partitions.size() << " partitions"
              << "  (CV=" << std::fixed << std::setprecision(3)
              << diag.partition_size_cv
              << " max/min=" << std::setprecision(1)
              << diag.partition_max_min << "x)\n";
    std::cout << "    Sizes: ";
    for (size_t i = 0; i < partitions.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << partitions[i].synopsis << "r+"
                  << partitions[i].dummy_num << "d";
    }
    std::cout << "\n";

    // ── Step 2: ServerInfo + ORAM init ──
    int base_port = 8881;
    std::vector<ServerInfo> servers(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        servers[i].server_id    = static_cast<int>(i);
        servers[i].port         = base_port + static_cast<int>(i);
        servers[i].pid          = 0;
        servers[i].partition_id = i;
        servers[i].assigned_bins = partitions[i].index;
    }

    MultiRingORAM_Servers multi_oram;
    multi_oram.distributeDataToPartitions(bin_info, partitions, servers);

    uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    uint32_t block_size  = tuple_width + AES::BLOCKSIZE + 2 * sizeof(uint32_t);
    uint32_t bucket_size = 8;
    uint32_t S = 4;

    // Fork one process per server.  Children initialize their RingORAMs
    // (and create their own NetIOConnectors) in parallel; the parent waits
    // for all ready signals before proceeding.
    auto t2 = clk::now();
    for (size_t i = 0; i < partitions.size(); ++i) {
        multi_oram.ServerInitialization(
            servers[i], partitions[i], bin_info, data, schema,
            bucket_size,
            "RingORAM_S" + std::to_string(i),
            block_size, "127.0.0.1", S);
    }
    multi_oram.waitForAllServersReady();  // blocks until slowest child is ready
    auto t3 = clk::now();
    diag.oram_init_ms = ms_between(t2, t3);

    std::vector<uint32_t> blocks_per_server(servers.size());
    for (size_t i = 0; i < servers.size(); ++i)
        blocks_per_server[i] =
            static_cast<uint32_t>(servers[i].assigned_data_indices.size());

    diag.server_diags.resize(servers.size());
    for (size_t i = 0; i < servers.size(); ++i) {
        diag.server_diags[i].server_id = servers[i].server_id;
        // ORAM tree is always sized to noisy_synopsis (privacy invariant).
        diag.server_diags[i].oram_tree_size = partitions[i].noisy_synopsis;
    }

    // ── Step 3: Epoch collection — FIXED WORKLOAD 100% Read ──
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        incoming_queue_.clear();
    }
    accepting_.store(false);

    // Pre-compute per-server Zipf weight vectors for Phase 3 epoch queries.
    // skew_weights[server_id][rank] ∝ 1/(rank+1)^SKEW_ALPHA.
    // Captured by reference in client lambdas (read-only, safe for concurrent access).
    std::vector<std::vector<double>> skew_weights(servers.size());
    for (size_t s = 0; s < servers.size(); ++s) {
        uint32_t N = blocks_per_server[s];
        skew_weights[s].resize(N);
        for (uint32_t i = 0; i < N; ++i)
            skew_weights[s][i] = 1.0 / std::pow(static_cast<double>(i + 1), SKEW_ALPHA);
    }

    QueriesReceiving qr;
    std::vector<std::thread> client_threads;
    auto t4 = clk::now();

    for (int c = 0; c < NUM_CLIENTS; c++) {
        client_threads.emplace_back([&, c]() {
            std::mt19937 rng(c * 7777 + 42);
            while (!accepting_.load())
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            for (int t_idx = 0; t_idx < TXNS_PER_CLIENT; t_idx++) {
                txn t;
                for (int q = 0; q < OPS_PER_TXN; q++) {
                    int sid = rng() % servers.size();
                    // Zipf-distributed block access (skewed workload)
                    std::discrete_distribution<uint32_t> _zipf(
                        skew_weights[static_cast<size_t>(sid)].begin(),
                        skew_weights[static_cast<size_t>(sid)].end());
                    uint32_t block_id = _zipf(rng);
                    OpType type = OpType::READ;
                    std::string val = "";
                    t.operations.push_back(
                        make_op(type, block_id, val, sid,
                                (q == OPS_PER_TXN - 1)));
                }
                push_txn_to_queue(t);
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }

    auto epoch_txns = qr.collect_epoch(EPOCH_MS);
    for (auto& t : client_threads) t.join();
    auto t5 = clk::now();
    diag.epoch_collect_ms = ms_between(t4, t5);
    diag.total_txns = static_cast<uint32_t>(epoch_txns.size());

    // ── Step 4: MVCC ──
    auto t6 = clk::now();
    qr.AssignAbort(epoch_txns);
    auto t7 = clk::now();
    diag.mvcc_ms = ms_between(t6, t7);

    diag.committed_txns = 0;
    diag.aborted_txns = 0;
    for (auto& tx : epoch_txns) {
        if (tx.is_committed) diag.committed_txns++;
        else diag.aborted_txns++;
    }

    // ── Step 5: Batch building ──
    auto t8 = clk::now();
    const auto& vcs = qr.getVersionChains();
    std::map<int, BatchInfo> read_batches;
    std::map<int, BatchInfo> update_batches;

    for (auto& s : servers) {
        read_batches[s.server_id].server_id   = s.server_id;
        read_batches[s.server_id].batch_type   = OpType::READ;
        update_batches[s.server_id].server_id  = s.server_id;
        update_batches[s.server_id].batch_type = OpType::UPDATE;
    }

    uint32_t total_read_ops = 0, total_update_ops = 0;
    for (auto& [pk, ops] : vcs) {
        for (auto& op : ops) {
            if (op.type == OpType::READ) {
                read_batches[op.server_id].operations.push_back(op);
                total_read_ops++;
            } else if (op.type == OpType::UPDATE) {
                update_batches[op.server_id].operations.push_back(op);
                total_update_ops++;
            }
        }
    }
    auto t9 = clk::now();
    diag.batch_build_ms = ms_between(t8, t9);
    diag.committed_ops = total_read_ops + total_update_ops;

    for (size_t i = 0; i < servers.size(); ++i) {
        int sid = servers[i].server_id;
        diag.server_diags[i].read_ops =
            static_cast<uint32_t>(read_batches[sid].operations.size());
        diag.server_diags[i].update_ops =
            static_cast<uint32_t>(update_batches[sid].operations.size());
    }

    std::cout << "  [Ops] " << total_read_ops << " R + " << total_update_ops
              << " W = " << diag.committed_ops << " committed"
              << "  (txns: " << diag.committed_txns << "/"
              << diag.total_txns << ")\n";

    // ── Step 6: Batch execution ──
    auto batch_clock_start = clk::now();

    // Phase A: READs
    auto read_phase_start = clk::now();
    {
        std::vector<double> per_server_read_ms(servers.size(), 0.0);
        std::vector<TransmissionResult> read_results(servers.size());
        std::vector<std::thread> read_threads;
        for (size_t i = 0; i < servers.size(); ++i) {
            read_threads.emplace_back([&, i]() {
                auto s_start = clk::now();
                if (!read_batches[servers[i].server_id].operations.empty()) {
                    read_results[i] = multi_oram.sendBatchToServer(
                        servers[i], read_batches[servers[i].server_id]);
                } else {
                    read_results[i].server_id = servers[i].server_id;
                    read_results[i].success = true;
                    read_results[i].records_sent = 0;
                    read_results[i].elapsed_time_ms = 0;
                }
                auto s_end = clk::now();
                per_server_read_ms[i] = ms_between(s_start, s_end);
            });
        }
        for (auto& t : read_threads) t.join();
        for (auto& r : read_results)
            assert(r.success && "READ batch failed");
        for (size_t i = 0; i < servers.size(); ++i)
            diag.server_diags[i].read_phase_ms = per_server_read_ms[i];
    }
    auto read_phase_end = clk::now();
    diag.read_phase_wall_ms = ms_between(read_phase_start, read_phase_end);

    // Phase B: UPDATEs
    auto update_phase_start = clk::now();
    {
        std::vector<double> per_server_update_ms(servers.size(), 0.0);
        std::vector<TransmissionResult> update_results(servers.size());
        std::vector<std::thread> update_threads;
        for (size_t i = 0; i < servers.size(); ++i) {
            update_threads.emplace_back([&, i]() {
                auto s_start = clk::now();
                if (!update_batches[servers[i].server_id].operations.empty()) {
                    update_results[i] = multi_oram.sendBatchToServer(
                        servers[i], update_batches[servers[i].server_id]);
                } else {
                    update_results[i].server_id = servers[i].server_id;
                    update_results[i].success = true;
                    update_results[i].records_sent = 0;
                    update_results[i].elapsed_time_ms = 0;
                }
                auto s_end = clk::now();
                per_server_update_ms[i] = ms_between(s_start, s_end);
            });
        }
        for (auto& t : update_threads) t.join();
        for (auto& r : update_results)
            assert(r.success && "UPDATE batch failed");
        for (size_t i = 0; i < servers.size(); ++i)
            diag.server_diags[i].update_phase_ms = per_server_update_ms[i];
    }
    auto update_phase_end = clk::now();
    diag.update_phase_wall_ms =
        ms_between(update_phase_start, update_phase_end);

    auto batch_clock_end = clk::now();
    diag.batch_exec_ms = ms_between(batch_clock_start, batch_clock_end);

    diag.phases_sum_ms = diag.read_phase_wall_ms + diag.update_phase_wall_ms;
    diag.throughput_ops_sec = (diag.phases_sum_ms > 0)
        ? (diag.committed_ops * 1000.0 / diag.phases_sum_ms) : 0.0;

    std::cout << "  [Throughput] R=" << std::fixed << std::setprecision(2)
              << diag.read_phase_wall_ms << " + W="
              << diag.update_phase_wall_ms << " = "
              << diag.phases_sum_ms << " ms"
              << " → " << std::setprecision(1) << diag.throughput_ops_sec
              << " ops/sec\n";

    // Derived
    for (auto& sd : diag.server_diags) {
        sd.total_ms = sd.read_phase_ms + sd.update_phase_ms;
        sd.read_per_op_ms = (sd.read_ops > 0)
            ? (sd.read_phase_ms / sd.read_ops) : 0.0;
        sd.update_per_op_ms = (sd.update_ops > 0)
            ? (sd.update_phase_ms / sd.update_ops) : 0.0;
    }

    auto compute_straggler = [](const std::vector<ServerDiag>& sds,
                                 auto get_ops, auto get_ms,
                                 double& out_max, double& out_min,
                                 double& out_ratio) {
        std::vector<double> times;
        for (auto& sd : sds)
            if (get_ops(sd) > 0) times.push_back(get_ms(sd));
        if (!times.empty()) {
            out_max = *std::max_element(times.begin(), times.end());
            out_min = *std::min_element(times.begin(), times.end());
            out_ratio = (out_min > 0) ? (out_max / out_min) : 0.0;
        } else {
            out_max = out_min = out_ratio = 0;
        }
    };
    compute_straggler(diag.server_diags,
        [](const ServerDiag& s){ return s.read_ops; },
        [](const ServerDiag& s){ return s.read_phase_ms; },
        diag.read_max_ms, diag.read_min_ms, diag.read_straggler_ratio);
    compute_straggler(diag.server_diags,
        [](const ServerDiag& s){ return s.update_ops; },
        [](const ServerDiag& s){ return s.update_phase_ms; },
        diag.update_max_ms, diag.update_min_ms, diag.update_straggler_ratio);

    // Per-server table
    std::cout << "  Per-Server:\n";
    std::cout << "    " << std::left
              << std::setw(5) << "Srv"  << std::setw(8) << "Tree"
              << std::setw(6) << "R_op" << std::setw(9) << "R_ms"
              << std::setw(9) << "R/op" << std::setw(6) << "W_op"
              << std::setw(9) << "W_ms" << std::setw(9) << "W/op"
              << std::setw(9) << "Total" << "\n";
    std::cout << "    " << std::string(70, '-') << "\n";
    for (auto& sd : diag.server_diags) {
        std::cout << "    "
                  << std::setw(5) << sd.server_id
                  << std::setw(8) << sd.oram_tree_size
                  << std::setw(6) << sd.read_ops
                  << std::setw(9) << std::setprecision(2) << sd.read_phase_ms
                  << std::setw(9) << std::setprecision(3) << sd.read_per_op_ms
                  << std::setw(6) << sd.update_ops
                  << std::setw(9) << std::setprecision(2) << sd.update_phase_ms
                  << std::setw(9) << std::setprecision(3) << sd.update_per_op_ms
                  << std::setw(9) << std::setprecision(2) << sd.total_ms << "\n";
    }

    std::cout << "  Straggler: R=" << std::setprecision(2)
              << diag.read_straggler_ratio
              << "x  W=" << diag.update_straggler_ratio << "x";
    if (std::max(diag.read_straggler_ratio, diag.update_straggler_ratio) > 2.0)
        std::cout << "  ⚠️ LOAD_IMBAL";
    std::cout << "\n";

    multi_oram.shutdownAllServers();
    return diag;
}


// ############################################################################
//  MAIN — Workload: 100% Read
// ############################################################################

int main() {
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  ORAM Performance — Workload: 100% Read                              ║\n";
    std::cout << "║  Read=100%  Write=0%                                          ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n\n";

    // ========================================================================
    // Configuration
    // ========================================================================
    // Resolve path relative to this source file, regardless of build dir.
    // __FILE__ is the absolute (or relative-to-source-root) path at compile time.
    auto resolve_csv_path = []() -> std::string {
        std::string src_file = __FILE__;
        // Walk up to the project root (parent of the directory containing this .cpp)
        std::string dir = src_file;
        auto last_slash = dir.find_last_of("/\\");
        if (last_slash != std::string::npos) dir = dir.substr(0, last_slash);
        return dir + "/../../Testing/Unit Tests/test_data_500.csv";
    };
    std::string csv_path = resolve_csv_path();
    std::string schema = "customer_id:int32,region:int32,order_amount:int32,timestamp:int32";
    std::vector<int> attr = {1};
    int primary_key_col = 0;

    // Phase 1
    std::vector<uint32_t> tree_sizes_to_test = {
        25, 50, 75, 100, 125, 150, 200, 250, 300, 400, 500
    };
    int    BENCH_OPS_PER_BATCH = 20;
    int    BENCH_NUM_BATCHES   = 20;
    // OVERHEAD_TOLERANCE = 10.0: skewed queries raise per-op overhead because
    // hot blocks trigger more EarlyReshuffle calls. A high tolerance ensures
    // Phase 2 always selects T = 500 (the largest benchmark size), so FTH
    // X-axis = 500/1000/1500/2000/2500 — identical to the uniform baseline.
    double OVERHEAD_TOLERANCE  = 10.0;

    // DP epsilon budget allocation
    // FixedPartCount: split budget — 70% for bin noising, 30% for threshold noise
    double fpc_epsilon_1_ratio = 0.7;
    double fpc_epsilon_2_ratio = 0.3;
    // FixedThreshold: threshold is FIXED (no noise), so ALL budget goes to bin noising.
    // This gives better-quality noised synopses and stable, deterministic partition counts.
    double fth_epsilon_1_ratio = 1.0;
    double fth_epsilon_2_ratio = 0.0;  // epsilon_threshold=0 → skip threshold noise
    // Keep old names as aliases for backward compat with FPC calls
    double epsilon_1_ratio = fpc_epsilon_1_ratio;
    double epsilon_2_ratio = fpc_epsilon_2_ratio;
    std::vector<double> epsilon_values = {0.01, 0.1, 1.0, 10.0, 10000.0};  // 10000 ~ infinity

    // FixedThreshold scaling: test data_size = mult × T for each multiple.
    // The actual partition count will naturally be ceil(mult*T / T) = mult.
    // Data is cycled if mult*T exceeds the loaded CSV row count.
    std::vector<uint32_t> FTH_MULTIPLES = {1, 2, 3, 4, 5};

    // Phase 3 — FIXED ops per txn for consistent committed counts
    int NUM_CLIENTS        = 4;
    int TXNS_PER_CLIENT    = 5;
    int OPS_PER_TXN        = 3;     // fixed, not random
    int EPOCH_MS           = 500;
    int NUM_REPEATED_RUNS  = 5;     // repeat each case, keep median

    // ========================================================================
    // Load data
    // ========================================================================
    std::vector<std::string> data = readCSV(csv_path);
    std::cout << "Loaded " << data.size() << " records\n";
    std::cout << "Workload: 100% Read  (R=" << READ_PCT << "% W=" << WRITE_PCT << "%)\n\n";

    // ========================================================================
    // Pre-scan
    // ========================================================================
    print_separator("Pre-Scan: Max Servers for Phase 3");
    std::vector<uint32_t> prescan_hints = {1, 2, 3, 4, 5};
    uint32_t max_partitions_phase3 = 0;

    // FixedPartCount: vary hint_k with full dataset
    for (double eps : epsilon_values) {
        for (uint32_t ns : prescan_hints) {
            Repartition rep;
            auto parts = rep.Repartition_Main(
                data, schema, attr,
                epsilon_1_ratio * eps,
                epsilon_2_ratio * eps,
                0.0,
                true, ns, primary_key_col);
            max_partitions_phase3 = std::max(max_partitions_phase3,
                static_cast<uint32_t>(parts.size()));
        }
    }

    // FixedThreshold: vary data_size = mult * T_placeholder.
    // Use T_placeholder = data.size() / max(FTH_MULTIPLES) as a conservative
    // estimate before Phase 2 runs; actual T is plugged in later.
    {
        uint32_t T_placeholder = static_cast<uint32_t>(data.size())
                                 / FTH_MULTIPLES.back();
        T_placeholder = std::max(T_placeholder, 1u);
        for (double eps : epsilon_values) {
            for (uint32_t mult : FTH_MULTIPLES) {
                uint32_t sz = mult * T_placeholder;
                auto subset = get_data_subset(data, sz);
                Repartition rep;
                auto parts = rep.Repartition_Main(
                    subset, schema, attr,
                    fth_epsilon_1_ratio * eps,  // all ε to bin noising
                    fth_epsilon_2_ratio * eps,  // 0 → no threshold noise
                    static_cast<double>(T_placeholder),
                    false, 1, primary_key_col);
                max_partitions_phase3 = std::max(max_partitions_phase3,
                    static_cast<uint32_t>(parts.size()));
            }
        }
    }
    std::cout << "  Max servers needed: " << max_partitions_phase3 << "\n";
    std::cout << "  Start servers on ports 8881-"
              << (8880 + max_partitions_phase3) << "\n";
    std::cout << "  Press ENTER when ready...";
    std::cin.get();

    // ####################################################################
    //  PHASE 1
    // ####################################################################
    print_separator("PHASE 1: ORAM Benchmark — " + WORKLOAD_TAG);

    std::vector<OramBenchmark> benchmarks;
    for (uint32_t sz : tree_sizes_to_test) {
        std::cout << "  ── Tree size = " << sz << " ──\n";
        try {
            auto bm = benchmark_single_oram_size(
                data, schema, attr,
                epsilon_1_ratio, epsilon_2_ratio, 50.0,
                primary_key_col,
                sz, BENCH_OPS_PER_BATCH, BENCH_NUM_BATCHES);

            std::cout << "    Init: " << std::fixed << std::setprecision(1)
                      << bm.init_ms << " ms  |  R/op: " << std::setprecision(3)
                      << bm.avg_read_per_op_ms << " ms  |  W/op: "
                      << bm.avg_write_per_op_ms << " ms  |  Avg: "
                      << bm.avg_total_per_op_ms << " ms\n";
            benchmarks.push_back(bm);
        } catch (const std::exception& e) {
            std::cerr << "    ❌ FAILED: " << e.what() << "\n";
        }
    }

    // Compute overhead ratios
    if (!benchmarks.empty()) {
        double base_read  = benchmarks.front().avg_read_per_op_ms;
        double base_write = benchmarks.front().avg_write_per_op_ms;
        double base_total = benchmarks.front().avg_total_per_op_ms;
        for (auto& bm : benchmarks) {
            bm.read_overhead_ratio  = (base_read  > 0)
                ? (bm.avg_read_per_op_ms  / base_read)  : 1.0;
            bm.write_overhead_ratio = (base_write > 0)
                ? (bm.avg_write_per_op_ms / base_write) : 1.0;
            bm.total_overhead_ratio = (base_total > 0)
                ? (bm.avg_total_per_op_ms / base_total) : 1.0;
        }
    }

    // Phase 1 summary table
    print_separator("Phase 1 Summary — " + WORKLOAD_TAG);
    std::cout << "  " << std::left
              << std::setw(8) << "Records" << std::setw(10) << "Capacity"
              << std::setw(10) << "R/op_ms"  << std::setw(10) << "W/op_ms"
              << std::setw(10) << "Avg/op"   << std::setw(12) << "Ratio"
              << "\n";
    std::cout << "  " << std::string(60, '-') << "\n";
    for (auto& bm : benchmarks) {
        std::cout << "  " << std::fixed
                  << std::setw(8) << bm.num_records
                  << std::setw(10) << bm.oram_capacity
                  << std::setw(10) << std::setprecision(3) << bm.avg_read_per_op_ms
                  << std::setw(10) << std::setprecision(3) << bm.avg_write_per_op_ms
                  << std::setw(10) << std::setprecision(3) << bm.avg_total_per_op_ms
                  << std::setw(5) << std::setprecision(2) << bm.total_overhead_ratio
                  << "x";
        if (bm.total_overhead_ratio > OVERHEAD_TOLERANCE)
            std::cout << " ⚠️";
        std::cout << "\n";
    }

    // ####################################################################
    //  PHASE 2
    // ####################################################################
    print_separator("PHASE 2: Threshold — " + WORKLOAD_TAG);
    auto threshold = find_empirical_threshold(
        benchmarks, static_cast<uint32_t>(data.size()), OVERHEAD_TOLERANCE);

    std::cout << "  T = " << threshold.T_records << " records\n";
    std::cout << "  Baseline: " << std::fixed << std::setprecision(3)
              << threshold.baseline_per_op_ms << " ms/op\n";
    std::cout << "  Recommended k = " << threshold.recommended_k
              << "  (for " << data.size() << " records)\n\n";

    // Write Phase 1+2 CSV
    {
        std::string csv_bench = "oram_benchmark_" + WORKLOAD_TAG + ".csv";
        std::ofstream f(csv_bench);
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
        f.close();
        std::cout << "  CSV: " << csv_bench << "\n";
    }

    // ####################################################################
    //  PHASE 3
    // ####################################################################
    print_separator("PHASE 3: End-to-End — " + WORKLOAD_TAG);

    uint32_t k_opt = threshold.recommended_k;
    // Fixed k_values: always test 1–5 so the FPC chart has a complete X-axis
    // regardless of what k_opt the Phase-1 threshold finder returns.
    // Previously this was built dynamically around k_opt, which caused hint_k=4
    // to be silently skipped whenever k_opt happened to equal 2 or 5.
    std::vector<uint32_t> k_values = {1, 2, 3, 4, 5};

    std::cout << "  Recommended k = " << k_opt << "  Testing: ";
    for (size_t i = 0; i < k_values.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << k_values[i];
        if (k_values[i] == k_opt) std::cout << "*";
    }
    std::cout << "\n\n";

    std::vector<DiagResult> all_results;

    // ── FixedPartCount: vary hint k, fixed full dataset ─────────────────────
    print_separator("PHASE 3a: FixedPartCount — " + WORKLOAD_TAG);
    for (double eps : epsilon_values) {
        for (uint32_t k : k_values) {
            print_separator("FixedPartCount eps="
                + std::to_string(eps) + " k=" + std::to_string(k)
                + (k == k_opt ? " (EMP)" : ""));
            std::vector<DiagResult> repeated_runs;
            for (int run = 0; run < NUM_REPEATED_RUNS; ++run) {
                std::cout << "  [Run " << (run + 1) << "/" << NUM_REPEATED_RUNS << "]\n";
                try {
                    auto result = run_endtoend_test(
                        data, schema, attr,
                        epsilon_1_ratio, epsilon_2_ratio, eps,
                        static_cast<double>(threshold.T_records),
                        true, k, primary_key_col,
                        NUM_SERVERS,
                        NUM_CLIENTS, TXNS_PER_CLIENT, OPS_PER_TXN, EPOCH_MS);
                    repeated_runs.push_back(result);
                    std::cout << "    ✅ Run " << (run + 1) << " done"
                              << " (tput=" << std::fixed << std::setprecision(1)
                              << result.throughput_ops_sec << " ops/sec)\n";
                } catch (const std::exception& e) {
                    std::cerr << "    ❌ Run " << (run + 1) << " FAILED: " << e.what() << "\n";
                }
            }
            if (!repeated_runs.empty()) {
                auto median_result = average_diag_results(repeated_runs);
                all_results.push_back(median_result);
                std::cout << "  ✅ PASSED  (median of " << repeated_runs.size()
                          << " runs, tput=" << std::fixed << std::setprecision(1)
                          << median_result.throughput_ops_sec << " ops/sec)\n";
            } else {
                std::cerr << "  ❌ ALL " << NUM_REPEATED_RUNS << " RUNS FAILED — skipping\n";
            }
        }
    }

    // ── FixedThreshold: vary data_size = mult × T, hint_k unused ────────────
    // X-axis = data_size multiple.  Partition count p = ceil(mult*T / T) = mult.
    // This gives the meaningful scaling curve for FixedThreshold.
    print_separator("PHASE 3b: FixedThreshold scaling — " + WORKLOAD_TAG);
    std::cout << "  T = " << threshold.T_records << " records\n";
    std::cout << "  Testing data_size = ";
    for (size_t i = 0; i < FTH_MULTIPLES.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << FTH_MULTIPLES[i] << "×T="
                  << FTH_MULTIPLES[i] * threshold.T_records;
    }
    std::cout << "\n\n";

    for (double eps : epsilon_values) {
        for (uint32_t mult : FTH_MULTIPLES) {
            uint32_t data_size = mult * threshold.T_records;
            auto subset = get_data_subset(data, data_size);
            print_separator("FixedThreshold eps=" + std::to_string(eps)
                + " mult=" + std::to_string(mult)
                + " data_size=" + std::to_string(data_size));
            std::vector<DiagResult> repeated_runs;
            for (int run = 0; run < NUM_REPEATED_RUNS; ++run) {
                std::cout << "  [Run " << (run + 1) << "/" << NUM_REPEATED_RUNS << "]\n";
                try {
                    auto result = run_endtoend_test(
                        subset, schema, attr,
                        fth_epsilon_1_ratio, fth_epsilon_2_ratio, eps,
                        static_cast<double>(threshold.T_records),
                        false, mult, primary_key_col,
                        NUM_SERVERS,
                        NUM_CLIENTS, TXNS_PER_CLIENT, OPS_PER_TXN, EPOCH_MS);
                    repeated_runs.push_back(result);
                    std::cout << "    ✅ Run " << (run + 1) << " done"
                              << " (tput=" << std::fixed << std::setprecision(1)
                              << result.throughput_ops_sec << " ops/sec)\n";
                } catch (const std::exception& e) {
                    std::cerr << "    ❌ Run " << (run + 1) << " FAILED: " << e.what() << "\n";
                }
            }
            if (!repeated_runs.empty()) {
                auto median_result = average_diag_results(repeated_runs);
                all_results.push_back(median_result);
                std::cout << "  ✅ PASSED  (median of " << repeated_runs.size()
                          << " runs, p=" << median_result.num_partitions
                          << ", data_size=" << data_size
                          << ", tput=" << std::fixed << std::setprecision(1)
                          << median_result.throughput_ops_sec << " ops/sec)\n";
            } else {
                std::cerr << "  ❌ ALL " << NUM_REPEATED_RUNS << " RUNS FAILED — skipping\n";
            }
        }
    }

    // Write Phase 3 CSV
    std::string csv_e2e = "e2e_" + WORKLOAD_TAG + ".csv";
    {
        std::ofstream csv(csv_e2e);
        // Note: for FixedPartCount  → hint_k = the k value passed to Algorithm 3
        //       for FixedThreshold  → hint_k = data_size multiple (mult), so
        //                             data_size = hint_k × T_records
        csv << "workload,post_processing,epsilon,hint_k,actual_partitions,"
            << "is_empirical_k,"
            << "partition_real_counts,partition_dummy_counts,"
            << "partition_balance_cv,partition_max_min,"
            << "committed_ops,committed_read_ops,committed_write_ops,"
            << "phases_sum_ms,batch_exec_ms,throughput_ops_sec,"
            << "read_phase_wall_ms,update_phase_wall_ms,"
            << "read_straggler_ratio,update_straggler_ratio,"
            << "per_server_tree_sizes,"
            << "per_server_read_ms_per_op,per_server_update_ms_per_op\n";

        for (auto& d : all_results) {
            // Sum per-server read/write ops for the totals
            uint32_t sum_r = 0, sum_w = 0;
            for (auto& sd : d.server_diags) {
                sum_r += sd.read_ops;
                sum_w += sd.update_ops;
            }

            csv << std::fixed << WORKLOAD_TAG << ","
                << d.post_processing << ","
                << std::setprecision(4) << d.epsilon << ","
                << d.num_servers_hint << ","
                << d.num_partitions << ","
                << (d.num_servers_hint == k_opt ? "yes" : "no") << ",\"";
            for (size_t i = 0; i < d.partition_real_counts.size(); ++i) {
                if (i > 0) csv << ";";
                csv << d.partition_real_counts[i];
            }
            csv << "\",\"";
            for (size_t i = 0; i < d.partition_dummy_counts.size(); ++i) {
                if (i > 0) csv << ";";
                csv << d.partition_dummy_counts[i];
            }
            csv << "\","
                << std::setprecision(3) << d.partition_size_cv << ","
                << std::setprecision(2) << d.partition_max_min << ","
                << d.committed_ops << ","
                << sum_r << "," << sum_w << ","
                << std::setprecision(2) << d.phases_sum_ms << ","
                << d.batch_exec_ms << ","
                << std::setprecision(1) << d.throughput_ops_sec << ","
                << std::setprecision(2) << d.read_phase_wall_ms << ","
                << d.update_phase_wall_ms << ","
                << d.read_straggler_ratio << ","
                << d.update_straggler_ratio << ",\"";
            for (size_t i = 0; i < d.server_diags.size(); ++i) {
                if (i > 0) csv << ";";
                csv << d.server_diags[i].oram_tree_size;
            }
            csv << "\",\"";
            for (size_t i = 0; i < d.server_diags.size(); ++i) {
                if (i > 0) csv << ";";
                csv << std::setprecision(3) << d.server_diags[i].read_per_op_ms;
            }
            csv << "\",\"";
            for (size_t i = 0; i < d.server_diags.size(); ++i) {
                if (i > 0) csv << ";";
                csv << std::setprecision(3) << d.server_diags[i].update_per_op_ms;
            }
            csv << "\"\n";
        }
        csv.close();
    }

    // ========================================================================
    // Scaling Analysis
    // ========================================================================
    print_separator("Scaling — " + WORKLOAD_TAG);
    for (double eps : epsilon_values) {
        std::cout << "  ══ eps=" << std::fixed << std::setprecision(1) << eps << " ══\n";

        // FixedPartCount: baseline = p=1 with full dataset
        {
            double baseline = 0;
            for (auto& d : all_results) {
                if (d.post_processing == "FixedPartCount"
                    && std::abs(d.epsilon - eps) < 0.001
                    && d.num_partitions == 1) {
                    baseline = d.throughput_ops_sec; break;
                }
            }
            std::cout << "  FixedPartCount:\n";
            std::cout << "    " << std::left << std::setw(5) << "k"
                      << std::setw(5) << "p" << std::setw(10) << "Tput"
                      << std::setw(8) << "Speedup" << std::setw(8) << "Effic" << "\n";
            std::cout << "    " << std::string(36, '-') << "\n";
            for (auto& d : all_results) {
                if (d.post_processing != "FixedPartCount") continue;
                if (std::abs(d.epsilon - eps) > 0.001) continue;
                double ideal  = static_cast<double>(d.num_partitions);
                double actual = (baseline > 0) ? (d.throughput_ops_sec / baseline) : 0;
                double eff    = (ideal > 0) ? (actual / ideal * 100.0) : 0;
                std::string mark = (d.num_servers_hint == k_opt) ? " ◀(k★)" : "";
                std::cout << "    " << std::setw(5) << d.num_servers_hint
                          << std::setw(5) << d.num_partitions
                          << std::setw(10) << std::setprecision(1) << d.throughput_ops_sec
                          << std::setw(7) << std::setprecision(2) << actual << "x"
                          << std::setw(7) << std::setprecision(1) << eff << "%"
                          << mark << "\n";
            }
            std::cout << "\n";
        }

        // FixedThreshold: baseline = mult=1 (data_size = T, p=1)
        {
            double baseline = 0;
            for (auto& d : all_results) {
                if (d.post_processing == "FixedThreshold"
                    && std::abs(d.epsilon - eps) < 0.001
                    && d.num_servers_hint == 1) {   // mult=1 → data_size=T → p=1
                    baseline = d.throughput_ops_sec; break;
                }
            }
            std::cout << "  FixedThreshold  (T=" << threshold.T_records << "):\n";
            std::cout << "    " << std::left << std::setw(8) << "mult"
                      << std::setw(8) << "data_sz" << std::setw(5) << "p"
                      << std::setw(10) << "Tput"
                      << std::setw(8) << "Speedup" << std::setw(8) << "Effic" << "\n";
            std::cout << "    " << std::string(47, '-') << "\n";
            for (auto& d : all_results) {
                if (d.post_processing != "FixedThreshold") continue;
                if (std::abs(d.epsilon - eps) > 0.001) continue;
                uint32_t mult    = d.num_servers_hint;
                uint32_t data_sz = mult * threshold.T_records;
                double ideal     = static_cast<double>(d.num_partitions);
                double actual    = (baseline > 0) ? (d.throughput_ops_sec / baseline) : 0;
                double eff       = (ideal > 0) ? (actual / ideal * 100.0) : 0;
                std::cout << "    " << std::setw(8) << mult
                          << std::setw(8) << data_sz
                          << std::setw(5) << d.num_partitions
                          << std::setw(10) << std::setprecision(1) << d.throughput_ops_sec
                          << std::setw(7) << std::setprecision(2) << actual << "x"
                          << std::setw(7) << std::setprecision(1) << eff << "%" << "\n";
            }
            std::cout << "\n";
        }
    }

    std::cout << "  Output: " << csv_e2e << "\n";
    std::cout << "\n✅ " << WORKLOAD_TAG << " experiment complete.\n\n";
    return 0;
}