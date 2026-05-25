//
// Created by Xining Yuan on 5/13/26.
//
//
// Insert_Benchmark.cpp
// ----------------------------------------------------------------------
// Measures insert-only RingORAM fill performance for a given capacity N.
//
// Usage:
//   ./Insert_Benchmark <N> [csv_path] [port]
//
//     N         : number of records to insert (also used as ORAM n_blocks).
//                 ORAM height = ceil(log2(N)) + 1; total slot capacity ~ 8N.
//     csv_path  : optional, default = insert_benchmark.csv (appended to,
//                 header written once if file doesn't exist).
//     port      : optional, default = 8700 (matches phase1 server pool).
//
// What this binary measures:
//   - total wall-clock time to insert N records into an empty insert-only
//     RingORAM (capacity computed from N)
//   - throughput = N / total_time
//   - peak stash occupancy (high-watermark across the run)
//
// One single storage server (Servers_MultiRingORAM) must already be
// listening on <port> before launching this binary.
// ----------------------------------------------------------------------

#include <iostream>
#include <fstream>
#include <chrono>
#include <random>
#include <string>
#include <cstdint>
#include <vector>
#include <cmath>
#include <iomanip>

#include "RingORAM.h"
#include "NetIOConnector.h"

// ---- Configuration knobs ----
static constexpr uint32_t BUCKET_SIZE  = 8;
static constexpr uint32_t S_PARAM      = 4;
static constexpr uint32_t TUPLE_WIDTH  = 32;   // bytes per record before encryption
// block_size = tuple_width + AES::BLOCKSIZE + 2*sizeof(uint32_t)
// AES::BLOCKSIZE = 16, so block_size = 32 + 16 + 8 = 56 bytes
static constexpr uint32_t BLOCK_SIZE   = TUPLE_WIDTH + 16 + 2 * sizeof(uint32_t);

static std::string make_record_payload(uint32_t block_id,
                                       std::mt19937& rng,
                                       uint32_t tuple_width) {
    // Match RingORAM_helper.cpp:test_ringoram payload format:
    //   value = bID(4 bytes) + raw_tuple(tuple_width bytes)
    // So total payload size = 4 + tuple_width bytes.
    std::string value;
    value.reserve(tuple_width);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    for (uint32_t i = 0; i < tuple_width; i++) {
        value.push_back(static_cast<char>(byte_dist(rng)));
    }
    int32_t bid_signed = static_cast<int32_t>(block_id);
    std::string bID(reinterpret_cast<const char*>(&bid_signed), sizeof(uint32_t));
    return bID + value;   // total length = 4 + tuple_width
}

int main(int argc, char** argv) {
    if (argc < 2 || argc > 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <N> [csv_path] [port]\n"
                  << "  N         number of INSERTs (also = ORAM n_blocks)\n"
                  << "  csv_path  default = insert_benchmark.csv\n"
                  << "  port      default = 8700\n";
        return 1;
    }

    uint32_t N = 0;
    try { N = static_cast<uint32_t>(std::stoul(argv[1])); }
    catch (...) {
        std::cerr << "FATAL: cannot parse N from arg1='" << argv[1] << "'\n";
        return 1;
    }
    if (N < 2) {
        std::cerr << "FATAL: N must be >= 2 (got " << N << ")\n";
        return 1;
    }

    std::string csv_path = (argc >= 3) ? argv[2] : "insert_benchmark.csv";
    int port             = (argc >= 4) ? std::stoi(argv[3]) : 8700;

    // ────────────────────────────────────────────────────────────────────
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  Insert-Only RingORAM Benchmark                              ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "  N (records to insert)  : " << N << "\n";
    std::cout << "  Bucket size            : " << BUCKET_SIZE << "  (Z=" << (BUCKET_SIZE - S_PARAM)
              << ", S=" << S_PARAM << ")\n";
    std::cout << "  Tuple width            : " << TUPLE_WIDTH << " bytes\n";
    std::cout << "  Block size             : " << BLOCK_SIZE << " bytes (with AES + 2×u32)\n";

    uint32_t height = static_cast<uint32_t>(std::ceil(std::log2(static_cast<double>(N)))) + 1;
    uint32_t num_leaves = 1u << (height - 1);
    uint32_t total_slots = ((1u << height) - 1) * (BUCKET_SIZE - S_PARAM);
    std::cout << "  Height (derived)       : " << height << "\n";
    std::cout << "  Num leaves             : " << num_leaves << "\n";
    std::cout << "  Total real slots       : " << total_slots
              << "  (~" << (total_slots / static_cast<double>(N)) << "× dataset)\n";
    std::cout << "  CSV output path        : " << csv_path << "\n";
    std::cout << "  Storage server port    : " << port << "\n";
    std::cout << "\n";

    // ── 1. Connect to storage server ────────────────────────────────────
    std::cout << "[1/5] Connecting to storage server on 127.0.0.1:" << port << "...\n";
    std::string oram_name = "InsertBench_N" + std::to_string(N);
    NetIOConnector* conn = nullptr;
    try {
        conn = new NetIOConnector("127.0.0.1", port, oram_name);
    } catch (const std::exception& e) {
        std::cerr << "FATAL: failed to connect: " << e.what() << "\n";
        return 1;
    }
    std::cout << "      ✓ connected\n";

    // ── 2. Initialize empty RingORAM ────────────────────────────────────
    std::cout << "[2/5] Initializing empty RingORAM (capacity=" << N << ")...\n";
    auto t_init_start = std::chrono::high_resolution_clock::now();
    RingORAM oram(N, BUCKET_SIZE, oram_name, BLOCK_SIZE, conn, S_PARAM);
    auto t_init_end = std::chrono::high_resolution_clock::now();
    double init_ms = std::chrono::duration<double, std::milli>(t_init_end - t_init_start).count();
    std::cout << "      ✓ ORAM ready in " << init_ms << " ms\n";

    // ── 3. INSERT N records, measure wall-clock ─────────────────────────
    std::cout << "[3/5] Inserting " << N << " records...\n";
    std::mt19937 rng(42);  // deterministic seed
    std::string payload;

    auto t_fill_start = std::chrono::high_resolution_clock::now();
    auto t_progress_last = t_fill_start;
    uint32_t progress_step = std::max<uint32_t>(N / 20, 1);  // 5% progress

    for (uint32_t block_id = 0; block_id < N; block_id++) {
        payload = make_record_payload(block_id, rng, TUPLE_WIDTH);
        oram.access(block_id, OpType::INSERT, payload);

        if ((block_id + 1) % progress_step == 0 || block_id + 1 == N) {
            auto now = std::chrono::high_resolution_clock::now();
            double elapsed_s = std::chrono::duration<double>(now - t_fill_start).count();
            double interval_s = std::chrono::duration<double>(now - t_progress_last).count();
            t_progress_last = now;
            uint32_t inserted = block_id + 1;
            double rate = inserted / elapsed_s;
            std::cout << "      [" << inserted << "/" << N << "] "
                      << std::fixed << std::setprecision(1)
                      << (100.0 * inserted / N) << "%  "
                      << "elapsed=" << elapsed_s << "s  "
                      << "rate=" << rate << " ops/s  "
                      << "stash=" << oram.get_stash_size()
                      << " (peak " << oram.stash_high_watermark << ")\n";
            std::cout.flush();
        }
    }
    auto t_fill_end = std::chrono::high_resolution_clock::now();
    double fill_ms = std::chrono::duration<double, std::milli>(t_fill_end - t_fill_start).count();

    uint32_t peak_stash_during_fill = oram.stash_high_watermark;
    uint32_t stash_at_end_of_fill   = oram.get_stash_size();
    std::cout << "      ✓ " << N << " inserts complete in "
              << std::fixed << std::setprecision(2) << fill_ms << " ms\n";
    std::cout << "      stash at end-of-fill      : " << stash_at_end_of_fill << "\n";
    std::cout << "      peak stash during fill    : " << peak_stash_during_fill << "\n";

    // ── 4. Drain stash (final flush before "ORAM is full") ──────────────
    std::cout << "[4/5] Final stash drain...\n";
    auto t_drain_start = std::chrono::high_resolution_clock::now();
    oram.flush_stash_if_needed();
    auto t_drain_end = std::chrono::high_resolution_clock::now();
    double drain_ms = std::chrono::duration<double, std::milli>(t_drain_end - t_drain_start).count();
    uint32_t stash_after_drain = oram.get_stash_size();
    uint32_t peak_stash_total  = oram.stash_high_watermark;
    std::cout << "      drain took " << drain_ms << " ms; stash now " << stash_after_drain << "\n";

    double total_ms       = fill_ms + drain_ms;
    double tput_fill_only = (N * 1000.0) / fill_ms;
    double tput_including = (N * 1000.0) / total_ms;

    std::cout << "\n";
    std::cout << "  ─── RESULTS ──────────────────────────────────────────\n";
    std::cout << "  Fill time            : " << fill_ms       << " ms\n";
    std::cout << "  Drain time           : " << drain_ms      << " ms\n";
    std::cout << "  Total time (fill+drain): " << total_ms    << " ms\n";
    std::cout << "  Throughput (fill only) : " << tput_fill_only << " ops/sec\n";
    std::cout << "  Throughput (fill+drain): " << tput_including << " ops/sec\n";
    std::cout << "  Peak stash           : " << peak_stash_total << "\n";

    // ── 5. Append result to CSV ─────────────────────────────────────────
    std::cout << "[5/5] Writing CSV...\n";
    bool csv_exists_with_data = false;
    {
        std::ifstream check(csv_path);
        if (check.is_open()) {
            check.seekg(0, std::ios::end);
            csv_exists_with_data = (check.tellg() > 0);
        }
    }
    std::ofstream csv_f(csv_path, std::ios::app);
    if (!csv_f.is_open()) {
        std::cerr << "WARNING: cannot open " << csv_path << " for append\n";
    } else {
        if (!csv_exists_with_data) {
            csv_f << "N,height,num_leaves,total_real_slots,"
                  << "init_ms,fill_ms,drain_ms,total_ms,"
                  << "throughput_fill_only_ops_per_sec,"
                  << "throughput_fill_plus_drain_ops_per_sec,"
                  << "stash_at_end_of_fill,stash_after_drain,peak_stash\n";
        }
        csv_f << N << ","
              << height << ","
              << num_leaves << ","
              << total_slots << ","
              << std::fixed << std::setprecision(2)
              << init_ms << ","
              << fill_ms << ","
              << drain_ms << ","
              << total_ms << ","
              << std::setprecision(1)
              << tput_fill_only << ","
              << tput_including << ","
              << stash_at_end_of_fill << ","
              << stash_after_drain << ","
              << peak_stash_total << "\n";
        csv_f.flush();
        std::cout << "      ✓ row appended to " << csv_path << "\n";
    }

    // Cleanup
    delete conn;
    std::cout << "\nDone.\n";
    return 0;
}