//
// Created by Xining Yuan on 2/18/26.
// Modified: per-server process model + fixed batch size with padding
//
// Architecture
// ─────────────
//  Parent (benchmark) ─── pipe ──► Child process i
//                                      owns NetIOConnector_i
//                                      owns RingORAM_i
//                      ◄── pipe ─── sends results back
//
// IPC protocol (parent → child, to_child pipe)
//   [uint32_t num_ops]          0 → shutdown
//   per op (num_ops times):
//     [uint8_t  type]           OpType cast to uint8
//     [uint32_t key]
//     [uint32_t val_len]
//     [char...  val]            val_len bytes
//     [uint8_t  is_dummy]       1 = padding dummy
//
// IPC protocol (child → parent, from_child pipe)
//   [uint32_t real_ops_done]    non-dummy ops processed
//   [double   elapsed_ms]       wall-clock inside child
//   [uint8_t  ok]               1 = success
//
// Fixed-batch guarantee
//   Every sub-batch sent to a child is exactly FIXED_BATCH_SIZE ops.
//   If a real batch has > FIXED_BATCH_SIZE ops it is split into multiple
//   sub-batches.  The last sub-batch is padded with dummy READ ops.
//

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstring>
#include <cassert>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
#ifdef __linux__
  #include <sys/prctl.h>   // PR_SET_PDEATHSIG — auto-kill child on parent death
#endif
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <iomanip>
#include <unordered_map>
#include "MultiRingORAM_Servers.h"

#define BUFFER_SIZE    4096
#define DEFAULT_STARTING_PORT 8881

// ============================================================================
// Globals
// ============================================================================
std::vector<pid_t> server_pids;
std::mutex         cout_mutex;

void safe_print(const std::string& msg) {
    std::lock_guard<std::mutex> lk(cout_mutex);
    std::cout << msg << std::flush;
}

void cleanup_servers(int signum) {
    std::cout << "\n\n🛑 Shutting down servers..." << std::endl;
    for (pid_t pid : server_pids) {
        kill(pid, SIGTERM);
        waitpid(pid, nullptr, 0);
    }
    exit(signum);
}

// ============================================================================
// CSV Reader
// ============================================================================
std::vector<std::string> readCSV(const std::string& filename) {
    std::vector<std::string> data;
    std::ifstream file(filename);
    if (!file.is_open())
        throw std::runtime_error("Failed to open file: " + filename);
    std::string line;
    std::getline(file, line); // skip header
    while (std::getline(file, line))
        if (!line.empty()) data.push_back(line);
    return data;
}

// ============================================================================
// Server Management (NetIO storage servers — unchanged)
// ============================================================================
pid_t startServer(int port, const std::string& server_exec) {
    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "❌ Failed to fork server process for port " << port << std::endl;
        return -1;
    }
    if (pid == 0) {
        // ── Zombie prevention ────────────────────────────────────────────
        // If parent (TPCC_Main) crashes / is OOM-killed / exits without a
        // clean shutdown, the kernel sends SIGTERM to this child immediately.
        // Without this, orphaned server processes accumulate across failed
        // runs and monopolize ports 8800-9500, causing the next run to
        // connect to stale ORAMs and hang. PR_SET_PDEATHSIG survives the
        // execl() below (assuming the server binary is not setuid).
        // Linux-only — macOS has no equivalent; not a problem since the
        // EC2 benchmark host is Linux. Local macOS dev builds skip it.
#ifdef __linux__
        prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

        std::string log_file = "server_" + std::to_string(port) + ".log";
        freopen(log_file.c_str(), "w", stdout);
        freopen(log_file.c_str(), "w", stderr);
        std::string port_str = std::to_string(port);
        execl(server_exec.c_str(), server_exec.c_str(), port_str.c_str(), nullptr);
        std::cerr << "❌ Failed to execute server: " << server_exec << std::endl;
        exit(1);
    }
    return pid;
}

bool waitForServer(int port, int max_attempts) {
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd < 0) continue;
        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (connect(sock_fd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            close(sock_fd);
            return true;
        }
        close(sock_fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

// ============================================================================
// Pipe helpers
// ============================================================================
bool MultiRingORAM_Servers::write_all(int fd, const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w <= 0) return false;
        p += w; n -= static_cast<size_t>(w);
    }
    return true;
}

bool MultiRingORAM_Servers::read_all(int fd, void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r <= 0) return false;
        p += r; n -= static_cast<size_t>(r);
    }
    return true;
}

// ============================================================================
// Data Distribution
// ============================================================================
void MultiRingORAM_Servers::distributeDataToPartitions(
    const BinInfo& bin_info,
    const std::vector<Partition>& partitions,
    std::vector<ServerInfo>& servers)
{
    std::cout << "\n📦 Step 3: Distributing data to partitions...\n";

    // ── Reset pos_map (4/26/26) ──────────────────────────────────────────
    // Defensive: if distributeDataToPartitions is ever called more than
    // once (e.g., re-init for a fresh experiment), don't accumulate stale
    // mappings.
    pk_to_blockid_.clear();

    for (uint32_t part_id = 0; part_id < partitions.size(); ++part_id) {
        const Partition& part = partitions[part_id];
        ServerInfo& server    = servers[part_id];

        partition_id_to_server_id_[part_id] = server.server_id;
        server_id_to_partition_id_[server.server_id] = part_id;

        // ── pos_map population (4/26/26) ──────────────────────────────────
        // Replay the EXACT iteration order that ServerInitialization uses
        // when calling oram->access(block_counter, INSERT, ...).  Both
        // loops walk part.index → bin_info.bin_key_to_data_indices.at(...)
        // in the same order, so block_counter increments lockstep.  We
        // record (server_id, primary_key) → block_id here so that later
        // ops can be routed correctly.
        //
        // NOTE: dummy padding records (Case A: part.dummy_num > 0) are NOT
        // entered into the map.  They have no application-level PK and
        // are never the target of a real op.
        auto& server_pkmap = pk_to_blockid_[server.server_id];
        uint32_t block_counter = 0;

        for (const auto& bin_key : part.index) {
            bin_key_to_partition_id_[bin_key] = part_id;
            auto it = bin_info.bin_key_to_data_indices.find(bin_key);
            if (it != bin_info.bin_key_to_data_indices.end()) {
                for (uint32_t data_idx : it->second) {
                    server.assigned_data_indices.push_back(data_idx);

                    // pk_mapper_ defaults to identity (pk == data_idx).
                    // The std::string here is unused by the default mapper;
                    // we don't have access to the data vector at this point,
                    // so we pass empty.  If the application overrides
                    // pk_mapper_ to parse from record bytes, the caller
                    // should call setPrimaryKeyMapper BEFORE this function
                    // AND ensure it works without record bytes (or call
                    // populatePkMapFromData separately — see TODO below).
                    uint32_t pk = pk_mapper_(data_idx, std::string{});
                    server_pkmap[pk] = block_counter;
                    ++block_counter;
                }
            }
        }
        // block_counter now equals the number of REAL records on this
        // partition.  ServerInitialization will continue from
        // block_counter onward for dummy padding (block_counter ..
        // block_counter + part.dummy_num - 1), but those don't need
        // map entries.

        std::cout << "  Server " << server.server_id
                  << " (Partition " << part_id << "): "
                  << server.assigned_bins.size() << " bins, "
                  << server.assigned_data_indices.size() << " records"
                  << "  pk_map_size=" << server_pkmap.size() << "\n";
    }
    std::cout << "✅ Data distribution complete.\n";
}

// ============================================================================
// pos_map lookup (4/26/26)
// ============================================================================
uint32_t MultiRingORAM_Servers::lookupBlockId(int server_id,
                                              uint32_t primary_key) const
{
    auto sit = pk_to_blockid_.find(server_id);
    if (sit == pk_to_blockid_.end()) return UINT32_MAX;
    auto pit = sit->second.find(primary_key);
    if (pit == sit->second.end()) return UINT32_MAX;
    return pit->second;
}

void MultiRingORAM_Servers::printPkMapStats() const {
    std::cout << "[pos_map] " << pk_to_blockid_.size() << " server(s):";
    for (const auto& [sid, m] : pk_to_blockid_) {
        std::cout << "  s" << sid << "=" << m.size() << " entries";
    }
    std::cout << std::endl;
}

// ============================================================================
// Server Process Initialization
// ============================================================================
// Child event loop (runs inside forked child process).
// oram is already initialized; child reads batches and executes them.
static void child_event_loop(RingORAM* oram, int read_fd, int write_fd) {
    using clk = std::chrono::high_resolution_clock;

    // Reusable helper for var-length writes (handles partial-write loop).
    auto write_all_local = [](int fd, const void* buf, size_t n) -> bool {
        const char* p = static_cast<const char*>(buf);
        size_t sent = 0;
        while (sent < n) {
            ssize_t w = write(fd, p + sent, n - sent);
            if (w <= 0) return false;
            sent += static_cast<size_t>(w);
        }
        return true;
    };

    while (true) {
        // ── Read num_ops (0 = shutdown) ──────────────────────────────────
        uint32_t num_ops = 0;
        ssize_t r = read(read_fd, &num_ops, sizeof(uint32_t));
        if (r <= 0 || num_ops == 0) break;   // shutdown

        // ── Execute ops ──────────────────────────────────────────────────
        auto t0 = clk::now();
        uint32_t real_ops = 0;

        // ── FIX (4/26/26): capture each access() return value ─────────────
        // For READ ops, access() returns the decrypted plaintext.  Previously
        // this was discarded, which made TPC-C transactions impossible (proxy
        // had no way to read row data through the ORAM).  We now collect each
        // result and stream them back after the summary header.
        std::vector<std::string> op_results;
        op_results.reserve(num_ops);

        for (uint32_t j = 0; j < num_ops; ++j) {
            uint8_t  type_byte;
            uint32_t key;
            uint32_t val_len;
            uint8_t  is_dummy;

            // Read fields
            if (read(read_fd, &type_byte, 1) != 1)          goto child_error;
            if (read(read_fd, &key,       4) != 4)          goto child_error;
            if (read(read_fd, &val_len,   4) != 4)          goto child_error;
            {
                std::string val(val_len, '\0');
                if (val_len > 0) {
                    size_t offset = 0;
                    while (offset < val_len) {
                        ssize_t got = read(read_fd, &val[offset], val_len - offset);
                        if (got <= 0) goto child_error;
                        offset += static_cast<size_t>(got);
                    }
                }
                if (read(read_fd, &is_dummy, 1) != 1)       goto child_error;

                OpType op_type = static_cast<OpType>(type_byte);

                // ── FIX (4/26/26): UPDATE val must carry bID(4) prefix ────
                // The convention (matches test_ringoram + ServerInitialization)
                // is that data_in for INSERT/UPDATE is `bID(4) + tuple_data`.
                // If proxy forgets the prefix, the bID stored in the cipher
                // is wrong and READ verification silently corrupts.  We log
                // once instead of aborting so a single misconfigured op does
                // not kill the whole benchmark.
                if (op_type == OpType::UPDATE && val_len > 0) {
                    static uint64_t bad_update = 0;
                    static uint64_t bad_warned = 0;
                    if (val_len < sizeof(uint32_t)) {
                        if (bad_warned == 0) {
                            std::cerr << "[child_event_loop] WARNING: UPDATE "
                                      << "val_len=" << val_len
                                      << " < 4 bytes; expected bID(4)+data."
                                      << "  Check proxy prepends block_id.\n";
                            bad_warned++;
                        }
                    } else {
                        int32_t embedded_bid = 0;
                        memcpy(&embedded_bid, val.data(), sizeof(int32_t));
                        if (static_cast<uint32_t>(embedded_bid) != key) {
                            bad_update++;
                            if (bad_update == 1 || bad_update % 1000 == 0) {
                                std::cerr << "[child_event_loop] WARNING: "
                                          << "UPDATE val bID(" << embedded_bid
                                          << ") != key(" << key
                                          << ").  Proxy may have forgotten "
                                          << "to prepend block_id; tally="
                                          << bad_update << "\n";
                            }
                        }
                    }
                }

                // Capture access() return value:
                //   READ   → plaintext (caller may need to truncate trailing
                //            zero padding to expected tuple width)
                //   UPDATE → "" (empty)
                //   INSERT → "" (empty)
                std::string acc = oram->access(key, op_type, val);
                op_results.push_back(std::move(acc));
                if (!is_dummy) ++real_ops;
            }
        }

        {
            auto t1 = clk::now();
            double elapsed_ms =
                std::chrono::duration<double, std::milli>(t1 - t0).count();
            uint8_t ok = 1;

            // ── Summary header (existing protocol) ───────────────────────
            if (!write_all_local(write_fd, &real_ops,   sizeof(uint32_t))) goto child_error;
            if (!write_all_local(write_fd, &elapsed_ms, sizeof(double)))    goto child_error;
            if (!write_all_local(write_fd, &ok,         sizeof(uint8_t)))   goto child_error;

            // ── Per-op results (NEW, only sent when ok==1) ───────────────
            // Layout per op: [u32 result_len] [result_len bytes plaintext]
            // Order matches input batch order.  result_len==0 for UPDATE/INSERT.
            for (const auto& res : op_results) {
                uint32_t result_len = static_cast<uint32_t>(res.size());
                if (!write_all_local(write_fd, &result_len, sizeof(uint32_t))) goto child_error;
                if (result_len > 0 &&
                    !write_all_local(write_fd, res.data(), result_len))
                                                                              goto child_error;
            }
        }
        continue;

    child_error:
        {
            uint32_t z = 0; double d = 0.0; uint8_t fail = 0;
            // Best-effort error response: matches old protocol so parent
            // sees ok=0 and aborts.  No per-op results follow.
            (void)write(write_fd, &z,    sizeof(uint32_t));
            (void)write(write_fd, &d,    sizeof(double));
            (void)write(write_fd, &fail, sizeof(uint8_t));
        }
        break;
    }

    delete oram;
    close(read_fd);
    close(write_fd);
    exit(0);
}

void MultiRingORAM_Servers::ServerInitialization(
    const ServerInfo& server,
    const Partition&  part,
    const BinInfo&    bin_info,
    const std::vector<std::string>& data,
    const std::string& schema_str,
    uint32_t bucket_size,
    std::string oram_name,
    uint32_t block_length,
    const std::string& host,
    uint32_t S_input)
{
    // ── Create pipe pairs ─────────────────────────────────────────────────
    int to_child_fds[2], from_child_fds[2];
    if (pipe(to_child_fds) < 0 || pipe(from_child_fds) < 0) {
        throw std::runtime_error("pipe() failed for server " +
                                 std::to_string(server.server_id));
    }

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed for server " +
                                 std::to_string(server.server_id));
    }

    if (pid == 0) {
        // ── CHILD ────────────────────────────────────────────────────────
        // Zombie prevention: if parent dies unexpectedly (OOM, crash, SIGINT),
        // kernel sends SIGTERM to us immediately so we don't become an orphan
        // holding onto a TCP port and ORAM state indefinitely.
        // Linux-only; macOS builds skip it harmlessly.
#ifdef __linux__
        prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

        close(to_child_fds[1]);    // child doesn't write to to_child
        close(from_child_fds[0]);  // child doesn't read from from_child
        int read_fd  = to_child_fds[0];
        int write_fd = from_child_fds[1];

        // ── Close ALL pipe fds inherited from previously-forked siblings ──
        // When parent already created pipes for servers 0..N-1 and then
        // forked us (server N), we inherited those fds. Close them so EOF
        // detection and pipe lifecycle work correctly.
        for (auto& [sid, sp] : server_processes_) {
            if (sp.to_child   != -1) close(sp.to_child);
            if (sp.from_child != -1) close(sp.from_child);
        }

        // ── Wrap all child work in try-catch so parent is always notified ──
        try {
            NetIOConnector* conn = new NetIOConnector(
                host.c_str(), server.port, oram_name);

            // ── FIX (4/26/26): clear server-side stash for this ORAM ────
            // server_storage is process-global on the server side; without
            // this clear() any leftover slots from a previous run on this
            // (oram_name, port) combination would persist past the new
            // tree's slot range — invisible bytes that bloat memory across
            // benchmark sweeps.  Especially matters when noisy_synopsis
            // changes between runs (different epsilon → different tree size).
            conn->clear(oram_name);

            uint32_t tuple_width_bytes = tupleWidthBytesFromSchema(schema_str);

            // ── ORAM capacity ─────────────────────────────────────────────
            // Privacy invariant: ORAM tree is ALWAYS sized to noisy_synopsis.
            //
            //   Case A (noisy >= real): noisy_synopsis = real + dummy_num
            //     → tree holds real + dummy records (unchanged behaviour)
            //
            //   Case B (noisy < real):  noisy_synopsis < real
            //     → tree is smaller than the real record count.
            //     → stash_overflow_count = real - noisy records permanently
            //       live in the stash (RingORAM stash_limit is pre-enlarged).
            //     → server sees the same noisy-sized tree regardless of noise
            //       sign, closing the tree-height side channel.
            uint32_t total_data_num = part.noisy_synopsis;

            RingORAM* oram = new RingORAM(
                total_data_num, bucket_size, oram_name, block_length, conn, S_input,
                part.stash_overflow_count);   // permanent_stash_reserve for Case B

            // ── Insert real records ───────────────────────────────────────
            // Insert ALL real records unconditionally.
            //
            // Case A: dummy_num > 0; tree has room for all real records.
            // Case B: stash_overflow_count > 0; tree is sized to noisy < real.
            //   The first ~noisy records fill the tree via normal eviction;
            //   the remaining stash_overflow_count records cannot evict into
            //   the tree (it's full) and permanently reside in the stash.
            //   RingORAM.stash_limit has been enlarged to accommodate them.
            uint32_t block_counter = 0;

            for (const auto& bin_key : part.index) {
                const auto& data_indices = bin_info.bin_key_to_data_indices.at(bin_key);
                for (uint32_t data_idx : data_indices) {
                    std::string value = data[data_idx];
                    value.resize(tuple_width_bytes);
                    int32_t blockID = block_counter;
                    std::string bID((const char*)(&blockID), sizeof(uint32_t));
                    value = bID + value;
                    oram->access(block_counter, OpType::INSERT, value);
                    block_counter++;
                }
            }

            // Insert dummy padding (Case A only; Case B: dummy_num == 0 → no-op)
            for (uint32_t d = 0; d < part.dummy_num; d++) {
                std::string dummy_value(block_length, '\0');
                dummy_value.resize(tuple_width_bytes);
                int32_t blockID = block_counter;
                std::string bID((const char*)(&blockID), sizeof(uint32_t));
                dummy_value = bID + dummy_value;
                oram->access(block_counter, OpType::INSERT, dummy_value);
                block_counter++;
            }

            std::cout << "[ServerInit] Inserted "
                      << (block_counter - part.dummy_num) << " real + "
                      << part.dummy_num << " dummy records."
                      << "  stash_overflow=" << part.stash_overflow_count
                      << "  (permanently in stash)\n";

            // ── Post-init stash flush ─────────────────────────────────────
            // Drain stash entries pushed in by Phase 2 (and normal INSERT eviction).
            // After this the stash is at most A entries — the normal operating level.
            oram->flush_stash_if_needed();

            // Signal parent: init succeeded
            uint8_t ready = 1;
            write(write_fd, &ready, sizeof(uint8_t));

            // Enter event loop (calls exit(0) internally)
            child_event_loop(oram, read_fd, write_fd);

        } catch (const std::exception& e) {
            // Signal parent: init failed with 0 so it doesn't hang forever
            std::cerr << "[Server " << server.server_id << "] INIT ERROR: "
                      << e.what() << std::endl;
            uint8_t failed = 0;
            write(write_fd, &failed, sizeof(uint8_t));
            close(read_fd);
            close(write_fd);
            exit(1);
        }
        // child_event_loop calls exit(0) — we never reach here
    }

    // ── PARENT ───────────────────────────────────────────────────────────
    close(to_child_fds[0]);    // parent doesn't read from to_child
    close(from_child_fds[1]);  // parent doesn't write to from_child

    ServerProcess sp;
    sp.pid        = pid;
    sp.to_child   = to_child_fds[1];
    sp.from_child = from_child_fds[0];

    server_processes_[server.server_id] = sp;
}

// ============================================================================
//  ServerInitializationBulk (4/18/26)  —  fast-path TPC-C init
// ============================================================================
//
//  Purpose
//  -------
//  The original ServerInitialization initializes RingORAM by calling
//  insert_slot() millions of times (once per bucket×slot for dummy init,
//  plus once per record insert). On localhost this is still bottlenecked
//  by TCP round-trips and takes hours for warehouse=5.
//
//  This alternate path:
//    1. Uses silent mode on NetIOConnector so RingORAM's constructor
//       populates metadata locally but does NOT send any dummy slots.
//    2. Generates the same dummy content locally and uploads it all in
//       ONE bulk_init_tree() send_data call.
//    3. Then performs real-record access(INSERT) as usual (this part
//       is unchanged and still uses per-slot insert_slot, but its cost
//       is much smaller than the dummy init loop was).
//
//  On-server state after this function is byte-equivalent to what the
//  original ServerInitialization produces.
//
//  NOTE: The insert-only RingORAM scenario at runtime must continue to use
//  the ORIGINAL ServerInitialization path.
// ============================================================================
void MultiRingORAM_Servers::ServerInitializationBulk(
    const ServerInfo& server,
    const Partition&  part,
    const BinInfo&    bin_info,
    const std::vector<std::string>& data,
    const std::string& schema_str,
    uint32_t bucket_size,
    std::string oram_name,
    uint32_t block_length,
    const std::string& host,
    uint32_t S_input)
{
    // ── Pipe + fork (identical to ServerInitialization) ──────────────────
    int to_child_fds[2], from_child_fds[2];
    if (pipe(to_child_fds) < 0 || pipe(from_child_fds) < 0) {
        throw std::runtime_error("pipe() failed for server " +
                                 std::to_string(server.server_id));
    }

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed for server " +
                                 std::to_string(server.server_id));
    }

    if (pid == 0) {
        // ── CHILD PROCESS ────────────────────────────────────────────────
        // Zombie prevention: see ServerInitialization for rationale.
        // Linux-only; macOS builds skip it harmlessly.
#ifdef __linux__
        prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

        close(to_child_fds[1]);
        close(from_child_fds[0]);
        int read_fd  = to_child_fds[0];
        int write_fd = from_child_fds[1];

        for (auto& [sid, sp] : server_processes_) {
            if (sp.to_child   != -1) close(sp.to_child);
            if (sp.from_child != -1) close(sp.from_child);
        }

        try {
            NetIOConnector* conn = new NetIOConnector(
                host.c_str(), server.port, oram_name);

            // ── FIX (4/26/26): clear server-side stash for this ORAM ────
            // Same reason as ServerInitialization — server_storage is
            // process-global, stale slots from previous runs leak.  Doubly
            // important here because BulkInit is the path used for benchmark
            // sweeps over (epsilon × workload × variant) where tree sizes
            // legitimately change between runs.
            conn->clear(oram_name);

            uint32_t tuple_width_bytes = tupleWidthBytesFromSchema(schema_str);
            uint32_t total_data_num = part.noisy_synopsis;

            // ── STEP 1: Silent-mode RingORAM construction ────────────────
            // RingORAM constructor runs normally (sets up height, stash,
            // bucket_blockdata, encryption keys, pos_map), but the per-slot
            // insert_slot loop is a no-op because conn is silent.
            conn->set_silent(true);

            RingORAM* oram = new RingORAM(
                total_data_num, bucket_size, oram_name, block_length, conn, S_input,
                part.stash_overflow_count);

            conn->set_silent(false);

            std::cout << "[BulkInit] Silent-mode ctor done "
                      << "(metadata ready, 0 bytes sent)\n";

            // ── STEP 2: Build the record list ────────────────────────────
            // All real records will go directly into the ORAM's stash, and
            // we'll upload a fresh dummy tree to the server. No per-record
            // access(INSERT), no eviction, no stash explosion.
            std::vector<std::pair<uint32_t, std::string>> records;
            records.reserve(part.index.size() * 64);   // rough lower bound

            uint32_t block_counter = 0;
            for (const auto& bin_key : part.index) {
                const auto& data_indices = bin_info.bin_key_to_data_indices.at(bin_key);
                for (uint32_t data_idx : data_indices) {
                    std::string value = data[data_idx];
                    value.resize(tuple_width_bytes);
                    int32_t blockID = block_counter;
                    std::string bID((const char*)(&blockID), sizeof(uint32_t));
                    value = bID + value;   // prepend 4-byte block_id prefix
                    records.emplace_back(block_counter, std::move(value));
                    block_counter++;
                }
            }
            // Add dummy "records" — same as original ServerInitialization's
            // dummy_num loop. These are indistinguishable from real records
            // from the server's perspective (both in stash, stash-first reads
            // never look for them, they just pad access pattern distributions
            // that downstream privacy analyses rely on).
            for (uint32_t d = 0; d < part.dummy_num; d++) {
                std::string dummy_value(tuple_width_bytes, '\0');
                int32_t blockID = block_counter;
                std::string bID((const char*)(&blockID), sizeof(uint32_t));
                dummy_value = bID + dummy_value.substr(sizeof(uint32_t));
                dummy_value.resize(tuple_width_bytes, '\0');
                records.emplace_back(block_counter, std::move(dummy_value));
                block_counter++;
            }

            std::cout << "[BulkInit] Built " << records.size()
                      << " records (" << (block_counter - part.dummy_num)
                      << " real + " << part.dummy_num << " dummy)\n";

            // ── STEP 3: Direct stash load — no network, no eviction ──────
            // This populates ORAM's internal state (stash + pos_map) without
            // going through access(INSERT) / EvictPath / WriteBucket.
            oram->bulk_load_stash(records);

            // Free the records vector — we don't need it after bulk_load_stash
            // copied the data into the ORAM's stash. This matters because
            // records is ~200-500 MB for big tables, and we're about to stream
            // a multi-GB dummy tree that needs its own headroom.
            {
                std::vector<std::pair<uint32_t, std::string>> empty;
                records.swap(empty);
            }

            // ── STEP 4: STREAM dummy tree to server (v4, OOM-safe) ───────
            // Previously we generated the entire dummy tree into a vector
            // (2-5 GB for ORDER_LINE-sized tables), then bulk_init_tree
            // copied it into another 2-5 GB send buffer. Peak memory per
            // child process was 7-12 GB. With 8 tables init in parallel,
            // total memory easily exceeded the machine's 32 GB RAM → OOM
            // killer killed 3-6 children, causing the entire run to fail.
            //
            // New approach: generate one slot at a time via callback,
            // accumulate into a 10 MB network buffer, flush, repeat.
            // Peak memory per child: ~10 MB + ORAM internal state.
            // Total peak across 8 children: <1 GB for the dummy tree stage.
            uint32_t total_slots = oram->get_total_slot_count();
            std::cout << "[BulkInit] Streaming " << total_slots
                      << " dummy slots to server (memory-safe, constant ~10MB)\n";

            conn->bulk_init_tree_stream(
                oram_name, block_length, total_slots,
                [&oram](uint32_t i) -> SlotPayload {
                    return oram->generate_one_dummy_slot(i);
                });

            std::cout << "[BulkInit] Uploaded " << total_slots
                      << " dummy slots to server via streaming.\n";

            // ── Optional: drain stash to A entries before measurements ──
            //
            // After BulkInit, ALL records live in stash and tree has only
            // dummies.  Runtime queries will gradually evict records into
            // the tree via EarlyReshuffle / EvictPath — BUT during this
            // warm-up phase each WriteBucket scans the (huge) stash, adding
            // ~10ms × height ≈ 250ms per eviction trigger.  Throughput is
            // artificially low for the first ~12% × N queries.
            //
            // If your benchmark needs steady-state throughput from query 1
            // (e.g., comparing against papers that report post-warmup
            // numbers), define BULK_INIT_FLUSH_STASH at compile time to
            // pay the warmup cost once during init instead.  Cost: ~50s
            // per child for ORDER_LINE-sized tables, parallelizable.
            //
            // Default (undefined) preserves the original BulkInit semantics.
#ifdef BULK_INIT_FLUSH_STASH
            std::cout << "[BulkInit] BULK_INIT_FLUSH_STASH set — "
                      << "draining stash before handover...\n";
            auto flush_t0 = std::chrono::steady_clock::now();
            oram->flush_stash_if_needed();
            auto flush_t1 = std::chrono::steady_clock::now();
            double flush_secs = std::chrono::duration<double>(
                flush_t1 - flush_t0).count();
            std::cout << "[BulkInit] flush_stash done in " << flush_secs
                      << "s; stash now " << oram->get_stash_size() << " entries\n";
#else
            // No flush_stash_if_needed(): records are intentionally in stash.
            // Runtime access(READ) will evict them gradually via EarlyReshuffle
            // and EvictPath, which is fine — stash-first reads always work.
#endif

            std::cout << "[BulkInit] Complete for ORAM '" << oram_name
                      << "'. stash_overflow=" << part.stash_overflow_count
                      << "\n";

            uint8_t ready = 1;
            write(write_fd, &ready, sizeof(uint8_t));

            child_event_loop(oram, read_fd, write_fd);

        } catch (const std::exception& e) {
            std::cerr << "[BulkInit Server " << server.server_id << "] ERROR: "
                      << e.what() << std::endl;
            uint8_t failed = 0;
            write(write_fd, &failed, sizeof(uint8_t));
            close(read_fd);
            close(write_fd);
            exit(1);
        }
    }

    // ── PARENT ───────────────────────────────────────────────────────────
    close(to_child_fds[0]);
    close(from_child_fds[1]);

    ServerProcess sp;
    sp.pid        = pid;
    sp.to_child   = to_child_fds[1];
    sp.from_child = from_child_fds[0];

    server_processes_[server.server_id] = sp;
}

void MultiRingORAM_Servers::waitForAllServersReady() {
    // Read the 1-byte ready signal from every child (1 = OK, 0 = failed).
    // All children initialize in parallel; this blocks until the slowest one.
    for (auto& [sid, sp] : server_processes_) {
        uint8_t ready = 0;
        if (!read_all(sp.from_child, &ready, 1)) {
            throw std::runtime_error(
                "Server process " + std::to_string(sid) +
                " closed its pipe before sending ready signal. "
                "Check that ./Server " + std::to_string(
                    // find the port from server_id_to_partition — just print sid
                    sid) + " is running.");
        }
        if (ready != 1) {
            throw std::runtime_error(
                "Server process " + std::to_string(sid) +
                " reported initialization failure (check stderr above).");
        }
        std::cout << "  [Server " << sid << "] ready ✓" << std::endl;
    }
}

void MultiRingORAM_Servers::shutdownAllServers() {
    for (auto& [sid, sp] : server_processes_) {
        // Send shutdown signal: num_ops = 0
        uint32_t shutdown = 0;
        write_all(sp.to_child, &shutdown, sizeof(uint32_t));
        close(sp.to_child);
        close(sp.from_child);
        waitpid(sp.pid, nullptr, 0);
    }
    server_processes_.clear();
}

// ============================================================================
// Batch Construction (unchanged)
// ============================================================================
// ── FIX (4/26/26): no longer silently drops ops when batch is full ──────
//
// Old behaviour: if `current_batch.operations.size() >= fixed_batch_size`,
// the function returned the unmodified batch — the new op was *thrown
// away*, with only a stdout log.  A caller who didn't pre-check size
// would silently lose ops.  This is a correctness bug for any batch
// builder that doesn't externally enforce size.
//
// New behaviour: always append.  If size now exceeds fixed_batch_size,
// log a stderr WARNING (loud, single-line, not flooding stdout).  The
// caller is still responsible for flushing-when-full; this is the
// minimum-disruption fix that turns silent data loss into a loud
// warning until the proper batch-construction layer (Step 4) is in
// place.
// ────────────────────────────────────────────────────────────────────────
BatchInfo MultiRingORAM_Servers::PutOpinBatch(
    BatchInfo current_batch, Operat current_op, uint32_t fixed_batch_size)
{
    if (current_op.type != current_batch.batch_type) {
        std::cerr << "[PutOpinBatch] WARNING: op type "
                  << static_cast<int>(current_op.type)
                  << " does not match batch type "
                  << static_cast<int>(current_batch.batch_type)
                  << "; op DROPPED.\n";
        return current_batch;
    }

    // Always append — never silently drop.
    current_batch.operations.push_back(current_op);

    if (current_batch.operations.size() > fixed_batch_size) {
        static uint64_t over_count = 0;
        over_count++;
        if (over_count == 1 || over_count % 100 == 0) {
            std::cerr << "[PutOpinBatch] WARNING: batch for server "
                      << current_batch.server_id
                      << " has " << current_batch.operations.size()
                      << " ops, exceeds FIXED_BATCH_SIZE=" << fixed_batch_size
                      << " (over-count #" << over_count << "). "
                      << "Caller must flush before adding (current call "
                      << "will not split). sendBatchToServer will still "
                      << "auto-chunk, so this is a perf/observability "
                      << "issue, not data loss.\n";
        }
    }
    return current_batch;
}

// ============================================================================
// Send Batch — exact real ops, no padding
//
// Timing model (matches proxy perspective):
//   wall_start → pipe write all ops → child processes → pipe read result → wall_end
//
// No dummy padding here: we want to measure the true cost of processing
// exactly the committed ops on each server.  Partition scalability
// (k↑ → smaller ORAM per partition → faster per op) is only visible if
// we don't inflate small batches with unnecessary dummy accesses.
//
// If FIXED_BATCH_SIZE padding is needed for security experiments, call
// sendBatchToServer_padded() instead (not used in throughput benchmarks).
// ============================================================================
TransmissionResult MultiRingORAM_Servers::sendBatchToServer(
    const ServerInfo& server, BatchInfo batch)
{
    TransmissionResult result;
    result.server_id    = server.server_id;
    result.success      = false;
    result.records_sent = 0;
    result.bytes_sent   = 0;

    // Empty batch: nothing to send — return immediately
    if (batch.operations.empty()) {
        result.success         = true;
        result.elapsed_time_ms = 0.0;
        return result;
    }

    auto it = server_processes_.find(server.server_id);
    if (it == server_processes_.end()) {
        result.error_message = "No process found for server " +
                               std::to_string(server.server_id);
        return result;
    }
    const ServerProcess& sp = it->second;

    // ── Proxy sends exactly the real ops — timing starts here ────────────
    auto wall_start = std::chrono::high_resolution_clock::now();

    const auto& ops  = batch.operations;
    size_t total_ops = ops.size();
    size_t offset    = 0;

    while (offset < total_ops) {
        // Chunk into at most FIXED_BATCH_SIZE real ops per round-trip.
        // No padding: real_count == num_to_send exactly.
        size_t real_count = std::min(
            static_cast<size_t>(FIXED_BATCH_SIZE),
            total_ops - offset);

        // Send header: exactly real_count ops (no padding)
        uint32_t num_to_send = static_cast<uint32_t>(real_count);
        if (!write_all(sp.to_child, &num_to_send, sizeof(uint32_t))) {
            result.error_message = "Pipe write failed (header)";
            return result;
        }

        // Send real ops only
        for (size_t j = 0; j < real_count; ++j) {
            const Operat& op = ops[offset + j];
            uint8_t  type_byte  = static_cast<uint8_t>(op.type);
            uint32_t key        = op.data_primary_key;
            uint32_t val_len    = static_cast<uint32_t>(op.data_value.size());
            // ── FIX (4/26/26): respect op.is_dummy ────────────────────────
            // Old code hardcoded dummy_flag = 0, defeating any padding the
            // batch builder might emit (Step 4).  Child counts is_dummy
            // ops separately for real_ops_done; padding only works if we
            // forward the flag.
            uint8_t  dummy_flag = op.is_dummy ? 1 : 0;

            if (!write_all(sp.to_child, &type_byte,  1))          goto pipe_err;
            if (!write_all(sp.to_child, &key,         4))          goto pipe_err;
            if (!write_all(sp.to_child, &val_len,     4))          goto pipe_err;
            if (val_len > 0 &&
                !write_all(sp.to_child, op.data_value.data(), val_len))
                                                                    goto pipe_err;
            if (!write_all(sp.to_child, &dummy_flag,  1))          goto pipe_err;
        }

        // ── Read result ───────────────────────────────────────────────────
        {
            uint32_t records_done = 0;
            double   elapsed_ms   = 0.0;
            uint8_t  ok           = 0;

            if (!read_all(sp.from_child, &records_done, sizeof(uint32_t))) goto pipe_err;
            if (!read_all(sp.from_child, &elapsed_ms,   sizeof(double)))   goto pipe_err;
            if (!read_all(sp.from_child, &ok,           sizeof(uint8_t)))  goto pipe_err;

            if (!ok) {
                result.error_message = "Child reported error for server " +
                                       std::to_string(server.server_id);
                return result;
            }
            result.records_sent += records_done;

            // ── FIX (4/26/26): receive per-op plaintext results ───────────
            // Child sends one [u32 len][len bytes] entry per op (in order).
            // For READ ops len > 0 (the decrypted plaintext), for UPDATE/
            // INSERT len == 0.  We accumulate into result.per_op_results
            // so the caller can pick out READ rows after the call returns.
            for (size_t j = 0; j < real_count; ++j) {
                uint32_t result_len = 0;
                if (!read_all(sp.from_child, &result_len, sizeof(uint32_t)))
                                                                          goto pipe_err;
                std::string op_result;
                if (result_len > 0) {
                    op_result.resize(result_len);
                    if (!read_all(sp.from_child, &op_result[0], result_len))
                                                                          goto pipe_err;
                }
                result.per_op_results.push_back(std::move(op_result));
            }
        }

        offset += real_count;
        continue;

    pipe_err:
        result.error_message = "Pipe I/O failed for server " +
                               std::to_string(server.server_id);
        return result;
    }

    auto wall_end = std::chrono::high_resolution_clock::now();
    result.elapsed_time_ms =
        std::chrono::duration<double, std::milli>(wall_end - wall_start).count();
    result.success = true;
    return result;
}

// ============================================================================
// Lookup helpers
// ============================================================================
int MultiRingORAM_Servers::getServerIdForBinKey(const std::string& bin_key) const {
    auto it = bin_key_to_partition_id_.find(bin_key);
    if (it == bin_key_to_partition_id_.end()) return -1;
    auto it2 = partition_id_to_server_id_.find(it->second);
    if (it2 == partition_id_to_server_id_.end()) return -1;
    return it2->second;
}

int MultiRingORAM_Servers::getServerIdForPartition(uint32_t partition_id) const {
    auto it = partition_id_to_server_id_.find(partition_id);
    if (it == partition_id_to_server_id_.end()) return -1;
    return it->second;
}

// ============================================================================
// Partition display (unchanged)
// ============================================================================
void printPartitionDetails(const std::vector<Partition>& partitions) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "📊 Partition Details\n";
    std::cout << std::string(70, '=') << "\n";

    uint32_t total_real = 0, total_noisy = 0, total_dummy = 0;
    for (size_t i = 0; i < partitions.size(); ++i) {
        const auto& p = partitions[i];
        total_real  += p.synopsis;
        total_noisy += p.noisy_synopsis;
        total_dummy += p.dummy_num;

        std::cout << "\nPartition " << i << ":\n";
        std::cout << "  ├─ Bins: [";
        for (size_t j = 0; j < std::min(p.index.size(), size_t(5)); ++j) {
            std::cout << "\"" << p.index[j] << "\"";
            if (j < std::min(p.index.size(), size_t(5)) - 1) std::cout << ", ";
        }
        if (p.index.size() > 5) std::cout << ", ...";
        std::cout << "] (total: " << p.index.size() << " bins)\n";
        std::cout << "  ├─ Real data (synopsis): " << p.synopsis << "\n";
        std::cout << "  ├─ Noisy synopsis: "        << p.noisy_synopsis << "\n";
        std::cout << "  └─ Dummy count: "            << p.dummy_num << "\n";
    }

    std::cout << "\n" << std::string(70, '-') << "\n";
    std::cout << "Summary:\n";
    std::cout << "  ├─ Total partitions: " << partitions.size() << "\n";
    std::cout << "  ├─ Total real: "       << total_real  << "\n";
    std::cout << "  ├─ Total noisy: "      << total_noisy << "\n";
    std::cout << "  ├─ Total dummy: "      << total_dummy << "\n";
    std::cout << "  └─ Overhead: " << std::fixed << std::setprecision(2)
              << (total_real > 0 ? 100.0 * total_dummy / total_real : 0.0)
              << "%\n";
    std::cout << std::string(70, '=') << "\n";
}

// ============================================================================
// Step 4 (4/26/26): build batches from oram_bound_ops
// ============================================================================
//
// Algorithm:
//   1. Group ops by server_id (the partition each op is routed to).
//   2. For each server: sort its ops with READs first (stable, preserves
//      original order within READ group and within UPDATE/INSERT group).
//   3. Slice into chunks of `batch_size`.  Last chunk may be < batch_size.
//   4. If pad_last_batch, append dummy READ ops to the last chunk until
//      it reaches batch_size.  Dummies have is_dummy=true, which the
//      child's event loop and our Step 5 result placement both honor.
//
// Output preserves slot_id ↔ result mapping: per_batch_op_to_slot[b][i]
// gives the slot_id for op #i in batch #b (or UINT32_MAX for a dummy).
// ============================================================================

static constexpr uint32_t kSlotDummy = UINT32_MAX;

BatchPlan buildBatchesFromOramOps(
    const std::vector<Operat>& oram_bound_ops,
    uint32_t                   batch_size,
    bool                       pad_last_batch)
{
    BatchPlan bp;

    if (batch_size == 0) {
        std::cerr << "[buildBatchesFromOramOps] ERROR: batch_size=0; "
                     "returning empty plan.\n";
        return bp;
    }

    // ── 1. Group slot_ids by server ──────────────────────────────────────
    std::map<int, std::vector<uint32_t>> server_to_slots;
    for (uint32_t s = 0; s < oram_bound_ops.size(); ++s) {
        server_to_slots[oram_bound_ops[s].server_id].push_back(s);
    }

    // ── 2. Per server: sort RBW ──────────────────────────────────────────
    // Stable sort: key is (priority, original_slot_id).  Priority 0 = READ,
    // 1 = UPDATE/INSERT.  This both enforces RBW within a server's ops AND
    // preserves original order (= ts-then-op-position-in-txn) within each
    // group — important when we split into multiple batches because the
    // first batches will contain only reads, then mixed reads+writes only
    // at the read/write boundary, then writes-only.
    for (auto& [server_id, slots] : server_to_slots) {
        std::stable_sort(slots.begin(), slots.end(),
            [&](uint32_t a, uint32_t b) {
                int prio_a = (oram_bound_ops[a].type == OpType::READ) ? 0 : 1;
                int prio_b = (oram_bound_ops[b].type == OpType::READ) ? 0 : 1;
                return prio_a < prio_b;
            });

        // ── 3 + 4. Chunk + pad ──────────────────────────────────────────
        size_t cursor = 0;
        while (cursor < slots.size()) {
            size_t take = std::min<size_t>(batch_size, slots.size() - cursor);

            BatchInfo bi;
            bi.server_id  = server_id;
            // batch_type: descriptive only — the wire protocol cares about
            // each Operat's own .type field.  Use the FIRST op's type as
            // a "predominantly" tag (READ or UPDATE) for diagnostics.
            bi.batch_type = oram_bound_ops[slots[cursor]].type;

            std::vector<uint32_t> slot_map;
            slot_map.reserve(batch_size);

            for (size_t i = 0; i < take; ++i) {
                uint32_t slot = slots[cursor + i];
                bi.operations.push_back(oram_bound_ops[slot]);
                bi.operations.back().is_dummy = false;
                slot_map.push_back(slot);
            }

            // Pad with dummy READs if last chunk under-fills and caller wants
            if (pad_last_batch && take < batch_size) {
                size_t need = batch_size - take;
                for (size_t k = 0; k < need; ++k) {
                    Operat dummy;
                    dummy.type             = OpType::READ;
                    dummy.data_primary_key = 0;       // any block_id works; child returns "[unknown]"
                    dummy.data_value.clear();
                    dummy.server_id        = server_id;
                    dummy.is_dummy         = true;    // ← critical: child won't count it,
                                                      //   sendBatchToServer must forward this flag
                    dummy.global_op_id     = 0;       // not in any real plan
                    bi.operations.push_back(dummy);
                    slot_map.push_back(kSlotDummy);
                }
            }

            bp.batches.push_back(std::move(bi));
            bp.per_batch_op_to_slot.push_back(std::move(slot_map));

            cursor += take;
        }
    }

    return bp;
}

// ============================================================================
// Step 5 (4/26/26): execute the dedup'd plan against ORAMs
// ============================================================================
//
// Builds batches via buildBatchesFromOramOps, dispatches them to the right
// server child via the existing sendBatchToServer helper, then walks the
// returned per_op_results and places each into per_slot_results[slot_id]
// (skipping padding dummies).
//
// pos_map TRANSLATION (4/26/26): each op in the BatchInfo passed to
// sendBatchToServer has its data_primary_key REPLACED with the partition-
// local block_id (lookup via lookupBlockId).  The original input vector
// `oram_bound_ops` is NOT modified, so the caller's plan still carries
// PKs for diagnostics / result assembly.
//
// PARALLEL per-server dispatch (paper §3.3 / §5.2 / §6.4):
// batches are grouped by server; each server's batches run in their own
// thread, so contacted ORAM instances execute concurrently.  Batches for
// the SAME server stay sequential (one ORAM, one pipe).  The epoch's
// completion time is therefore bounded by the slowest contacted server —
// this is exactly the max_i(...) term in the §6.4 cost model.
// ============================================================================
ExecutionResult MultiRingORAM_Servers::executeOramOps(
    const std::vector<Operat>& oram_bound_ops,
    uint32_t                   batch_size,
    bool                       pad_to_batch_size)
{
    ExecutionResult er;
    er.per_slot_results.assign(oram_bound_ops.size(), std::string{});

    if (oram_bound_ops.empty()) {
        er.success = true;
        return er;
    }

    if (batch_size == 0) {
        er.success       = false;
        er.error_message = "batch_size must be > 0";
        return er;
    }

    // Build batches at the requested granularity.
    BatchPlan bp = buildBatchesFromOramOps(
        oram_bound_ops, batch_size, pad_to_batch_size);
    er.total_batches_sent = bp.batches.size();

    // ── Group batch indices by server ───────────────────────────────────
    // Iterating b in ascending order keeps each server's batch list in its
    // original (RBW-sorted) order, so per-server execution order is
    // unchanged versus the old sequential code.
    std::map<int, std::vector<size_t>> batches_by_server;
    for (size_t b = 0; b < bp.batches.size(); ++b)
        batches_by_server[bp.batches[b].server_id].push_back(b);

    // One job per server; outcomes[j] is written only by worker j, so there
    // is no shared-write race on this vector.
    struct ServerOutcome {
        bool        ok = true;
        std::string error_message;
        uint32_t    real_ops  = 0;
        uint32_t    dummy_ops = 0;
    };
    std::vector<std::pair<int, std::vector<size_t>>> server_jobs(
        batches_by_server.begin(), batches_by_server.end());
    std::vector<ServerOutcome> outcomes(server_jobs.size());

    auto t_start = std::chrono::high_resolution_clock::now();

    // Worker: process all batches for ONE server, sequentially.
    // Safe to run concurrently across servers because:
    //   - each worker talks to a different child via a different fd pair;
    //   - server_processes_ / pk_to_blockid_ are only read (no writes here);
    //   - bp.batches[b] for a given server is touched by exactly one worker;
    //   - er.per_slot_results is pre-sized and every op has a UNIQUE slot_id,
    //     so writes land on disjoint elements (no reallocation, no overlap).
    auto run_server = [&](size_t job_idx) {
        ServerOutcome&               oc        = outcomes[job_idx];
        const std::vector<size_t>&   batch_ids = server_jobs[job_idx].second;

        for (size_t b : batch_ids) {
            BatchInfo&                   bi       = bp.batches[b];
            const std::vector<uint32_t>& slot_map = bp.per_batch_op_to_slot[b];

            if (slot_map.size() != bi.operations.size()) {
                oc.ok            = false;
                oc.error_message = "internal: slot_map size mismatch";
                return;
            }

            // ── pos_map translation ───────────────────────────────────────
            // For each REAL op, rewrite (server_id, primary_key) → block_id.
            // Dummy ops keep PK=0.  oram_bound_ops (caller's plan) is NOT
            // modified — only this batch's local copy is.
            for (Operat& op : bi.operations) {
                if (op.is_dummy) continue;
                uint32_t bid = lookupBlockId(bi.server_id, op.data_primary_key);
                if (bid == UINT32_MAX) {
                    std::cerr << "[executeOramOps] WARNING: pk=" << op.data_primary_key
                              << " has no block_id on server=" << bi.server_id
                              << " (slot bypassed; result will be empty).\n";
                    op.is_dummy = true;     // demote to dummy so child won't crash
                    continue;
                }
                op.data_primary_key = bid;
            }

            ServerInfo stub;
            stub.server_id = bi.server_id;

            TransmissionResult tr = sendBatchToServer(stub, bi);

            if (!tr.success) {
                oc.ok            = false;
                oc.error_message = "Batch " + std::to_string(b) +
                                   " (server " + std::to_string(bi.server_id) +
                                   ") failed: " + tr.error_message;
                return;
            }

            oc.real_ops  += tr.records_sent;
            oc.dummy_ops += (bi.operations.size() - tr.records_sent);

            if (tr.per_op_results.size() != bi.operations.size()) {
                std::cerr << "[executeOramOps] WARNING: batch " << b
                          << " expected " << bi.operations.size()
                          << " per-op results, got " << tr.per_op_results.size()
                          << "\n";
            }

            size_t n = std::min(slot_map.size(), tr.per_op_results.size());
            for (size_t i = 0; i < n; ++i) {
                uint32_t slot = slot_map[i];
                if (slot == kSlotDummy) continue;
                er.per_slot_results[slot] = std::move(tr.per_op_results[i]);
            }
        }
    };

    // Spawn one thread per contacted server; main thread joins them all.
    std::vector<std::thread> workers;
    workers.reserve(server_jobs.size());
    for (size_t j = 0; j < server_jobs.size(); ++j)
        workers.emplace_back(run_server, j);
    for (auto& w : workers) w.join();

    auto t_end = std::chrono::high_resolution_clock::now();
    er.total_elapsed_ms =
        std::chrono::duration<double, std::milli>(t_end - t_start).count();

    // ── Merge per-server outcomes ───────────────────────────────────────
    er.success = true;
    for (const auto& oc : outcomes) {
        er.total_real_ops_sent  += oc.real_ops;
        er.total_dummy_ops_sent += oc.dummy_ops;
        if (!oc.ok) {
            er.success = false;
            if (er.error_message.empty()) er.error_message = oc.error_message;
        }
    }

    std::cout << "[executeOramOps]"
              << " batches="      << er.total_batches_sent
              << "  servers="     << server_jobs.size() << " (parallel)"
              << "  batch_size="  << batch_size
              << "  real_ops="    << er.total_real_ops_sent
              << "  dummy_ops="   << er.total_dummy_ops_sent
              << "  elapsed_ms="  << er.total_elapsed_ms
              << std::endl;

    return er;
}