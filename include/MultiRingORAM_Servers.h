//
// Created by Xining Yuan on 2/21/26.
// Modified: per-server process model + fixed batch size with padding
//

#ifndef MULTIRINGORAM_SERVERS_H
#define MULTIRINGORAM_SERVERS_H

#include "RingORAM.h"
#include "Repartition.h"
#include <sys/types.h>
#include <functional>

// ============================================
// Fixed Batch Size Configuration
// ============================================
// Maximum ops per round-trip to a child server process.
// If the real op count exceeds this, the batch is split into chunks of
// this size.  No dummy padding is added — each chunk contains only real
// ops so that partition scalability is measured accurately.
// (For security experiments requiring oblivious batch sizes, use a
//  dedicated padded variant instead.)
static constexpr uint32_t FIXED_BATCH_SIZE = 64;

// ============================================
// Data Structures
// ============================================
struct Operat {
    OpType   type;
    uint32_t data_primary_key;
    std::string data_value;
    int      server_id;
    bool     last_one        = false;
    bool     read_marker     = false;
    uint32_t txn_timestamp_id = 0;
    bool     is_dummy        = false;   // true → padding dummy, not counted in records_sent

    // ── NEW (4/26/26) ───────────────────────────────────────────────────
    // Globally-unique op id within one epoch, assigned by
    // QueriesReceiving::collect_epoch().  Used by:
    //   - DedupPlan::per_op_decision        (key)
    //   - DedupPlan::oram_bound_ops slot_id (= position in that vector)
    //   - Step 5 result assembly            (cross-references to per_slot_results)
    // Pre-collect_epoch ops have global_op_id = 0; this is fine because
    // dedup never runs before collect_epoch.
    uint32_t global_op_id    = 0;
};

struct BatchInfo {
    int server_id;
    OpType batch_type;
    std::vector<Operat> operations;
};

struct ServerInfo {
    int    server_id;
    int    port;
    pid_t  pid;
    size_t partition_id;
    std::vector<std::string> assigned_bins;
    std::vector<uint32_t>    assigned_data_indices;
};

// Holds the file-descriptors for the pipe pair to/from a child server process.
struct ServerProcess {
    pid_t pid        = -1;
    int   to_child   = -1;   // parent writes ops here, child reads
    int   from_child = -1;   // child writes results here, parent reads
};

struct TransmissionResult {
    int    server_id;
    bool   success;
    size_t bytes_sent;
    size_t records_sent;          // non-dummy ops processed
    double elapsed_time_ms;
    std::string response;
    std::string error_message;

    // Per-op plaintext results returned by child's access() calls.
    //   - For OpType::READ: the decrypted plaintext (bID(4) + tuple_data + maybe
    //     trailing padding zeros).  Caller should truncate to expected width
    //     (i.e. 4 + tuple_width) before parsing.
    //   - For OpType::UPDATE / INSERT: empty string (access() returns "").
    // Order corresponds 1-to-1 with the input batch's operations.
    std::vector<std::string> per_op_results;
};

// ============================================================================
// Step 4 + 5 supporting structures (4/26/26)
// ============================================================================
// Defined here (before the MultiRingORAM_Servers class) so executeOramOps
// can return ExecutionResult by value with a complete type visible.
//
// These hold the output of the batch builder (step 4) and the per-slot
// result vector returned by the dispatch loop (step 5).
//
// Note: DedupPlan lives in Queries_Receiving.h (it's primarily owned by
// QueriesReceiving).  To avoid an include cycle, executeOramOps takes
// the OPERAT VECTOR (`oram_bound_ops`) directly rather than a DedupPlan
// reference — the caller decomposes plan.oram_bound_ops, sends it in,
// and pairs the returned per_slot_results with plan.per_op_decision
// when assembling final client responses.
// ============================================================================

struct BatchPlan {
    // Batches in send order.  All READ-heavy batches for a server come
    // before any UPDATE-heavy batches for the same server.  Different
    // servers' batches are interleaved by server iteration order.
    std::vector<BatchInfo> batches;

    // For result assembly: per-batch op-index → slot_id (or UINT32_MAX
    // for padding dummies).  Same shape as batches[i].operations.
    std::vector<std::vector<uint32_t>> per_batch_op_to_slot;
};

struct ExecutionResult {
    // Per-slot results from ORAM, indexed by slot_id (= position in the
    // input oram_bound_ops vector passed to executeOramOps).  For READ
    // ops: decrypted plaintext (caller must truncate to expected width).
    // For UPDATE/INSERT: empty string.  Padding dummies are NOT in here
    // (their results are discarded inside executeOramOps).
    std::vector<std::string> per_slot_results;

    bool        success = true;
    std::string error_message;
    double      total_elapsed_ms     = 0.0;
    size_t      total_real_ops_sent  = 0;
    size_t      total_dummy_ops_sent = 0;
    size_t      total_batches_sent   = 0;
};

// Free-standing batch builder (defined in MultiRingORAM_Servers.cpp).
//   oram_bound_ops : the dedup'd op list (each op has server_id set)
//   batch_size     : ops per batch (use FIXED_BATCH_SIZE in production)
//   pad_last_batch : if true, last under-filled batch per server is padded
//                    with dummy READ ops to reach batch_size
// Returns a BatchPlan.  Caller passes it through executeOramOps OR sends
// each batch manually via sendBatchToServer.
BatchPlan buildBatchesFromOramOps(
    const std::vector<Operat>& oram_bound_ops,
    uint32_t                   batch_size,
    bool                       pad_last_batch);

// ============================================
// Global Variables
// ============================================
extern std::vector<pid_t> server_pids;
extern std::mutex cout_mutex;

pid_t startServer(int port, const std::string& server_exec);
bool  waitForServer(int port, int max_attempts = 30);

class MultiRingORAM_Servers {
public:
    // ── Partition / data routing ──────────────────────────────────────────
    void distributeDataToPartitions(
        const BinInfo& bin_info,
        const std::vector<Partition>& partitions,
        std::vector<ServerInfo>& servers);

    // ── Server process lifecycle ──────────────────────────────────────────
    // Fork a child server process for this partition.  The child creates its
    // own NetIOConnector (using server.port), builds the RingORAM, and then
    // enters a pipe-based event loop waiting for batches.
    // Returns immediately; call waitForAllServersReady() after all forks.
    void ServerInitialization(
        const ServerInfo&      server,
        const Partition&       part,
        const BinInfo&         bin_info,
        const std::vector<std::string>& data,
        const std::string&     schema_str,
        uint32_t               bucket_size,
        std::string            oram_name,
        uint32_t               block_length,
        const std::string&     host,   // e.g. "127.0.0.1"
        uint32_t               S_input);

    // ── Fast-path init for TPC-C (4/18/26) ───────────────────────────────
    // Same signature as ServerInitialization, but uses silent-mode RingORAM
    // construction + bulk_init_tree upload. Reduces init time from hours
    // to seconds per partition.
    //
    // Do NOT use for the insert-only RingORAM path — use the original
    // ServerInitialization for that scenario.
    void ServerInitializationBulk(
        const ServerInfo&      server,
        const Partition&       part,
        const BinInfo&         bin_info,
        const std::vector<std::string>& data,
        const std::string&     schema_str,
        uint32_t               bucket_size,
        std::string            oram_name,
        uint32_t               block_length,
        const std::string&     host,
        uint32_t               S_input);
    // ─────────────────────────────────────────────────────────────────────

    // Block until all forked child processes have finished initializing.
    void waitForAllServersReady();

    // Send shutdown signal to all children and reap them.
    void shutdownAllServers();

    // ── FIX (4/27/26): RAII safety net for child-process leaks ──────────
    // Without this destructor, `delete mrs` does NOT shut down child
    // processes — server_processes_ map is just dropped, leaving N
    // child PIDs orphaned (but still bound to ports).  Across an init
    // sweep with 8 tables × 10 configs that's 80+ zombies.  Symptoms:
    //   - 100+ Servers_MultiRingORAM in pgrep
    //   - new ServerInitialization hangs because ports are taken
    //   - second config never finishes init
    //
    // RAII destructor calls shutdownAllServers() if any children are
    // still in the map.  Idempotent — calling shutdownAllServers()
    // explicitly first is safe (the map is cleared on success).
    ~MultiRingORAM_Servers() {
        if (!server_processes_.empty()) {
            shutdownAllServers();
        }
    }
    // ────────────────────────────────────────────────────────────────────

    // ── Batch sending ─────────────────────────────────────────────────────
    // Sends exactly the real ops in the batch, chunked into sub-batches of
    // at most FIXED_BATCH_SIZE per round-trip.  No dummy padding.
    // Timing: proxy sends → child executes → child replies → proxy returns.
    TransmissionResult sendBatchToServer(
        const ServerInfo& server,
        BatchInfo current_batch);

    BatchInfo PutOpinBatch(
        BatchInfo current_batch,
        Operat    current_op,
        uint32_t  fixed_batch_size);

    // ── Step 4 + 5 (4/26/26): full epoch dispatcher ──────────────────────
    // Sends pre-built batches to their respective server children, in
    // batch order, and gathers per-slot results.
    //
    // batch_size  : ops per round-trip to each child.  Per Obladi paper:
    //                 - TPC-C:      bwrite ≈ 2000 (paper §11.1)
    //                 - FreeHealth: bwrite ≈ 200
    //                 - SmallBank:  500 (typical OLTP, paper Fig 10b)
    //               Default = FIXED_BATCH_SIZE; pass per-workload value.
    //
    // pad_to_batch_size : true → every batch is exactly batch_size on the
    //                     wire (security default).  false → real ops only
    //                     (throughput-measurement default in benchmarks).
    //
    // Internally translates each op's data_primary_key → block_id via
    // lookupBlockId() before sending to child.  Op copies are mutated;
    // the input `oram_bound_ops` vector is NOT modified.
    ExecutionResult executeOramOps(
        const std::vector<Operat>&   oram_bound_ops,
        uint32_t                     batch_size       = FIXED_BATCH_SIZE,
        bool                         pad_to_batch_size = true);
    // ─────────────────────────────────────────────────────────────────────

    void printDataToPartitionMapping();

    // ── Lookup helpers ────────────────────────────────────────────────────
    int getServerIdForBinKey(const std::string& bin_key) const;
    int getServerIdForPartition(uint32_t partition_id) const;

    // ── pos_map: per-partition (primary_key → block_id) (4/26/26) ────────
    // The proxy's "level-2" lookup, after attribute → server_id is already
    // resolved.  Within a partition, block_id is what RingORAM expects on
    // every access(); primary_key is the application-level identifier.
    //
    // Populated by distributeDataToPartitions (deterministic iteration over
    // part.index → bin_info.bin_key_to_data_indices, in the SAME order as
    // ServerInitialization's INSERT loop, so the (server_id, pk, block_id)
    // assignment matches the child's view).
    //
    // Convention: by default, primary_key == data_idx (the index into the
    // global `data` vector).  Override via setPrimaryKeyMapper() if your
    // application uses a different PK extraction (e.g., parse PK from the
    // record bytes).
    using PrimaryKeyMapper = std::function<uint32_t(uint32_t /*data_idx*/,
                                                    const std::string& /*record*/)>;
    void setPrimaryKeyMapper(PrimaryKeyMapper m) { pk_mapper_ = std::move(m); }

    // Lookup; returns UINT32_MAX if not found (proxy should treat that as
    // an internal bug — every committed op's key MUST be in the map).
    uint32_t lookupBlockId(int server_id, uint32_t primary_key) const;

    // Diagnostic: dump map sizes per server.
    void printPkMapStats() const;

private:
    // Pipe I/O helpers (handle partial reads/writes)
    static bool write_all(int fd, const void* buf, size_t n);
    static bool read_all(int fd, void* buf, size_t n);

    // Core routing maps
    std::map<std::string, uint32_t> bin_key_to_partition_id_;
    std::map<uint32_t, int>         partition_id_to_server_id_;
    std::map<int, uint32_t>         server_id_to_partition_id_;

    // Process handles (replaces server_orams_)
    std::map<int, ServerProcess>    server_processes_;

    std::unordered_map<uint32_t, uint32_t> data_primary_key_to_attr_key;

    // Per-partition position map (primary_key → block_id), keyed first
    // by server_id.  See public-section comment above for invariants.
    std::map<int, std::unordered_map<uint32_t, uint32_t>> pk_to_blockid_;

    // Pluggable PK extractor; default = identity (data_idx as PK).
    PrimaryKeyMapper pk_mapper_ =
        [](uint32_t data_idx, const std::string& /*rec*/) -> uint32_t {
            return data_idx;
        };
};

#endif // MULTIRINGORAM_SERVERS_H