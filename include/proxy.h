// //
// // Created for Obladi Proxy Implementation
// // Based on Obladi OSDI'18 paper design
// // Extended with insert-only partition and dynamic repartitioning
// //
//
// #ifndef PROXY_H
// #define PROXY_H
//
// #include <memory>
// #include <vector>
// #include <unordered_map>
// #include <mutex>
// #include <random>
// #include "RingORAM.h"
//
// #pragma once
// #include <cmath>
// #include <map>
// #include <thread>
// #include <atomic>
// #include <string>
// #include <chrono>
// #include "Repartition.h"
// #include "Util.h"
// #include "optype.h"
//
// // Forward declarations
// class NetIOConnector;
//
// // ============================================
// // Transaction and Version Management Structures
// // ============================================
//
// // Version of a data object in the version chain
// struct ObjectVersion {
//     uint32_t txn_id;              // Transaction that created this version
//     std::string value;            // Encrypted data value
//     uint32_t timestamp;           // MVCC timestamp
//     uint32_t max_read_ts;         // Check the latest version on read op in the chain
//     uint32_t read_marker;         // Highest timestamp that read this version
//     ObjectVersion* next;          // Next version in chain
//
//     ObjectVersion(uint32_t tid, const std::string& val, uint32_t ts)
//         : txn_id(tid), value(val), timestamp(ts), max_read_ts(0), read_marker(0), next(nullptr) {}
// };
//
// // Version chain for a single key
// struct VC {
//     uint32_t key;
//     ObjectVersion* head;          // Most recent version
//     std::mutex chain_mutex;
//
//     explicit VC(uint32_t k) : key(k), head(nullptr) {}
//     ~VC() {
//         ObjectVersion* curr = head;
//         while (curr) {
//             ObjectVersion* tmp = curr;
//             curr = curr->next;
//             delete tmp;
//         }
//     }
// };
//
// // Per-partition transaction state
// struct PerPartitionTxnState {
//     std::vector<Operat> read_ops;   // All read operations on this partition
//     std::vector<Operat> write_ops;  // All write operations on this partition
// };
//
// // Transaction metadata
// struct Transaction {
//     uint32_t txn_id;
//     uint32_t timestamp;
//
//     // One txn's ops may span multiple partitions
//     // key: partition_id -> value: partition-specific read/write ops
//     std::unordered_map<uint32_t, PerPartitionTxnState> per_partition;
//
//     std::vector<uint32_t> read_deps;
//     std::unordered_map<uint32_t, std::string> writes;   // Key -> encrypted value
//     std::unordered_map<uint32_t, std::string> reads;    // Key -> encrypted value
//
//     bool committed;
//     bool aborted;
//     bool finished;
//     std::chrono::steady_clock::time_point start_time;
//
//     Transaction(uint32_t tid, uint32_t ts)
//         : txn_id(tid),
//           timestamp(ts),
//           committed(false),
//           aborted(false),
//           finished(false),
//           start_time(std::chrono::steady_clock::now()) {}
// };
//
// // ============================================
// // Batch Structures
// // ============================================
//
// // Request in a batch (can be read or write)
// struct BatchRequest {
//     uint32_t txn_id;
//     uint32_t key;
//     OpType op_type;               // READ, UPDATE, INSERT
//     std::string data;             // For writes
//     bool is_dummy;                // True if this is a padding request
//     int source_index;
//
//     BatchRequest() : txn_id(0), key(0), op_type(OpType::READ), is_dummy(true), source_index(-1) {}
//     BatchRequest(uint32_t tid, uint32_t k, OpType op, const std::string& d = "")
//         : txn_id(tid), key(k), op_type(op), data(d), is_dummy(false), source_index(-1) {}
// };
//
// // A single read or write batch
// struct Bat {
//     std::vector<BatchRequest> requests;
//     uint32_t batch_id;
//     uint32_t epoch_id;
//     uint32_t partition_id;
//     bool is_read_batch;           // true for read batch, false for write batch
//
//     Bat(uint32_t bid, uint32_t eid, uint32_t pid, bool is_read, size_t size)
//         : batch_id(bid), epoch_id(eid), partition_id(pid), is_read_batch(is_read) {
//         requests.reserve(size);
//     }
// };
//
// // ============================================
// // Partition Structure
// // ============================================
// struct PartitionInfo {
//     uint32_t partition_id;
//     std::vector<std::string> bin_indices;
//     uint32_t real_count;
//     uint32_t dummy_count;
//     uint32_t read_ops;
//     uint32_t write_ops;
//     uint32_t batch_size;
//
//     // ORAM相关
//     std::unique_ptr<RingORAM> oram;
//     NetIOConnector* connector;
//     // Version cache (stash)
//     std::unordered_map<uint32_t, VC*> version_cache;
//     std::mutex cache_mutex;
//
//     // ⭐ Stash capacity限制（= RingORAM的A）
//     uint32_t stash_capacity;
//
//     // ⭐ Random eviction需要的RNG（每个partition独立）
//     std::mt19937 rng;
//
//     // Batch相关
//     std::vector<std::unique_ptr<Bat>> current_read_batches;
//     std::vector<std::unique_ptr<Bat>> current_write_batches;
//     uint32_t current_read_batch_idx;
//     uint32_t current_write_batch_idx;
//     uint32_t num_read_batches;
//     uint32_t num_write_batches;
//
//     // ✅ 修正构造函数参数类型
//     PartitionInfo(uint32_t pid,
//                   const std::vector<std::string>& bins,
//                   uint32_t real,
//                   uint32_t dummy,
//                   uint32_t r_ops,
//                   uint32_t w_ops,
//                   uint32_t batch_sz)
//         : partition_id(pid)
//         , bin_indices(bins)
//         , real_count(real)
//         , dummy_count(dummy)
//         , read_ops(r_ops)
//         , write_ops(w_ops)
//         , batch_size(batch_sz)
//         , connector(nullptr)
//         , stash_capacity(0)  // 初始化为0，稍后从ORAM获取
//         , rng(std::random_device{}())  // ⭐ 初始化RNG
//         , current_read_batch_idx(0)
//         , current_write_batch_idx(0)
//         , num_read_batches(0)
//         , num_write_batches(0)
//     {
//         // 计算需要的batch数量
//         if (batch_size > 0) {
//             num_read_batches = (read_ops + batch_size - 1) / batch_size;
//             num_write_batches = (write_ops + batch_size - 1) / batch_size;
//         }
//     }
// };
//
// // ============================================
// // Epoch Structure
// // ============================================
//
// struct Epoch {
//     uint32_t epoch_id;
//     std::chrono::steady_clock::time_point start_time;
//     std::vector<std::unique_ptr<Transaction>> transactions;
//     std::atomic<bool> is_active;
//     std::atomic<uint32_t> next_txn_id;
//     std::vector<uint32_t> epoch_txns;
//
//     explicit Epoch(uint32_t eid)
//         : epoch_id(eid),
//           start_time(std::chrono::steady_clock::now()),
//           is_active(true),
//           next_txn_id(0) {}
// };
//
// // ============================================
// // Main Proxy Class
// // ============================================
//
// class ObladiProxy {
// public:
//     ObladiProxy(
//         uint32_t total_objects,
//         uint32_t bin_count,
//         double epsilon_dp,                          // 总的 epsilon budget
//         uint32_t max_partitions,
//         uint32_t threshold,
//         const std::vector<NetIOConnector*>& connectors,
//         uint32_t block_len,
//         uint32_t Z_param,
//         uint32_t S_param,
//         uint32_t batch_size,
//         uint64_t fixed_time_interval_ms,
//         // ✅ 新增参数：Repartitioning 配置
//         const std::vector<int>& attribute_indices,  // 用于分区的属性索引
//         double epsilon_synopsis,                     // synopsis 的 epsilon
//         double epsilon_threshold_budget,             // threshold 的 epsilon
//         double threshold_pub,                        // 分区阈值（DP 保护的）
//         bool use_fixed_partition_number,            // true: 固定分区数, false: 固定阈值
//         double fixed_threshold_value,               // 固定阈值模式的阈值
//         const std::string& schema_str = "bin_id:uint32"  // schema 描述（可选）
//     );
//
//     ~ObladiProxy();
//
//     // Epoch timer
//     uint64_t fixed_time_interval_ms_;
//     std::chrono::steady_clock::time_point epoch_start_time_;
//
//     enum class EpochState {
//         ACCEPTING_TXN,     // 接收新事务
//         GRACE_PERIOD,      // 宽限期
//         CLOSED             // 已关闭
//     };
//
//     EpochState epoch_state_;
//     std::mutex epoch_state_mutex_;
//     std::thread epoch_timer_thread_;
//     std::atomic<bool> timer_running_;
//     std::condition_variable timer_cv_;
//
//     struct TxnOperationStatus {
//         bool all_ops_received = false;
//         uint32_t op_count = 0;
//     };
//     std::unordered_map<uint32_t, TxnOperationStatus> txn_op_status_;
//     std::mutex txn_op_status_mutex_;
//     void LoadInitialData(
//         uint32_t bin_size,
//         const std::unordered_map<uint32_t, std::string>& initial_data
//     );
//
//     // Transaction interface
//     void StartEpochTimer();
//     void StopEpochTimer();
//     void EpochTimerThreadFunc();
//     bool ShouldCloseEpoch();
//     void MarkOperationReceived(uint32_t txn_id, bool is_last);
//     void CheckAndAbortIncompleteTxns();
//     uint32_t BeginTransaction();
//     void AddReadOp(uint32_t global_txn_id, uint32_t key, uint32_t partition_id);
//     void AddWriteOp(uint32_t global_txn_id,
//                 uint32_t key,
//                 uint32_t partition_id,
//                 const std::string& value,
//                 bool is_new_key);
//     bool Read(uint32_t txn_id, uint32_t key, std::string& out_value);
//     bool Write(uint32_t txn_id, uint32_t key, const std::string& value);
//     bool Commit(uint32_t txn_id);
//     void Abort(uint32_t txn_id);
//     std::array<double, 2> EndEpoch();
//     void RunMVSTOValidation(const std::vector<uint32_t>& epoch_txns);
//     std::array<double, 2> BuildAndExecuteBatches(const std::vector<uint32_t>& epoch_txns);
//     void GarbageCollectEpoch(const std::vector<uint32_t>& epoch_txns);
//
//     // Epoch management
//     void StartNewEpoch();
//     bool IsEpochActive() const { return current_epoch_ && current_epoch_->is_active.load(); }
//
//     // Statistics
//     void PrintStatistics() const;
//     std::vector<uint32_t> GetPartitionHeights() const;
//     std::vector<uint32_t> GetPartitionTrueCounts() const;
//     std::vector<std::vector<std::string>> GetPartitionBinIndices() const;
//     uint32_t GetBinCount() const;
//
//     // ⭐ Stash管理方法（使用Random Eviction）
//     void WriteBackToORAM(uint32_t partition_id, uint32_t key, VC* chain);
//     void EvictCommittedVersions(uint32_t partition_id);
//
// private:
//     std::atomic<bool> end_epoch_in_progress_{false};
//     static constexpr uint32_t SYSTEM_TXN_ID = 0xFFFFFFFF;
//     static constexpr uint32_t SYSTEM_TS     = 0;
//
//     // ============================================
//     // Phase 1: Data Partitioning
//     // ============================================
//     void PerformPartitioning(const std::vector<uint32_t>& data_distribution);
//     void InitializePartitions();
//     void BulkInsertToPartition(
//     uint32_t partition_id,
//     const std::vector<uint32_t>& keys,
//     const std::vector<std::string>& values,
//     bool enable_parallel_for_partition /*= false*/,
//     size_t max_workers /*= std::thread::hardware_concurrency() ? std::thread::hardware_concurrency() : 4*/
// );
//
//     // ============================================
//     // Phase 2: Version Chain Management
//     // ============================================
//     ObjectVersion* ReadFromVersionChain(uint32_t partition_id, uint32_t key,
//                                         uint32_t timestamp);
//     void AddToVersionChain(uint32_t partition_id, uint32_t key,
//                           uint32_t txn_id, const std::string& value,
//                           uint32_t timestamp);
//     void UpdateReadMarker(uint32_t partition_id,
//                          uint32_t key,
//                          uint32_t read_ts);
//
//     // ============================================
//     // Phase 3: Batching and Deduplication
//     // ============================================
//     void DeduplicateAndBatch(uint32_t partition_id);
//     void AddToBatch(uint32_t partition_id, const BatchRequest& req);
//     void PadBatch(Bat* bat);
//
//     // ============================================
//     // Phase 4: ORAM Execution
//     // ============================================
//     void ExecuteReadBatch(uint32_t partition_id, Bat* batch,
//                          std::atomic<uint64_t>& total_oram_time_us);
//     void ExecuteWriteBatch(uint32_t partition_id, Bat* bat,
//                           std::atomic<uint64_t>& total_oram_time_us);
//     void ProcessBatchResults(uint32_t partition_id, Bat* bat,
//                             const std::vector<std::string>& results);
//
//     // ============================================
//     // Concurrency Control (MVTSO)
//     // ============================================
//     uint32_t AssignTimestamp();
//
//     // ============================================
//     // Epoch Management Helper Functions
//     // ============================================
//     void FlushVersionCache(uint32_t partition_id);
//     void AbortUnfinishedTransactions();
//     void CommitEpochTransactions();
//
//     // ============================================
//     // NEW: Insert-Only Partition Management
//     // ============================================
//     void ClearPartitions();
//     bool IsKeyInInsertPartition(uint32_t key) const;
//     bool ShouldRepartition() const;
//     void TriggerRepartitioning();
//     void CollectAllData(std::vector<uint32_t>& keys,
//                        std::vector<std::string>& values);
//     void RebuildPartitions(const std::vector<uint32_t>& keys,
//                           const std::vector<std::string>& values);
//
//     // ============================================
//     // Member Variables
//     // ============================================
//
//     // Repartitioning
//     std::unique_ptr<Repartition> repartitioner_;
//     std::vector<Partition> partitions_metadata_;  // From Repartition
//
//     // Partition management
//     std::vector<std::unique_ptr<PartitionInfo>> partitions_;
//     mutable std::mutex partition_mutex_;
//     std::unordered_map<uint32_t, uint32_t> key_to_partition_;
//     // NEW: Insert-only partition
//     uint32_t insert_partition_pid_{0};       // pid of the dedicated insert partition
//     PartitionInfo* insert_partition_{nullptr}; // non-owning ptr into partitions_
//     std::atomic<uint32_t> insert_count_{0};  // how many new keys inserted
//     uint32_t repartition_threshold_{0};      // when to trigger repartition
//     mutable std::mutex insert_mutex_;
//
//     // Epoch management
//     std::unique_ptr<Epoch> current_epoch_;
//     uint32_t epoch_counter_;
//     std::mutex epoch_mutex_;
//
//     // Transaction management
//     std::unordered_map<uint32_t, std::unique_ptr<Transaction>> active_transactions_;
//     std::mutex txn_mutex_;
//     std::atomic<uint32_t> global_timestamp_counter_;
//
//     // Network and storage
//     std::vector<NetIOConnector*> connectors_;
//
//     // Configuration
//     uint32_t total_objects_;
//     uint32_t bin_count_;
//     double epsilon_dp_;
//     uint32_t max_partitions_;
//     uint32_t threshold_;          // Partitioning threshold
//     uint32_t block_length_;
//     uint32_t Z_param_;
//     uint32_t S_param_;
//     uint32_t batch_size_;
//     std::vector<size_t> part_to_conn_;
//
//     // Statistics
//     std::atomic<uint64_t> total_commits_;
//     std::atomic<uint64_t> total_aborts_;
//     std::atomic<uint64_t> total_reads_;
//     std::atomic<uint64_t> total_writes_;
//
//     // Thread safety
//     mutable std::mutex stats_mutex_;
//     std::vector<int> attribute_indices_;
//     double epsilon_synopsis_;
//     double epsilon_threshold_budget_;
//     double threshold_pub_;
//     bool use_fixed_partition_number_;
//     double fixed_threshold_value_;
//     std::string schema_str_;
// };
//
// #endif // PROXY_H