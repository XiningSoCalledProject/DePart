// //
// // Created for Obladi Proxy Implementation
// // Based on Obladi OSDI'18 paper design
// //
//
// #include "proxy.h"
// #include <algorithm>
// #include <iostream>
// #include <cassert>
// #include <cmath>
// #include <thread>
// #include <array>
// #include <limits>
// #include <stdexcept>
// #include <future>
// #include <atomic>
//
// namespace OverflowCheck {
//     // Check if a + b would overflow for uint32_t
//     inline bool WouldAddOverflow(uint32_t a, uint32_t b) {
//         return a > (std::numeric_limits<uint32_t>::max() - b);
//     }
//
//     // Check if a * b would overflow for uint32_t
//     inline bool WouldMultiplyOverflow(uint32_t a, uint32_t b) {
//         if (a == 0 || b == 0) return false;
//         return a > (std::numeric_limits<uint32_t>::max() / b);
//     }
//
//     // Check if a + b would overflow for uint64_t
//     inline bool WouldAddOverflow64(uint64_t a, uint64_t b) {
//         return a > (std::numeric_limits<uint64_t>::max() - b);
//     }
//
//     // Safe addition for uint32_t
//     inline uint32_t SafeAdd(uint32_t a, uint32_t b, const char* context) {
//         if (WouldAddOverflow(a, b)) {
//             throw std::overflow_error(std::string("Integer overflow in ") + context +
//                                      ": " + std::to_string(a) + " + " + std::to_string(b));
//         }
//         return a + b;
//     }
//
//     // Safe multiplication for uint32_t
//     inline uint32_t SafeMultiply(uint32_t a, uint32_t b, const char* context) {
//         if (WouldMultiplyOverflow(a, b)) {
//             throw std::overflow_error(std::string("Integer overflow in ") + context +
//                                      ": " + std::to_string(a) + " * " + std::to_string(b));
//         }
//         return a * b;
//     }
//
//     // Safe addition for uint64_t
//     inline uint64_t SafeAdd64(uint64_t a, uint64_t b, const char* context) {
//         if (WouldAddOverflow64(a, b)) {
//             throw std::overflow_error(std::string("Integer overflow in ") + context +
//                                      ": " + std::to_string(a) + " + " + std::to_string(b));
//         }
//         return a + b;
//     }
// }
//
// // ============================================
// // Constructor and Initialization
// // ============================================
// ObladiProxy::ObladiProxy(
//     uint32_t total_objects,
//     uint32_t bin_count,
//     double epsilon_dp,
//     uint32_t max_partitions,
//     uint32_t threshold,
//     const std::vector<NetIOConnector*>& connectors,
//     uint32_t block_len,
//     uint32_t Z_param,
//     uint32_t S_param,
//     uint32_t batch_size,
//     uint64_t fixed_time_interval_ms,
//     const std::vector<int>& attribute_indices,
//     double epsilon_synopsis,
//     double epsilon_threshold_budget,
//     double threshold_pub,
//     bool use_fixed_partition_number,
//     double fixed_threshold_value,
//     const std::string& schema_str)
//     : total_objects_(total_objects)
//     , bin_count_(bin_count)
//     , epsilon_dp_(epsilon_dp)
//     , max_partitions_(max_partitions)
//     , threshold_(threshold)
//     , connectors_(connectors)
//     , block_length_(block_len)
//     , Z_param_(Z_param)
//     , S_param_(S_param)
//     , epoch_counter_(0)
//     , global_timestamp_counter_(0)
//     , total_commits_(0)
//     , total_aborts_(0)
//     , total_reads_(0)
//     , total_writes_(0)
//     , batch_size_(batch_size)
//     , fixed_time_interval_ms_(fixed_time_interval_ms)
//     , epoch_state_(EpochState::ACCEPTING_TXN)
//     , timer_running_(false)
//     // ✅ 初始化新成员变量
//     , attribute_indices_(attribute_indices)
//     , epsilon_synopsis_(epsilon_synopsis)
//     , epsilon_threshold_budget_(epsilon_threshold_budget)
//     , threshold_pub_(threshold_pub)
//     , use_fixed_partition_number_(use_fixed_partition_number)
//     , fixed_threshold_value_(fixed_threshold_value)
//     , schema_str_(schema_str)
// {
//     // ✅ Input validation
//     if (bin_count == 0) {
//         throw std::invalid_argument("[Proxy] bin_count cannot be zero");
//     }
//
//     if (total_objects == 0) {
//         throw std::invalid_argument("[Proxy] total_objects cannot be zero");
//     }
//
//     if (connectors.empty()) {
//         throw std::invalid_argument("[Proxy] connectors cannot be empty");
//     }
//
//     if (batch_size == 0) {
//         throw std::invalid_argument("[Proxy] batch_size cannot be zero");
//     }
//
//     // ✅ 验证 epsilon 分配
//     if (epsilon_synopsis <= 0 || epsilon_threshold_budget <= 0) {
//         throw std::invalid_argument("[Proxy] epsilon values must be positive");
//     }
//
//     // ✅ 验证 attribute indices
//     if (attribute_indices.empty()) {
//         throw std::invalid_argument("[Proxy] attribute_indices cannot be empty");
//     }
//
//     // ✅ Check Z_param + S_param overflow
//     if (OverflowCheck::WouldAddOverflow(Z_param, S_param)) {
//         throw std::overflow_error("[Proxy] Z_param + S_param would overflow");
//     }
//
//     std::cout << "[Proxy] Initializing with " << total_objects << " objects, "
//               << bin_count << " bins, epsilon=" << epsilon_dp << std::endl;
//     std::cout << "[Proxy] Repartitioning config:" << std::endl;
//     std::cout << "  - Schema: " << schema_str_ << std::endl;
//     std::cout << "  - Attributes: [";
//     for (size_t i = 0; i < attribute_indices_.size(); ++i) {
//         std::cout << attribute_indices_[i];
//         if (i < attribute_indices_.size() - 1) std::cout << ", ";
//     }
//     std::cout << "]" << std::endl;
//     std::cout << "  - Epsilon (synopsis): " << epsilon_synopsis_ << std::endl;
//     std::cout << "  - Epsilon (threshold): " << epsilon_threshold_budget_ << std::endl;
//     std::cout << "  - Threshold (pub): " << threshold_pub_ << std::endl;
//     std::cout << "  - Mode: " << (use_fixed_partition_number_ ? "Fixed partition number" : "Fixed threshold") << std::endl;
//     if (use_fixed_partition_number_) {
//         std::cout << "  - Target partitions: " << max_partitions_ << std::endl;
//     } else {
//         std::cout << "  - Fixed threshold: " << fixed_threshold_value_ << std::endl;
//     }
//
//     // Initialize repartitioner
//     repartitioner_ = std::make_unique<Repartition>();
//
//     // For initialization, assume uniform distribution across bins
//     // ✅ Check division safety
//     if (bin_count == 0) {
//         throw std::invalid_argument("[Proxy] bin_count cannot be zero for distribution");
//     }
//     std::vector<uint32_t> data_distribution(bin_count, total_objects / bin_count);
//
//     // Perform DP-based repartitioning
//     PerformPartitioning(data_distribution);
//
//     // ✅ Validate connector count
//     uint32_t required_connectors = partitions_metadata_.size();
//     if (connectors_.size() < required_connectors) {
//         std::cout << "[Proxy] ERROR: Not enough connectors!" << std::endl;
//         std::cout << "  - Created partitions: " << partitions_metadata_.size() << std::endl;
//         std::cout << "  - Required connectors: " << required_connectors
//                   << " (1 per partition)" << std::endl;
//         std::cout << "  - Available connectors: " << connectors_.size() << std::endl;
//         throw std::runtime_error("[Proxy] Insufficient connectors for parallel partition access");
//     } else {
//         std::cout << "[Proxy] Connector check: ✅ " << connectors_.size()
//                   << " available for " << partitions_metadata_.size()
//                   << " partitions (1:1 mapping)" << std::endl;
//     }
//
//     // Initialize partition structures and ORAM instances
//     InitializePartitions();
//
//     // Start first epoch
//     current_epoch_ = std::make_unique<Epoch>(epoch_counter_++);
//
//     std::cout << "[Proxy] Initialization complete. Created " << partitions_.size()
//               << " partitions." << std::endl;
//
//     // Set repartitioning threshold (default: 10% of total objects or min 1000)
//     // ✅ Safe division
//     uint32_t ten_percent = total_objects_ / 10;
//     repartition_threshold_ = std::max(1000u, ten_percent);
//
//     std::cout << "[Proxy] Repartition threshold set to "
//               << repartition_threshold_ << std::endl;
// }
//
// ObladiProxy::~ObladiProxy() {
//     std::cout << "[Proxy] Shutting down..." << std::endl;
//     StopEpochTimer();
//     // Cleanup partitions (unique_ptrs handle ORAM cleanup)
//     partitions_.clear();
//
//     std::cout << "[Proxy] Shutdown complete." << std::endl;
// }
//
// // ============================================
// // Phase 1: Data Partitioning
// // ============================================
// void ObladiProxy::PerformPartitioning(const std::vector<uint32_t>& data_distribution) {
//     std::cout << "[Proxy] Performing DP-based partitioning..." << std::endl;
//
//     // ✅ Step 1: 从 data_distribution 生成合成数据
//     // 格式取决于 schema_str_ 和 attribute_indices_
//     std::vector<std::string> synthetic_data;
//     synthetic_data.reserve(total_objects_);
//
//     // 为每个 bin 生成对应数量的数据
//     for (uint32_t bin_id = 0; bin_id < bin_count_; ++bin_id) {
//         uint32_t count = data_distribution[bin_id];
//         for (uint32_t i = 0; i < count; ++i) {
//             // 简单格式: 每行一个 bin_id
//             // 如果有多个属性，可以用逗号分隔，例如 "bin_id,timestamp"
//             synthetic_data.push_back(std::to_string(bin_id));
//         }
//     }
//
//     std::cout << "[Proxy] Generated " << synthetic_data.size()
//               << " synthetic data points" << std::endl;
//
//     // ✅ Step 2: 调用 Repartition_Main，使用成员变量中的参数
//     try {
//         partitions_metadata_ = repartitioner_->Repartition_Main(
//             synthetic_data,
//             schema_str_,                    // 从成员变量
//             attribute_indices_,              // 从成员变量
//             epsilon_synopsis_,               // 从成员变量
//             epsilon_threshold_budget_,       // 从成员变量
//             threshold_pub_,                  // 从成员变量
//             use_fixed_partition_number_,     // 从成员变量
//             max_partitions_                  // 目标分区数
//         );
//     } catch (const std::exception& e) {
//         std::cerr << "[Proxy] Repartitioning failed: " << e.what() << std::endl;
//         throw;
//     }
//
//     std::cout << "[Proxy] Partitioning complete. Created "
//               << partitions_metadata_.size() << " partitions." << std::endl;
//
//     // ✅ Step 3: 打印详细的分区信息
//     std::cout << "[Proxy] Partition details:" << std::endl;
//     uint32_t total_bins_assigned = 0;
//     uint32_t total_real_data = 0;
//     uint32_t total_dummy_data = 0;
//
//     for (size_t i = 0; i < partitions_metadata_.size(); ++i) {
//         const auto& part = partitions_metadata_[i];
//         total_bins_assigned += part.index.size();
//         total_real_data += part.synopsis;
//         total_dummy_data += part.dummy_num;
//
//         std::cout << "  Partition " << i << ":" << std::endl;
//         std::cout << "    - Bins: " << part.index.size()
//                   << " (indices: [";
//         for (size_t j = 0; j < std::min(part.index.size(), size_t(5)); ++j) {
//             std::cout << part.index[j];
//             if (j < std::min(part.index.size(), size_t(5)) - 1) std::cout << ", ";
//         }
//         if (part.index.size() > 5) std::cout << ", ...";
//         std::cout << "])" << std::endl;
//         std::cout << "    - Real data (synopsis): " << part.synopsis << std::endl;
//         std::cout << "    - Noisy synopsis: " << part.noisy_synopsis << std::endl;
//         std::cout << "    - Dummy data: " << part.dummy_num << std::endl;
//     }
//
//     std::cout << "[Proxy] Summary:" << std::endl;
//     std::cout << "  - Total bins assigned: " << total_bins_assigned
//               << " / " << bin_count_ << std::endl;
//     std::cout << "  - Total real data: " << total_real_data << std::endl;
//     std::cout << "  - Total dummy data: " << total_dummy_data << std::endl;
//     std::cout << "  - Overhead: "
//               << (total_real_data > 0 ?
//                   (100.0 * total_dummy_data / total_real_data) : 0.0)
//               << "%" << std::endl;
//
//     // ✅ 验证所有 bins 都被分配
//     if (total_bins_assigned != bin_count_) {
//         std::cerr << "[Proxy] WARNING: Not all bins were assigned to partitions!"
//                   << std::endl;
//     }
// }
//
// void ObladiProxy::InitializePartitions() {
//     std::cout << "[Partition Init] Initializing partitions (dummy trees, real data via BulkInsert)...\n";
//
//     if (partitions_metadata_.empty()) {
//         throw std::runtime_error("[InitPartitions] partitions_metadata_ is empty; run PerformPartitioning() first");
//     }
//     if (connectors_.empty()) {
//         throw std::runtime_error("[InitPartitions] connectors_ is empty");
//     }
//
//     partitions_.clear();
//     partitions_.reserve(partitions_metadata_.size());
//
//     // find the connector's allocation;
//     const bool use_random_map =
//         (!part_to_conn_.empty() && part_to_conn_.size() == partitions_metadata_.size());
//
//     for (size_t i = 0; i < partitions_metadata_.size(); ++i) {
//         const auto& meta = partitions_metadata_[i];
//
//         // calculate the cap = real + dummy;
//         uint32_t real  = meta.synopsis;
//         uint32_t dummy = meta.dummy_num;
//
// #ifdef HAVE_OVERFLOW_CHECK
//         if (OverflowCheck::WouldAddOverflow(real, dummy)) {
//             throw std::overflow_error("[InitPartitions] Capacity overflow for partition " + std::to_string(i));
//         }
// #endif
//         uint32_t cap = real + dummy;
//         if (cap == 0) {
//             throw std::invalid_argument("[InitPartitions] Partition capacity cannot be zero");
//         }
//
// #ifdef HAVE_OVERFLOW_CHECK
//         uint32_t bucket_size = OverflowCheck::SafeAdd(Z_param_, S_param_, "bucket_size");
// #else
//         uint32_t bucket_size = Z_param_ + S_param_;
// #endif
//
//         auto pinfo = std::make_unique<PartitionInfo>(
//             static_cast<uint32_t>(i),
//             meta.index,
//             real,
//             dummy,
//             /*r_ops*/cap,
//             /*w_ops*/cap,
//             batch_size_
//         );
//         pinfo -> batch_size = batch_size_;
//
//         // choose connector;
//         size_t conn_idx = use_random_map
//                             ? part_to_conn_[i]
//                             : (i % connectors_.size());
//         if (conn_idx >= connectors_.size()) {
//             throw std::runtime_error("[InitPartitions] connector index out of range");
//         }
//         pinfo -> connector = connectors_[conn_idx];
//
//         std::string oram_name = "partition_" + std::to_string(i);
//
//         try {
//             // 1) construct ORAM (record all metadata info);
//             pinfo->oram = std::make_unique<RingORAM>(
//                 cap,            // capacity = real + dummy
//                 bucket_size,    // Z + S
//                 oram_name,
//                 block_length_,
//                 pinfo->connector,
//                 S_param_        // every bucket's dummy slots #;
//             );
//
//             // record stash cap;
//             pinfo -> stash_capacity = pinfo -> oram -> GetStashCapacity();
//
//             std::cout << "  [InitPartition] pid=" << i
//                       << " cap=" << cap
//                       << " bucket_size=" << bucket_size
//                       << " stash_capacity=" << pinfo->stash_capacity
//                       << " connector_idx=" << conn_idx
//                       << " name=" << oram_name
//                       << "\n";
//         } catch (const std::exception& e) {
//             std::cerr << "[InitPartitions][ERROR] Failed to init ORAM for pid "
//                       << i << ": " << e.what() << "\n";
//             throw;
//         }
//
//         // 2). initialize read & write batch queue;
//         pinfo -> current_read_batches.clear();
//         pinfo -> current_read_batches.reserve(pinfo -> num_read_batches);
//         for (uint32_t b = 0; b < pinfo->num_read_batches; ++b) {
//             pinfo->current_read_batches.push_back(std::make_unique<Bat>(
//                 b, epoch_counter_, static_cast<uint32_t>(i),
//                 /*is_read=*/true, pinfo -> batch_size));
//         }
//
//         pinfo -> current_write_batches.clear();
//         pinfo -> current_write_batches.reserve(pinfo -> num_write_batches);
//         for (uint32_t b = 0; b < pinfo -> num_write_batches; ++b) {
//             pinfo -> current_write_batches.push_back(std::make_unique<Bat>(
//                 b, epoch_counter_, static_cast<uint32_t>(i),
//                 /*is_read=*/false, pinfo->batch_size));
//         }
//
//         pinfo -> current_read_batch_idx  = 0;
//         pinfo -> current_write_batch_idx = 0;
//
//         partitions_.push_back(std::move(pinfo));
//     }
//
//     std::cout << "[Partition Init] ✅ "
//               << partitions_.size()
//               << " partitions initialized (dummy trees ready; use BulkInsertToPartition next)\n";
// }
//
// void ObladiProxy::LoadInitialData(uint32_t bin_size,
//     const std::unordered_map<uint32_t, std::string>& initial_data) {
//     std::cout << "\n========================================\n";
//     std::cout << "[Proxy::LoadInitialData] Loading " << initial_data.size()
//               << " initial keys (bin_size=" << bin_size << ")...\n";
//     std::cout << "========================================\n\n";
//
//     if (initial_data.empty()) {
//         std::cout << "[LoadInitialData] No initial data to load\n";
//         return;
//     }
//     if (bin_size == 0) {
//         throw std::invalid_argument("[LoadInitialData] bin_size must be > 0");
//     }
//
//     // Part 1:) order the unordered_map;
//     std::vector<std::pair<uint32_t, std::string>> kv(initial_data.begin(), initial_data.end());
//     std::sort(kv.begin(), kv.end(),
//               [](const auto& a, const auto& b){ return a.first < b.first; });
//
//     // --- 1) Mass-bins by "quantity threshold" ---
//     // bin_counts[i] = number of keys in the i-th bin
//     // key_to_bin[k] = which bin_id k belongs to
//     std::vector<uint32_t> bin_counts;
//     bin_counts.reserve((kv.size() + bin_size - 1) / bin_size);
//
//     std::unordered_map<uint32_t, uint32_t> key_to_bin;
//     key_to_bin.reserve(kv.size());
//
//     uint32_t cur_count = 0;
//     uint32_t cur_bin_id = 0;
//     for (const auto& [k, v] : kv) {
//         key_to_bin[k] = cur_bin_id;
//         ++cur_count;
//         if (cur_count == bin_size) {
//             bin_counts.push_back(cur_count);
//             cur_count = 0;
//             ++cur_bin_id;
//         }
//     }
//
//     // Q1: How to deal with the tail bin?
//     // 1. merge to the previous bin;
//     // 2. leave it as a separate bin;
//     // 2.a. Use this tail bin to do the SVT + DP;
//     // 2.b. Leave this bin in the insert-only tree, waiting new data;
//     if (cur_count > 0) {
//         bin_counts.push_back(cur_count);
//         ++cur_bin_id;
//     }
//
//     std::cout << "[LoadInitialData] Built " << bin_counts.size()
//               << " mass-bins by count (tail size=" << cur_count << ")\n";
//
//     // --- 2) use DP partitions（SVT+DP method） ---
//     PerformPartitioning(bin_counts);
//
//     std::cout << "[LoadInitialData] DP partitioning produced "
//               << partitions_metadata_.size() << " partitions\n";
//
//     // --- 3) assign every partition random and unrepeated connector ---
//     const size_t num_parts = partitions_metadata_.size();
//     const size_t num_conns = connectors_.size();
//
//     if (num_parts == 0) {
//         std::cerr << "[LoadInitialData] No partitions to assign connectors\n";
//     } else {
//         if (num_conns < num_parts) {
//             throw std::runtime_error(
//                 "[LoadInitialData] Not enough connectors for one-to-one mapping: "
//                 "parts=" + std::to_string(num_parts) +
//                 ", connectors=" + std::to_string(num_conns));
//         }
//
//         // generate 0 tonum_conns-1 indices，and random order;
//         std::vector<size_t> perm(num_conns);
//         std::iota(perm.begin(), perm.end(), 0);
//         std::mt19937_64 rng(42);
//         std::shuffle(perm.begin(), perm.end(), rng);
//
//         // only use num_parts as partition → connector mapping;
//         perm.resize(num_parts);
//
//         // --- 4) initialize partitions（construct RingORAM etc）
//         InitializePartitions();
//
//         // --- 5) according to the perm assigning each partition for specific connector;
//         for (size_t pid = 0; pid < partitions_.size(); ++pid) {
//             size_t conn_idx = perm[pid];
//             auto* new_conn  = connectors_.at(conn_idx);
//             partitions_[pid]->connector = new_conn;
//             if (partitions_[pid]->oram) {
//                 partitions_[pid]->oram->SetConnector(new_conn);
//             }
//         }
//
//     }
//
//     // --- 6) find the kep-partition mapping ---
//     // Firstly, check which bin this key is in;
//     // Then, check which partition this key is in;
//     std::unordered_map<uint32_t, std::vector<uint32_t>> partition_keys;
//     std::unordered_map<uint32_t, std::vector<std::string>> partition_vals;
//     size_t unmapped = 0;
//
//     // to quickly find bin ∈ partition，order the partition's index and put in the hash set;
//     std::vector<std::unordered_set<uint32_t>> part_bin_sets;
//     part_bin_sets.reserve(partitions_metadata_.size());
//     for (const auto& meta : partitions_metadata_) {
//         // ✅ meta.index 现在是 vector<string>，需要转换为 uint32_t
//         std::unordered_set<uint32_t> bin_set;
//         for (const std::string& bin_str : meta.index) {
//             bin_set.insert(std::stoul(bin_str));
//         }
//         part_bin_sets.push_back(std::move(bin_set));
//     }
//
//     for (const auto& [k, v] : kv) {
//         const uint32_t b = key_to_bin[k];
//         bool found = false;
//         for (size_t pid = 0; pid < partitions_metadata_.size(); ++pid) {
//             if (part_bin_sets[pid].count(b)) {
//                 partition_keys[static_cast<uint32_t>(pid)].push_back(k);
//                 partition_vals[static_cast<uint32_t>(pid)].push_back(v);
//                 key_to_partition_[k] = static_cast<uint32_t>(pid);
//                 found = true;
//                 break;
//             }
//         }
//         if (!found) {
//             ++unmapped;
//             if (unmapped <= 10) {
//                 std::cerr << "[LoadInitialData] Warning: key " << k
//                           << " (bin " << b << ") not mapped to any partition\n";
//             }
//         }
//     }
//     if (unmapped) {
//         std::cerr << "[LoadInitialData] Unmapped keys: " << unmapped << "\n";
//     }
//
//     // --- 5) batch every partition's key/value（call BulkInsertToPartition） ---
//     {
//         std::mutex err_mu;
//         std::atomic<size_t> grand_total{0};
//         std::vector<std::thread> ths;
//         ths.reserve(partition_keys.size());
//
//         const bool enable_parallel_for_partition = false;
//         const size_t per_partition_max_workers   = std::thread::hardware_concurrency() ?
//                                                    std::thread::hardware_concurrency() : 4;
//
//         for (const auto& [pid, keys] : partition_keys) {
//             // Create local copies to avoid structured binding capture issue
//             uint32_t partition_id = pid;
//             const auto& local_keys = keys;  // Create reference to keys
//             ths.emplace_back([&, partition_id]() {
//                 try {
//                     const auto& vals = partition_vals.at(partition_id);
//
//                     BulkInsertToPartition(
//                         partition_id,
//                         local_keys,
//                         vals,
//                         /*enable_parallel_for_partition=*/enable_parallel_for_partition,
//                         /*max_workers=*/per_partition_max_workers
//                     );
//
//                     grand_total.fetch_add(local_keys.size(), std::memory_order_relaxed);
//                 } catch (const std::exception& e) {
//                     std::lock_guard<std::mutex> g(err_mu);
//                     std::cerr << "[InitLoad] pid=" << partition_id << " failed: " << e.what() << "\n";
//                 }
//             });
//         }
//         for (auto& t : ths) t.join();
//
//         std::cout << "\n[LoadInitialData] ✅ Completed. Total keys loaded: "
//                   << grand_total.load() << "\n";
//     }
// }
//
// void ObladiProxy::BulkInsertToPartition(
//     uint32_t partition_id,
//     const std::vector<uint32_t>& keys,
//     const std::vector<std::string>& values,
//     bool enable_parallel_for_partition /*= false*/,
//     size_t max_workers /*= std::thread::hardware_concurrency() ? std::thread::hardware_concurrency() : 4*/
// ) {
//     if (keys.size() != values.size()) {
//         throw std::invalid_argument("[BulkInsertToPartition] Keys and values size mismatch");
//     }
//
//     if (partition_id >= partitions_.size()) {
//         throw std::invalid_argument("[BulkInsertToPartition] Invalid partition_id: " +
//                                    std::to_string(partition_id));
//     }
//
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo || !pinfo -> oram) {
//         throw std::runtime_error("[BulkInsertToPartition] Null partition or ORAM");
//     }
//
//     const size_t total_keys = keys.size();
//     if (total_keys == 0) {
//         std::cout << "[BulkInsertToPartition] Nothing to insert for partition "
//                   << partition_id << "\n";
//         return;
//     }
//
//     // Strategy: Use write batches mechanism
//     // batch_size should equal stash_capacity to handle all data at once
//     uint32_t effective_batch_size = pinfo -> stash_capacity;
//     if (effective_batch_size == 0) {
//         throw std::runtime_error("[BulkInsertToPartition] stash_capacity is 0");
//     }
//
//     std::cout << "[BulkInsertToPartition] pid=" << partition_id
//               << " total=" << total_keys
//               << " stash_cap=" << effective_batch_size
//               << " parallel=" << (enable_parallel_for_partition ? "ON" : "OFF") << "\n";
//
//     // Part 1: Create batches;
//     struct LocalBatch {
//         std::unique_ptr<Bat> bat;
//     };
//     std::vector<LocalBatch> insert_batches;
//     insert_batches.reserve((total_keys + effective_batch_size - 1) / effective_batch_size);
//
//     const size_t num_full_batches = total_keys / effective_batch_size;
//     const size_t remaining        = total_keys % effective_batch_size;
//
//     std::cout << "  Will create " << num_full_batches
//               << " full batches + " << (remaining > 0 ? "1" : "0")
//               << " partial batch" << std::endl;
//
//     // Part 2: Create full batches;
//     for (size_t b = 0; b < num_full_batches; ++b) {
//         auto batch = std::make_unique<Bat>(
//             static_cast<uint32_t>(b),
//             /*epoch_id*/ 0,
//             partition_id,
//             /*is_read=*/false,
//             effective_batch_size
//         );
//         const size_t start = b * effective_batch_size;
//         for (size_t i = 0; i < effective_batch_size; ++i) {
//             const size_t idx = start + i;
//             batch->requests.emplace_back(
//                 SYSTEM_TXN_ID, keys[idx], OpType::INSERT, values[idx]);
//         }
//         insert_batches.push_back({std::move(batch)});
//     }
//
//     // Part 3: Create partial batch (if remaining)
//     if (remaining > 0) {
//         auto batch = std::make_unique<Bat>(
//             static_cast<uint32_t>(num_full_batches),
//             0,
//             partition_id,
//             false,
//             static_cast<uint32_t>(remaining)
//         );
//
//         const size_t start = num_full_batches * effective_batch_size;
//         for (size_t i = 0; i < remaining; ++i) {
//             const size_t idx = start + i;
//             batch->requests.emplace_back(
//                 SYSTEM_TXN_ID, keys[idx], OpType::INSERT, values[idx]);
//         }
//         // Q: If the # of ops < batch size, whether we need to add paddings?
//         while (batch -> requests.size() < effective_batch_size) batch -> requests.emplace_back();
//         insert_batches.push_back({std::move(batch)});
//     }
//
//     std::cout << "  Created " << insert_batches.size()
//               << " batch(es): " << num_full_batches
//               << " full, " << (remaining ? 1 : 0) << " partial\n";
//
//     // Part 4: Execute all batches
//     auto run_one_batch = [&](LocalBatch& lb, size_t ordinal) {
//         Bat* batch = lb.bat.get();
//         std::cout << "    [pid " << partition_id << "] Exec batch "
//                   << (ordinal + 1) << "/" << insert_batches.size() << " ..." << std::flush;
//
//         // Inside the batch, run op serially;
//         for (const auto& req : batch->requests) {
//             if (req.is_dummy) continue;
//             pinfo -> oram -> access(req.key, OpType::INSERT, req.data);
//         }
//         std::cout << " ✅\n";
//     };
//
//     if (!enable_parallel_for_partition || insert_batches.size() <= 1) {
//         // serially execute;
//         for (size_t i = 0; i < insert_batches.size(); ++i) {
//             run_one_batch(insert_batches[i], i);
//         }
//     } else {
//         // parallel execution;
//         const size_t workers = std::max<size_t>(1, std::min<size_t>(
//             max_workers ? max_workers : 4, insert_batches.size()));
//
//         std::atomic<size_t> next{0};
//         std::vector<std::thread> pool;
//         pool.reserve(workers);
//
//         for (size_t t = 0; t < workers; ++t) {
//             pool.emplace_back([&](){
//                 while (true) {
//                     size_t idx = next.fetch_add(1, std::memory_order_relaxed);
//                     if (idx >= insert_batches.size()) break;
//                     run_one_batch(insert_batches[idx], idx);
//                 }
//             });
//         }
//         for (auto& th : pool) th.join();
//     }
//
//     std::cout << "[BulkInsertToPartition] ✅ pid=" << partition_id
//               << " inserted " << total_keys << " key(s)\n";
// }
//
// void ObladiProxy::ClearPartitions() {
//     std::lock_guard<std::mutex> lock(partition_mutex_);
//
//     std::cout << "[Proxy::ClearPartitions] Clearing existing partitions..." << std::endl;
//
//     partitions_.clear();
//     partitions_metadata_.clear();
//     key_to_partition_.clear();
//
//     // 不清空 insert partition，因为我们可能还需要它
//     // insert_partition_ = nullptr;
//
//     std::cout << "[Proxy::ClearPartitions] Partitions cleared." << std::endl;
// }
//
// bool ObladiProxy::IsKeyInInsertPartition(uint32_t key) const {
//     std::lock_guard<std::mutex> lock(partition_mutex_);
//
//     auto it = key_to_partition_.find(key);
//     if (it == key_to_partition_.end()) {
//         return false; // Key doesn't exist yet
//     }
//
//     // Check if mapped to the dedicated insert partition (insert_partition_pid_)
//     return it->second == insert_partition_pid_;
// }
//
// std::vector<uint32_t> ObladiProxy::GetPartitionHeights() const {
//     // 和 IsKeyInInsertPartition 一样，用 partition_mutex_ 保护一下
//     std::lock_guard<std::mutex> lock(partition_mutex_);
//
//     std::vector<uint32_t> heights;
//     heights.reserve(partitions_.size());
//
//     for (size_t pid = 0; pid < partitions_.size(); ++pid) {
//         const auto &pinfo_up = partitions_[pid];
//         if (!pinfo_up) continue;
//
//         if (static_cast<int>(pid) == insert_partition_pid_) {
//             continue;
//         }
//
//         const PartitionInfo *pinfo = pinfo_up.get();
//         if (pinfo->oram) {
//             heights.push_back(pinfo->oram->GetHeight());
//         }
//     }
//
//     return heights;
// }
//
// uint32_t ObladiProxy::GetBinCount() const {
//     return bin_count_;
// }
//
// std::vector<uint32_t> ObladiProxy::GetPartitionTrueCounts() const {
//     std::vector<uint32_t> counts;
//     counts.reserve(partitions_metadata_.size());
//     for (const auto& p : partitions_metadata_) {
//         counts.push_back(p.synopsis);
//     }
//     return counts;
// }
//
// std::vector<std::vector<std::string>> ObladiProxy::GetPartitionBinIndices() const {
//     std::vector<std::vector<std::string>> idx;
//     idx.reserve(partitions_metadata_.size());
//     for (const auto& p : partitions_metadata_) {
//         idx.push_back(p.index);
//     }
//     return idx;
// }
//
// // ============================================
// // Phase 2: Transaction Interface
// // ============================================
// uint32_t ObladiProxy::BeginTransaction() {
//     // Step 1: Check the epoch status;
//     {
//         std::lock_guard<std::mutex> state_lock(epoch_state_mutex_);
//         if (epoch_state_ != EpochState::ACCEPTING_TXN) {
//             std::cerr << "[BeginTxn] ERROR: Not accepting new transactions!" << std::endl;
//             return static_cast<uint32_t>(-1);
//         }
//     } // ⭐ epoch_state_mutex_ free;
//
//     // Step 2: check whether the epoch is active?
//     if (!IsEpochActive()) {
//         std::cerr << "[BeginTxn] ERROR: No active epoch!" << std::endl;
//         return static_cast<uint32_t>(-1);
//     }
//
//     // Step 3: check the time;
//     if (ShouldCloseEpoch()) {
//         std::cerr << "[BeginTxn] ERROR: Time interval expired!" << std::endl;
//         return static_cast<uint32_t>(-1);
//     }
//
//     // ⭐ Step 4: get epoch_mutex_（right now does not have other key;）
//     std::lock_guard<std::mutex> epoch_lock(epoch_mutex_);
//
//     {
//         std::lock_guard<std::mutex> state_lock(epoch_state_mutex_);
//         if (epoch_state_ != EpochState::ACCEPTING_TXN) {
//             std::cerr << "[BeginTxn] ERROR: Epoch closed during transaction creation!" << std::endl;
//             return static_cast<uint32_t>(-1);
//         }
//     }
//
//     uint32_t local_txn_id  = current_epoch_->next_txn_id.fetch_add(1);
//     uint32_t timestamp     = AssignTimestamp();
//
//     auto txn = std::make_unique<Transaction>(local_txn_id, timestamp);
//
//     uint32_t global_txn_id = (current_epoch_->epoch_id << 16) | local_txn_id;
//
//     {
//         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//         active_transactions_[global_txn_id] = std::move(txn);
//     }
//
//     current_epoch_->epoch_txns.push_back(global_txn_id);
//
//     {
//         std::lock_guard<std::mutex> op_lock(txn_op_status_mutex_);
//         txn_op_status_[global_txn_id] = TxnOperationStatus{};
//     }
//
//     std::cout << "[BeginTxn] Started txn " << global_txn_id
//               << " with timestamp " << timestamp << std::endl;
//
//     return global_txn_id;
// }
//
// void ObladiProxy::StartEpochTimer() {
//     // If a timer is already running, stop it safely first;
//     if (timer_running_.load()) {
//         std::cout << "[StartTimer] Timer already running, stopping first...\n";
//         StopEpochTimer();
//     }
//
//     // Avoid to be concurrent with EndEpoch;
//     if (end_epoch_in_progress_.load()) {
//         std::cout << "[StartTimer] EndEpoch in progress, waiting...\n";
//         for (int i = 0; i < 100 && end_epoch_in_progress_.load(); ++i)
//             std::this_thread::sleep_for(std::chrono::milliseconds(10));
//         if (end_epoch_in_progress_.load())
//             std::cerr << "[StartTimer] WARNING: EndEpoch still in progress after waiting!\n";
//     }
//
//     // Record the starting time and set the beginning sign;
//     epoch_start_time_ = std::chrono::steady_clock::now();
//     timer_running_.store(true);
//
//     // Firstly construct the temporary tread for obj t;
//     std::thread t(&ObladiProxy::EpochTimerThreadFunc, this);
//
//     // If the member thread object is still joinable, handle it first;
//     if (epoch_timer_thread_.joinable()) {
//         if (epoch_timer_thread_.get_id() == std::this_thread::get_id()) {
//             // Starting the next round in the timer thread:
//             // handing the new thread t to the system (detaching it);
//             std::cerr << "[StartTimer] Called from timer thread; detaching new thread\n";
//             t.detach();
//             return;
//         } else {
//             epoch_timer_thread_.join();
//         }
//     }
//
//     // Then move the temporary thread to a member variable;
//     epoch_timer_thread_ = std::move(t);
//     std::cout << "[StartTimer] Epoch timer started (interval: "
//               << fixed_time_interval_ms_ << "ms)...\n";
// }
//
// void ObladiProxy::StopEpochTimer() {
//     // stop sign and weak for awaiting;
//     timer_running_.store(false);
//     timer_cv_.notify_all();
//
//     if (epoch_timer_thread_.joinable()) {
//         if (epoch_timer_thread_.get_id() == std::this_thread::get_id()) {
//             // cannot join itself，so use detach;
//             std::cerr << "[StopTimer] Called from timer thread; detaching self\n";
//             epoch_timer_thread_.detach();
//         } else {
//             epoch_timer_thread_.join();
//         }
//     }
// }
//
// void ObladiProxy::EpochTimerThreadFunc() {
//     std::cout << "[Timer] Timer thread started\n";
//
//     while (timer_running_.load()) {
//         auto now = std::chrono::steady_clock::now();
//         auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - epoch_start_time_)
//                            .count(); // int64_t
//
//         const uint64_t interval  = fixed_time_interval_ms_;
//         if (elapsed >= static_cast<int64_t>(interval)) {
//             std::cout << "[Timer] Interval (" << interval << "ms) reached, attempting to close epoch...\n";
//
//             bool expected = false;
//             if (end_epoch_in_progress_.compare_exchange_strong(expected, true)) {
//                 std::cout << "[Timer] Successfully acquired EndEpoch lock\n";
//
//                 bool state_changed = false;
//                 {
//                     std::lock_guard<std::mutex> g(epoch_state_mutex_);
//                     if (epoch_state_ == EpochState::ACCEPTING_TXN) {
//                         epoch_state_ = EpochState::CLOSED;
//                         state_changed = true;
//                         std::cout << "[Timer] Epoch state changed to CLOSED\n";
//                     }
//                 }
//
//                 if (state_changed) {
//                     timer_running_.store(false);
//                     timer_cv_.notify_all();
//
//                     std::cout << "[Timer] Triggering EndEpoch...\n";
//                     try {
//                         EndEpoch();
//                         std::cout << "[Timer] EndEpoch completed successfully\n";
//                     } catch (const std::exception& e) {
//                         std::cerr << "[Timer] EndEpoch threw exception: " << e.what() << "\n";
//                     }
//                 }
//
//                 end_epoch_in_progress_.store(false);
//                 break;
//             } else {
//                 std::cout << "[Timer] Another thread is already executing EndEpoch, timer thread exiting\n";
//                 timer_running_.store(false);
//                 timer_cv_.notify_all();
//                 break;
//             }
//         }
//
//         // Recalculate the safe waiting time;
//         const uint64_t remaining = (elapsed >= static_cast<int64_t>(interval))
//                                        ? 0ULL
//                                        : (interval - static_cast<uint64_t>(elapsed));
//         const uint64_t wait_ms = std::min<uint64_t>(remaining, 100ULL);
//
//         std::unique_lock<std::mutex> lk(epoch_state_mutex_);
//         timer_cv_.wait_for(lk, std::chrono::milliseconds(wait_ms), [this]{
//             return !timer_running_.load();
//         });
//
//         if (!timer_running_.load()) {
//             std::cout << "[Timer] Timer stopped externally, exiting\n";
//             break;
//         }
//     }
//
//     std::cout << "[Timer] Timer thread exiting\n";
// }
//
// bool ObladiProxy::ShouldCloseEpoch() {
//     auto now = std::chrono::steady_clock::now();
//     auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
//         now - epoch_start_time_).count();
//     return elapsed >= static_cast<int64_t>(fixed_time_interval_ms_);
// }
//
// void ObladiProxy::AddReadOp(uint32_t global_txn_id, uint32_t key, uint32_t partition_id) {
//     std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//
//     auto it = active_transactions_.find(global_txn_id);
//     if (it == active_transactions_.end()) {
//         std::cerr << "[AddReadOp] ERROR: Unknown txn " << global_txn_id << std::endl;
//         return;
//     }
//     Transaction* txn = it -> second.get();
//
//     // ⭐ use the given partition_id，do not need to check key_to_partition_;
//     PerPartitionTxnState& pstate = txn -> per_partition[partition_id];
//
//     Operat op{};
//     op.txn      = global_txn_id;
//     op.type     = OpType::READ;
//     op.key      = key;
//     op.last_one = false;
//     op.value.clear();
//
//     pstate.read_ops.push_back(op);
//     txn -> reads.emplace(key, std::string{});
//
//     std::cout << "[AddReadOp] Txn " << global_txn_id
//               << " READ key " << key
//               << " in partition " << partition_id << std::endl;
// }
//
// bool ObladiProxy::Read(uint32_t txn_id, uint32_t key, std::string& out_value) {
//     total_reads_.fetch_add(1);
//
//     Transaction* txn = nullptr;
//     {
//         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//         auto it = active_transactions_.find(txn_id);
//         if (it == active_transactions_.end() || it -> second -> aborted) return false;
//         txn = it -> second.get();
//     }
//
//     uint32_t partition_id = 0;
//     bool key_found = false;
//
//     {
//         std::lock_guard<std::mutex> part_lock(partition_mutex_);
//         auto ktp_it = key_to_partition_.find(key);
//
//         if (ktp_it != key_to_partition_.end()) {
//             uint32_t assigned_pid = ktp_it->second;
//
//             if (assigned_pid < partitions_.size()) {
//                 PartitionInfo* pinfo = partitions_[assigned_pid].get();
//                 if (pinfo && pinfo -> oram) {
//                     if (pinfo -> oram->Exists(key)) {
//                         partition_id = assigned_pid;
//                         key_found = true;
//                     }
//                 }
//             }
//
//             if (!key_found) {
//                 if (insert_partition_ && insert_partition_ -> oram) {
//                     if (insert_partition_ -> oram->Exists(key)) {
//                         partition_id = insert_partition_pid_;
//                         key_found = true;
//                     }
//                 }
//             }
//
//             if (!key_found) {
//                 std::cerr << "[Read] Key " << key << " not found" << std::endl;
//                 txn->aborted = true;
//                 total_aborts_.fetch_add(1);
//                 return false;
//             }
//         } else {
//             std::cerr << "[Read] Key " << key << " not in key_to_partition_\n";
//             txn->aborted = true;
//             total_aborts_.fetch_add(1);
//             return false;
//         }
//     }
//
//     // ⭐ pass partition_id
//     AddReadOp(txn_id, key, partition_id);
//     out_value.clear();
//     return true;
// }
//
// void ObladiProxy::AddWriteOp(uint32_t global_txn_id,
//                              uint32_t key,
//                              uint32_t partition_id,
//                              const std::string& value,
//                              bool is_new_key) {
//     std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//     auto it = active_transactions_.find(global_txn_id);
//     if (it == active_transactions_.end()) {
//         std::cerr << "[AddWriteOp] ERROR: Unknown txn " << global_txn_id << std::endl;
//         return;
//     }
//     Transaction* txn = it->second.get();
//
//     // ⭐ use partition_id，don't need to check key_to_partition_;
//     PerPartitionTxnState& pstate = txn -> per_partition[partition_id];
//
//     Operat op{};
//     op.txn      = global_txn_id;
//     op.type     = is_new_key ? OpType::INSERT : OpType::WRITE;
//     op.key      = key;
//     op.last_one = false;
//     op.value    = value;
//
//     pstate.write_ops.push_back(op);
//     txn -> writes[key] = value;
//
//     std::cout << "[AddWriteOp] Txn " << global_txn_id
//               << " WRITE key " << key
//               << " in partition " << partition_id << std::endl;
// }
//
// bool ObladiProxy::Write(uint32_t txn_id, uint32_t key, const std::string& value) {
//     total_writes_.fetch_add(1);
//
//     // Step 1: find the txn;
//     Transaction* txn = nullptr;
//     uint32_t timestamp = 0;
//     {
//         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//         auto it = active_transactions_.find(txn_id);
//         if (it == active_transactions_.end()) {
//             return false;
//         }
//         txn = it->second.get();
//         if (txn->aborted) {
//             return false;
//         }
//         timestamp = txn->timestamp;
//     }
//
//     // Step 2: check partition_id;
//     bool is_new_key = false;
//     uint32_t partition_id = 0;
//     {
//         std::lock_guard<std::mutex> part_lock(partition_mutex_);
//         auto key_it = key_to_partition_.find(key);
//
//         if (key_it == key_to_partition_.end()) {
//             is_new_key = true;
//             partition_id = insert_partition_pid_;
//             key_to_partition_[key] = insert_partition_pid_;
//
//             insert_count_.fetch_add(1);
//             if (ShouldRepartition()) {
//                 std::cout << "[Write] Repartition threshold reached" << std::endl;
//             }
//         } else {
//             partition_id = key_it->second;
//             is_new_key = false;
//         }
//     }
//
//     // Step 3: change the data;
//     AddWriteOp(txn_id, key, partition_id, value, is_new_key);
//
//     // ⭐ Step 4: if no txn_mutex_ add to the version chain;
//     AddToVersionChain(partition_id, key, txn_id, value, timestamp);
//
//     std::cout << "[Write] Txn " << txn_id
//               << (is_new_key ? " inserted" : " updated")
//               << " key " << key << std::endl;
//
//     return true;
// }
//
// bool ObladiProxy::Commit(uint32_t txn_id) {
//     // ⭐ 先释放 txn_mutex_，再调用 MarkOperationReceived
//     {
//         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//
//         auto it = active_transactions_.find(txn_id);
//         if (it == active_transactions_.end()) {
//             return false;
//         }
//
//         Transaction* txn = it->second.get();
//         if (txn->aborted) {
//             return false;
//         }
//
//         txn->finished = true;  // ⭐ 先设置 finished
//     }  // ⭐ 释放 txn_mutex_
//
//     // ⭐ 在没有持有 txn_mutex_ 时调用 MarkOperationReceived
//     MarkOperationReceived(txn_id, true);
//
//     return true;
// }
//
// void ObladiProxy::Abort(uint32_t txn_id) {
//     std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//
//     auto it = active_transactions_.find(txn_id);
//     if (it != active_transactions_.end()) {
//         it->second->aborted = true;
//         total_aborts_.fetch_add(1);
//         std::cout << "[Abort] Txn " << txn_id << " aborted." << std::endl;
//     }
// }
//
// void ObladiProxy::MarkOperationReceived(uint32_t txn_id, bool is_last) {
//     std::lock_guard<std::mutex> lock(txn_op_status_mutex_);
//
//     auto it = txn_op_status_.find(txn_id);
//     if (it == txn_op_status_.end()) {
//         // if the txn does not exist, clean;
//         return;
//     }
//
//     TxnOperationStatus& status = it->second;
//     status.op_count++;
//
//     if (is_last) {
//         status.all_ops_received = true;
//         std::cout << "[MarkOp] Txn " << txn_id
//                   << " received all operations (total: " << status.op_count << ")"
//                   << std::endl;
//     }
// }
//
// void ObladiProxy::CheckAndAbortIncompleteTxns() {
//     // ⭐ first op_lock，then txn_lock;
//     std::lock_guard<std::mutex> op_lock(txn_op_status_mutex_);
//     std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//
//     if (!current_epoch_) {
//         return;
//     }
//
//     std::cout << "[CheckIncomplete] Checking incomplete transactions..." << std::endl;
//
//     uint32_t aborted_count = 0;
//
//     for (uint32_t txn_id : current_epoch_->epoch_txns) {
//         auto txn_it = active_transactions_.find(txn_id);
//         if (txn_it == active_transactions_.end()) {
//             continue;
//         }
//
//         Transaction* txn = txn_it->second.get();
//         if (txn->aborted) {
//             continue;
//         }
//
//         auto op_it = txn_op_status_.find(txn_id);
//         if (op_it == txn_op_status_.end() || !op_it->second.all_ops_received) {
//             std::cerr << "[CheckIncomplete] Txn " << txn_id
//                       << " incomplete, aborting" << std::endl;
//
//             txn->aborted = true;
//             total_aborts_.fetch_add(1);
//             aborted_count++;
//         }
//     }
//
//     std::cout << "[CheckIncomplete] Aborted " << aborted_count
//               << " incomplete transactions" << std::endl;
// }
//
// std::array<double, 2> ObladiProxy::EndEpoch() {
//     const bool from_timer_thread =
//         (std::this_thread::get_id() == epoch_timer_thread_.get_id());
//
//     if (!from_timer_thread) {
//         bool expected = false;
//         if (!end_epoch_in_progress_.compare_exchange_strong(expected, true)) {
//             std::cout << "[EndEpoch] Another thread executing, returning" << std::endl;
//             return {0, 0};
//         }
//     }
//
//     struct EndEpochGuard {
//         std::atomic<bool>& flag;
//         bool should_release;
//
//         EndEpochGuard(std::atomic<bool>& f, bool release)
//             : flag(f), should_release(release) {}
//
//         ~EndEpochGuard() {
//             if (should_release) {
//                 flag.store(false);
//             }
//         }
//     };
//     EndEpochGuard guard(end_epoch_in_progress_, !from_timer_thread);
//
//     if (timer_running_.load() && !from_timer_thread) {
//         StopEpochTimer();
//     }
//
//     // ⭐ Reduce the scope of `epoch_mutex_`
//     uint32_t eid;
//     std::vector<uint32_t> epoch_txns;
//
//     {
//         std::lock_guard<std::mutex> epoch_lock(epoch_mutex_);
//
//         if (!IsEpochActive()) {
//             std::cerr << "[EndEpoch] No active epoch!" << std::endl;
//             return {0, 0};
//         }
//
//         eid = current_epoch_->epoch_id;
//         epoch_txns = current_epoch_->epoch_txns;  // 复制
//
//         std::cout << "[EndEpoch] Ending epoch " << eid
//                   << " with " << epoch_txns.size() << " transactions" << std::endl;
//     }  // ⭐ 释放 epoch_mutex_
//
//     // ⭐ Step 1: check the imcomplete txn;
//     CheckAndAbortIncompleteTxns();
//
//     // ⭐ Step 2: MVSTO invalidation;
//     std::cout << "[EndEpoch] ✅ Running MVSTO validation with all version chains present"
//               << std::endl;
//     RunMVSTOValidation(epoch_txns);
//
//     // ⭐ Step 3: construct and run batches;
//     std::array<double, 2> batch_times = BuildAndExecuteBatches(epoch_txns);
//
//     // ⭐ Step 4: GC clean the completed txn;
//     GarbageCollectEpoch(epoch_txns);
//
//     std::cout << "[EndEpoch] ✅ Starting safe eviction after validation and execution"
//               << std::endl;
//     for (uint32_t pid = 0; pid < partitions_.size(); ++pid) {
//         EvictCommittedVersions(pid);
//     }
//
//     // ⭐ Step 5: check whether it needs to do the repartitioning;
//     if (ShouldRepartition()) {
//         std::cout << "[EndEpoch] Repartitioning triggered!" << std::endl;
//         TriggerRepartitioning();
//     }
//
//     // ⭐ Step 6: start the new epoch;
//     StartNewEpoch();
//
//     std::cout << "[EndEpoch] Epoch " << eid << " ended successfully. "
//               << "Read: " << batch_times[0] << "ms, Write: " << batch_times[1] << "ms"
//               << std::endl;
//
//     return batch_times;
// }
//
// void ObladiProxy::RunMVSTOValidation(const std::vector<uint32_t>& epoch_txns) {
//     // ⭐ get txn_mutex_，and protect active_transactions_'s access;
//     std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//
//     // ============================================
//     // Step 0: abort the incomplete txn;
//     // ============================================
//     for (uint32_t tid : epoch_txns) {
//         auto it = active_transactions_.find(tid);
//         if (it == active_transactions_.end()) continue;
//
//         Transaction* txn = it->second.get();
//         if (!txn->finished && !txn->aborted) {
//             txn->aborted = true;
//             std::cout << "[MVSTO] Txn " << tid
//                       << " aborted (not finished at epoch boundary)" << std::endl;
//         }
//     }
//
//     // ============================================
//     // Step 1: collect all other txns;（finished && !aborted）
//     // ============================================
//     std::vector<Transaction*> candidates;
//     for (auto tid : epoch_txns) {
//         auto it = active_transactions_.find(tid);
//         if (it == active_transactions_.end()) continue;
//
//         Transaction* txn = it->second.get();
//         if (txn->aborted)  continue;
//         if (!txn->finished) continue;
//
//         candidates.push_back(txn);
//     }
//
//     std::cout << "[MVSTO] Validating " << candidates.size()
//               << " candidate transactions" << std::endl;
//
//     // ============================================
//     // Step 2: sort by the timestamp;
//     // ============================================
//     std::sort(candidates.begin(), candidates.end(),
//               [](Transaction* a, Transaction* b) {
//                   return a->timestamp < b->timestamp;
//               });
//
//     // ============================================
//     // Step 3: Validate each candidate transaction;
//     // ============================================
//     for (Transaction* txn : candidates) {
//         bool ok = true;
//
//         // ------------------------------------------------
//         // Rule A: check read dependence;
//         // ------------------------------------------------
//         // if txn T read txn D's written data,
//         // then D's timestamp should <= T's timestamp，and D cannot be aborted;
//         for (uint32_t dep_id : txn->read_deps) {
//             auto dep_it = active_transactions_.find(dep_id);
//             if (dep_it == active_transactions_.end()) continue;
//
//             Transaction* dep_txn = dep_it->second.get();
//
//             // 检查时间戳顺序
//             if (dep_txn->timestamp > txn->timestamp) {
//                 ok = false;
//                 std::cout << "[MVSTO] Txn " << txn->txn_id
//                           << " failed: read dep " << dep_id
//                           << " has higher timestamp" << std::endl;
//                 break;
//             }
//
//             // check whether the dependent txn aborted;
//             if (dep_txn->aborted) {
//                 ok = false;
//                 std::cout << "[MVSTO] Txn " << txn->txn_id
//                           << " failed: read dep " << dep_id
//                           << " is aborted" << std::endl;
//                 break;
//             }
//         }
//
//         // ------------------------------------------------
//         // Rule B: Write-After-Read (WAR) conflict check;
//         // ------------------------------------------------
//         // check：whether other txn. having older tmp read the old version of data I will write;
//         // if yes，it has WAR conflict，need to abort;
//         if (ok) {
//             for (const auto& kv : txn->writes) {
//                 uint32_t key = kv.first;
//
//                 // ⭐⭐⭐ get partition_mutex_ ⭐⭐⭐
//                 // since key_to_partition_ is shared data，should be accessed with lock;
//                 uint32_t pid;
//                 bool found_partition = false;
//                 {
//                     // ⭐ get partition_mutex_;
//                     std::lock_guard<std::mutex> part_lock(partition_mutex_);
//
//                     auto pid_it = key_to_partition_.find(key);
//                     if (pid_it != key_to_partition_.end()) {
//                         pid = pid_it->second;
//                         found_partition = true;
//                     }
//                 }  // ⭐ free partition_mutex_;
//
//                 // if cannot find partition or partition is invalid，skip key;
//                 if (!found_partition) {
//                     std::cout << "[MVSTO] Warning: key " << key
//                               << " not found in key_to_partition_" << std::endl;
//                     continue;
//                 }
//
//                 if (pid >= partitions_.size()) {
//                     std::cout << "[MVSTO] Warning: invalid partition " << pid
//                               << " for key " << key << std::endl;
//                     continue;
//                 }
//
//                 PartitionInfo* pinfo = partitions_[pid].get();
//                 if (!pinfo) {
//                     std::cout << "[MVSTO] Warning: null partition " << pid << std::endl;
//                     continue;
//                 }
//
//                 // ⭐ lock partition's version_cache
//                 std::lock_guard<std::mutex> cache_lock(pinfo->cache_mutex);
//
//                 auto vc_it = pinfo->version_cache.find(key);
//                 if (vc_it == pinfo->version_cache.end()) {
//                     // if the key is not in the stash, in the ORAM;
//                     continue;
//                 }
//
//                 VC* chain = vc_it->second;
//                 if (!chain) continue;
//
//                 // ⭐ lock version chain
//                 std::lock_guard<std::mutex> chain_lock(chain->chain_mutex);
//
//                 // find this txn's current version;
//                 ObjectVersion* curr = chain->head;
//                 while (curr && curr->timestamp != txn->timestamp) {
//                     curr = curr->next;
//                 }
//
//                 if (!curr) {
//                     std::cout << "[MVSTO] Warning: cannot find version for txn "
//                               << txn->txn_id << " key " << key << std::endl;
//                     continue;
//                 }
//
//                 // ⭐ WAR conflict check：
//                 // check "older" version（curr -> next）whether being read by "later" txn;
//                 // if older -> max_read_ts > txn -> timestamp，it represents：
//                 //   - it has a txn with larger timestamp（T2）read this old version;
//                 //   - current txn（T1）should write the new version;
//                 //   - it violate the serial order（T2 should see T1's written）;
//                 ObjectVersion* older = curr->next;
//                 if (older && older->max_read_ts > txn->timestamp) {
//                     ok = false;
//
//                     std::cout << "[MVSTO] WAR conflict detected:" << std::endl;
//                     std::cout << "  - Txn " << txn->txn_id
//                               << " (ts=" << txn->timestamp << ")"
//                               << " writes key " << key << std::endl;
//                     std::cout << "  - But older version was read at ts="
//                               << older->max_read_ts
//                               << " (> " << txn->timestamp << ")" << std::endl;
//                     break;
//                 }
//             }
//         }
//
//         // ================================================
//         // Step 4: according to the check result，commit or abort txn;
//         // ================================================
//         if (!ok) {
//             txn->aborted = true;
//             total_aborts_.fetch_add(1);
//             std::cout << "[MVSTO] ❌ Txn " << txn->txn_id
//                       << " ABORTED (validation failed)" << std::endl;
//         } else {
//             txn->committed = true;
//             total_commits_.fetch_add(1);
//             std::cout << "[MVSTO] ✅ Txn " << txn->txn_id
//                       << " COMMITTED" << std::endl;
//         }
//     }
//
//     std::cout << "[MVSTO] Validation complete. "
//               << "Committed: " << total_commits_.load()
//               << ", Aborted: " << total_aborts_.load() << std::endl;
// }
//
// std::array<double, 2> ObladiProxy::BuildAndExecuteBatches(const std::vector<uint32_t>& epoch_txns) {
//     if (partitions_.empty()) return {0, 0};
//
//     std::cout << "[BuildAndExecuteBatches] Start building batches for epoch "
//               << current_epoch_->epoch_id << " ..." << std::endl;
//
//     // ============================================
//     // Step 0: collect all read and write ops from committed txns;
//     // ============================================
//     std::vector<std::vector<Operat>> per_partition_reads(partitions_.size());
//     std::vector<std::vector<Operat>> per_partition_writes(partitions_.size());
//
//     {
//         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//         for (uint32_t global_tid : epoch_txns) {
//             auto it = active_transactions_.find(global_tid);
//             if (it == active_transactions_.end()) continue;
//
//             Transaction* txn = it->second.get();
//             if (!txn->committed || txn->aborted) {
//                 continue;
//             }
//
//             for (auto& kv : txn->per_partition) {
//                 uint32_t pid = kv.first;
//                 PerPartitionTxnState& pstate = kv.second;
//
//                 if (pid >= per_partition_reads.size()) continue;
//
//                 // collect READ op;
//                 for (const Operat& op : pstate.read_ops) {
//                     Operat r = op;
//                     r.txn = global_tid;
//                     per_partition_reads[pid].push_back(r);
//                 }
//
//                 // collect WRITE op;
//                 for (const Operat& op : pstate.write_ops) {
//                     Operat w = op;
//                     w.txn = global_tid;
//                     per_partition_writes[pid].push_back(w);
//                 }
//             }
//         }
//     }
//
//     // ============================================
//     // Step 1: construct READ batches;
//     // ⭐ PRIVACY FIX: try to read from version chain,
//     //    if cannot find, read from ORAM;
//     // ============================================
//     std::cout << "[BuildAndExecuteBatches] Building READ batches..." << std::endl;
//
//     // use to record in every partition which keys need to get from ORAM;
//     std::vector<std::vector<Operat>> per_partition_oram_reads(partitions_.size());
//
//     // try to read all data from version chain;
//     {
//         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//
//         for (size_t pid = 0; pid < partitions_.size(); ++pid) {
//             PartitionInfo* pinfo = partitions_[pid].get();
//             if (!pinfo) continue;
//
//             auto& reads = per_partition_reads[pid];
//
//             for (const Operat& read_op : reads) {
//                 uint32_t key = read_op.key;
//                 uint32_t txn_id = read_op.txn;
//
//                 auto txn_it = active_transactions_.find(txn_id);
//                 if (txn_it == active_transactions_.end()) continue;
//                 Transaction* txn = txn_it->second.get();
//
//                 // try to read from version chain;
//                 ObjectVersion* version = ReadFromVersionChain(pid, key, txn->timestamp);
//
//                 if (version) {
//                     // successfully read from version chain;
//                     txn->reads[key] = version->value;
//
//                     // Track write-read dependency;
//                     if (version->txn_id != txn_id && version->txn_id != SYSTEM_TXN_ID) {
//                         txn->read_deps.push_back(version->txn_id);
//                     }
//
//                     // Update read marker;
//                     UpdateReadMarker(pid, key, txn->timestamp);
//
//                     std::cout << "[BuildAndExecuteBatches] Read key " << key
//                               << " from version chain for txn " << txn_id << std::endl;
//                 } else {
//                     // need to get from ORAM;
//                     per_partition_oram_reads[pid].push_back(read_op);
//                     std::cout << "[BuildAndExecuteBatches] Key " << key
//                               << " not in version chain, will read from ORAM" << std::endl;
//                 }
//             }
//         }
//     }
//
//     // for all keys that need to read from ORAM, construct batches;
//     for (size_t pid = 0; pid < partitions_.size(); ++pid) {
//         PartitionInfo* pinfo = partitions_[pid].get();
//         if (!pinfo) continue;
//
//         auto& oram_reads = per_partition_oram_reads[pid];
//         if (oram_reads.empty()) {
//             pinfo -> current_read_batches.clear();
//             pinfo -> num_read_batches = 0;
//             continue;
//         }
//
//         uint32_t batch_size = pinfo -> batch_size;
//         if (batch_size == 0) batch_size = 1;
//
//         pinfo -> current_read_batches.clear();
//         uint32_t batch_id = 0;
//
//         size_t idx = 0;
//         while (idx < oram_reads.size()) {
//             size_t end = std::min(idx + static_cast<size_t>(batch_size), oram_reads.size());
//
//             auto batch = std::make_unique<Bat>(
//                 batch_id++,
//                 current_epoch_->epoch_id,
//                 static_cast<uint32_t>(pid),
//                 /*is_read=*/true,
//                 batch_size
//             );
//
//             for (size_t i = idx; i < end; ++i) {
//                 const auto& r = oram_reads[i];
//                 BatchRequest req(r.txn, r.key, OpType::READ, "");
//                 batch->requests.push_back(std::move(req));
//             }
//
//             while (batch->requests.size() < batch_size) {
//                 BatchRequest dummy;
//                 dummy.is_dummy = true;
//                 dummy.op_type = OpType::READ;
//                 batch->requests.push_back(dummy);
//             }
//
//             std::cout << "  [BuildAndExecuteBatches] Partition " << pid
//                       << " created READ batch " << batch->batch_id
//                       << " with " << batch->requests.size() << " requests" << std::endl;
//
//             pinfo->current_read_batches.push_back(std::move(batch));
//             idx = end;
//         }
//
//         pinfo->num_read_batches = static_cast<uint32_t>(pinfo->current_read_batches.size());
//     }
//
//     // ============================================
//     // Step 2: construct WRITE batches;
//     // ============================================
//     std::cout << "[BuildAndExecuteBatches] Building WRITE batches..." << std::endl;
//
//     for (size_t pid = 0; pid < partitions_.size(); ++pid) {
//         PartitionInfo* pinfo = partitions_[pid].get();
//         if (!pinfo) continue;
//
//         auto& writes = per_partition_writes[pid];
//         if (writes.empty()) {
//             continue;
//         }
//
//         uint32_t batch_size = pinfo->batch_size;
//         if (batch_size == 0) batch_size = 1;
//
//         pinfo->current_write_batches.clear();
//         uint32_t batch_id = 0;
//
//         size_t idx = 0;
//         while (idx < writes.size()) {
//             size_t end = std::min(idx + static_cast<size_t>(batch_size), writes.size());
//
//             auto batch = std::make_unique<Bat>(
//                 batch_id++,
//                 current_epoch_->epoch_id,
//                 static_cast<uint32_t>(pid),
//                 /*is_read=*/false,
//                 batch_size
//             );
//
//             for (size_t i = idx; i < end; ++i) {
//                 const auto& w = writes[i];
//                 BatchRequest req(w.txn, w.key, w.type, w.value);
//                 batch->requests.push_back(std::move(req));
//             }
//
//             while (batch->requests.size() < batch_size) {
//                 BatchRequest dummy;
//                 dummy.is_dummy = true;
//                 dummy.op_type = OpType::UPDATE;
//                 batch->requests.push_back(dummy);
//             }
//
//             std::cout << "  [BuildAndExecuteBatches] Partition " << pid
//                       << " created WRITE batch " << batch->batch_id
//                       << " with " << batch->requests.size() << " requests" << std::endl;
//
//             pinfo->current_write_batches.push_back(std::move(batch));
//             idx = end;
//         }
//
//         pinfo->num_write_batches = static_cast<uint32_t>(pinfo->current_write_batches.size());
//     }
//
//     // ============================================
//     // Step 3: ⭐ execute all READ batches（in parallel）;
//     // ⭐ TIMING FIX: record every partition's execution time，find the max one;
//     // ============================================
//     std::cout << "[BuildAndExecuteBatches] Executing READ batches in parallel..." << std::endl;
//
//     // ⭐ record time for every partition;
//     std::vector<uint64_t> per_partition_read_times(partitions_.size(), 0);
//     std::vector<std::thread> read_workers;
//
//     for (uint32_t pid = 0; pid < partitions_.size(); ++pid) {
//         PartitionInfo* pinfo = partitions_[pid].get();
//         if (!pinfo) continue;
//         if (pinfo->current_read_batches.empty()) continue;
//
//         // start a new thread for every partition;
//         read_workers.emplace_back([this, pid, &per_partition_read_times]() {
//             PartitionInfo* local_pinfo = partitions_[pid].get();
//
//             // ⭐ timer start：calculate the time finishing all READ batches in the partition;
//             auto partition_start = std::chrono::high_resolution_clock::now();
//
//             std::atomic<uint64_t> total_oram_time_us{0};
//             for (auto& batch_ptr : local_pinfo->current_read_batches) {
//                 Bat* batch = batch_ptr.get();
//                 if (!batch) continue;
//
//                 // execute this batch（use ExecuteReadBatch，with no atomic）;
//                 ExecuteReadBatch(pid, batch, total_oram_time_us);
//             }
//
//             // ⭐ timer ends;
//             auto partition_end = std::chrono::high_resolution_clock::now();
//             auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
//                 partition_end - partition_start).count();
//
//             per_partition_read_times[pid] = duration_us;
//
//             std::cout << "[BuildAndExecuteBatches] Partition " << pid
//                       << " READ batches completed in " << duration_us << " us" << std::endl;
//         });
//     }
//
//     // wait until all READ threads complete;
//     for (auto& th : read_workers) {
//         if (th.joinable()) {
//             th.join();
//         }
//     }
//
//     // ⭐ find the longest time that partition finishes;
//     uint64_t max_read_time_us = 0;
//     for (size_t pid = 0; pid < per_partition_read_times.size(); ++pid) {
//         if (per_partition_read_times[pid] > max_read_time_us) {
//             max_read_time_us = per_partition_read_times[pid];
//         }
//     }
//     double read_duration_ms = max_read_time_us / 1000;
//
//     std::cout << "[BuildAndExecuteBatches] All READ batches completed." << std::endl;
//     std::cout << "[BuildAndExecuteBatches] ⭐ MAX READ time across all partitions: "
//               << read_duration_ms << " ms (parallel execution)" << std::endl;
//
//     // ============================================
//     // Step 4: ⭐ execute all WRITE batches（in parallel）;
//     // ⭐ TIMING FIX: record every partition's execution time，get max;
//     // ============================================
//     std::cout << "[BuildAndExecuteBatches] Executing WRITE batches in parallel..." << std::endl;
//
//     // ⭐ record time for every partition;
//     std::vector<uint64_t> per_partition_write_times(partitions_.size(), 0);
//     std::vector<std::thread> write_workers;
//
//     for (uint32_t pid = 0; pid < partitions_.size(); ++pid) {
//         PartitionInfo* pinfo = partitions_[pid].get();
//         if (!pinfo) continue;
//         if (pinfo->current_write_batches.empty()) continue;
//
//         // start a new thread for every partition;
//         write_workers.emplace_back([this, pid, &per_partition_write_times]() {
//             // 1) dedup + padding;
//             DeduplicateAndBatch(pid);
//
//             // ⭐ timer starts：calculate the time finishing all write batches in this partition;
//             auto partition_start = std::chrono::high_resolution_clock::now();
//
//             std::atomic<uint64_t> total_oram_time_us{0};
//
//             // 2) execute all write batches;
//             PartitionInfo* local_pinfo = partitions_[pid].get();
//             for (auto& batch_ptr : local_pinfo->current_write_batches) {
//                 Bat* batch = batch_ptr.get();
//                 if (!batch) continue;
//
//                 // execute this batch（use ExecuteWriteBatch，with no atomic）;
//                 ExecuteWriteBatch(pid, batch, total_oram_time_us);
//             }
//
//             // ⭐ timer ends;
//             auto partition_end = std::chrono::high_resolution_clock::now();
//             auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
//                 partition_end - partition_start).count();
//
//             per_partition_write_times[pid] = duration_us;
//
//             std::cout << "[BuildAndExecuteBatches] Partition " << pid
//                       << " WRITE batches completed in " << duration_us << " us" << std::endl;
//         });
//     }
//
//     // wait until all WRITE finishes;
//     for (auto& th : write_workers) {
//         if (th.joinable()) {
//             th.join();
//         }
//     }
//
//     // ⭐ use the longest time that partition finishes;
//     uint64_t max_write_time_us = 0;
//     for (size_t pid = 0; pid < per_partition_write_times.size(); ++pid) {
//         if (per_partition_write_times[pid] > max_write_time_us) {
//             max_write_time_us = per_partition_write_times[pid];
//         }
//     }
//     double write_duration_ms = max_write_time_us / 1000;
//
//     std::cout << "[BuildAndExecuteBatches] All WRITE batches completed." << std::endl;
//     std::cout << "[BuildAndExecuteBatches] ⭐ MAX WRITE time across all partitions: "
//               << write_duration_ms << " ms (parallel execution)" << std::endl;
//
//     std::cout << "[BuildAndExecuteBatches] Done." << std::endl;
//
//     return {read_duration_ms, write_duration_ms};
// }
//
// // =======================================================================================
// // ⭐ helper funcs：ExecuteReadBatchSimple and ExecuteWriteBatchSimple
// // =======================================================================================
// void ObladiProxy::GarbageCollectEpoch(const std::vector<uint32_t>& epoch_txns) {
//     std::cout << "[GC] Starting garbage collection for epoch "
//               << current_epoch_->epoch_id << std::endl;
//
//     // ----------------------------------------
//     // Step 1: delete this epoch's txn's metadata;
//     // ----------------------------------------
//     {
//         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//
//         for (uint32_t tid : epoch_txns) {
//             auto it = active_transactions_.find(tid);
//             if (it != active_transactions_.end()) {
//                 active_transactions_.erase(it);
//             }
//         }
//     }
//
//     // ----------------------------------------
//     // Step 2: clean all partitions' version_cache
//     // ----------------------------------------
//     for (auto& pinfo_uptr : partitions_) {
//         PartitionInfo* pinfo = pinfo_uptr.get();
//         if (!pinfo) continue;
//
//         std::lock_guard<std::mutex> cache_lock(pinfo->cache_mutex);
//
//         for (auto& kv : pinfo->version_cache) {
//             VC* chain = kv.second;
//             if (!chain) continue;
//
//             std::lock_guard<std::mutex> chain_lock(chain->chain_mutex);
//
//             ObjectVersion* v = chain->head;
//             while (v) {
//                 ObjectVersion* next = v->next;
//                 delete v;
//                 v = next;
//             }
//             chain->head = nullptr;
//
//             delete chain;
//         }
//
//         pinfo->version_cache.clear();
//     }
//
//     std::cout << "[GC] Finished garbage collection for epoch "
//               << current_epoch_->epoch_id << std::endl;
// }
//
// // ============================================
// // Phase 2: Version Chain Management
// // ============================================
// ObjectVersion* ObladiProxy::ReadFromVersionChain(uint32_t partition_id,
//                                                 uint32_t key,
//                                                 uint32_t read_ts) {
//     if (partition_id >= partitions_.size()) {
//         return nullptr;
//     }
//
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo) {
//         return nullptr;
//     }
//
//     std::lock_guard<std::mutex> cache_lock(pinfo->cache_mutex);
//
//     auto it = pinfo -> version_cache.find(key);
//     if (it == pinfo -> version_cache.end()) {
//         // ⭐ Stash miss
//         std::cout << "[VersionChain] STASH MISS for key " << key
//                   << " (partition " << partition_id << ")" << std::endl;
//         return nullptr;
//     }
//
//     VC* chain = it -> second;
//     std::lock_guard<std::mutex> chain_lock(chain->chain_mutex);
//
//     ObjectVersion* v = chain -> head;
//     while (v && v -> timestamp > read_ts) {
//         v = v->next;
//     }
//
//     return v;
// }
//
// void ObladiProxy::AddToVersionChain(uint32_t partition_id,
//                                     uint32_t key,
//                                     uint32_t txn_id,
//                                     const std::string& value,
//                                     uint32_t timestamp) {
//     if (partition_id >= partitions_.size()) return;
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo) return;
//     // ⭐ add the new version;
//     {
//         std::lock_guard<std::mutex> cache_lock(pinfo -> cache_mutex);
//
//         VC* chain = nullptr;
//         auto it = pinfo -> version_cache.find(key);
//         if (it == pinfo -> version_cache.end()) {
//             chain = new VC(key);
//             pinfo -> version_cache[key] = chain;
//         } else {
//             chain = it -> second;
//         }
//
//         std::lock_guard<std::mutex> chain_lock(chain -> chain_mutex);
//
//         ObjectVersion* new_version = new ObjectVersion(txn_id, value, timestamp);
//         new_version -> next = chain -> head;
//         chain -> head = new_version;
//     }
//
//     std::cout << "[VersionChain] Added version for key " << key << std::endl;
// }
//
// void ObladiProxy::WriteBackToORAM(uint32_t partition_id, uint32_t key, VC* chain) {
//     if (partition_id >= partitions_.size()) return;
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo || !chain) return;
//
//     // ✅ find the newest version;
//     ObjectVersion* committed_version = nullptr;
//
//     {
//         std::lock_guard<std::mutex> chain_lock(chain->chain_mutex);
//         ObjectVersion* v = chain->head;
//
//         // traverse all version chains, find the first submitted version;
//         while (v) {
//             // 检查这个版本是否属于系统操作（SYSTEM_TXN_ID）
//             if (v->txn_id == SYSTEM_TXN_ID) {
//                 committed_version = v;
//                 break;
//             }
//
//             // check whether the txn is submitted;
//             std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//             auto txn_it = active_transactions_.find(v->txn_id);
//
//             if (txn_it != active_transactions_.end()) {
//                 Transaction* txn = txn_it->second.get();
//                 // ✅ only finished && !aborted version can be written back;
//                 if (txn->finished && !txn->aborted) {
//                     committed_version = v;
//                     break;
//                 }
//             } else {
//             }
//
//             v = v->next;
//         }
//     }
//
//     // ✅ only write the committed version;
//     if (committed_version) {
//         try {
//             pinfo->oram->access(key, OpType::WRITE, committed_version->value);
//             std::cout << "[WriteBack] ✅ Key " << key
//                       << " written back to ORAM (txn=" << committed_version->txn_id
//                       << ", committed version)" << std::endl;
//         } catch (const std::exception& e) {
//             std::cerr << "[WriteBack] ❌ Failed to write key " << key
//                       << ": " << e.what() << std::endl;
//         }
//     } else {
//         std::cout << "[WriteBack] ⚠️ Key " << key
//                   << " has no committed version, skipping write-back" << std::endl;
//     }
// }
//
// void ObladiProxy::EvictCommittedVersions(uint32_t partition_id) {
//     if (partition_id >= partitions_.size()) return;
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo) return;
//
//     std::cout << "[SafeEvict] Starting safe eviction for partition " << partition_id
//               << " (stash size: " << pinfo->version_cache.size()
//               << ", capacity: " << pinfo->stash_capacity << ")" << std::endl;
//
//     // ✅ only if the stash reaches the capacity, evict data;
//     while (pinfo->version_cache.size() > pinfo->stash_capacity) {
//         uint32_t evict_key = 0;
//         VC* chain_to_evict = nullptr;
//         bool found_safe_key = false;
//
//         // Step 1: find the safe key and do the eviction;
//         {
//             std::lock_guard<std::mutex> cache_lock(pinfo->cache_mutex);
//
//             if (pinfo->version_cache.empty()) break;
//
//             // traverse all keys, and find the committed key that can be evicted;
//             for (auto& [key, chain] : pinfo->version_cache) {
//                 std::lock_guard<std::mutex> chain_lock(chain->chain_mutex);
//
//                 bool all_committed = true;
//                 ObjectVersion* v = chain->head;
//
//                 // check whether all versions of this key being committed;
//                 while (v) {
//                     if (v->txn_id != SYSTEM_TXN_ID) {
//                         std::lock_guard<std::mutex> txn_lock(txn_mutex_);
//                         auto txn_it = active_transactions_.find(v->txn_id);
//
//                         if (txn_it != active_transactions_.end()) {
//                             Transaction* txn = txn_it->second.get();
//                             // if this txn is unfinished or aborted, cannot do the eviction;
//                             if (!txn->finished || txn->aborted) {
//                                 all_committed = false;
//                                 break;
//                             }
//                         }
//                     }
//                     v = v->next;
//                 }
//
//                 // ✅ find a safe key;
//                 if (all_committed) {
//                     evict_key = key;
//                     chain_to_evict = chain;
//                     pinfo->version_cache.erase(key);
//                     found_safe_key = true;
//                     std::cout << "[SafeEvict] Selected key " << key
//                               << " for eviction (all versions committed)" << std::endl;
//                     break;
//                 }
//             }
//
//             // if cannot find the safe key, use the random eviction as fallback;
//             if (!found_safe_key) {
//                 std::cout << "[SafeEvict] No fully committed key to evict; stop eviction to preserve correctness" << std::endl;
//                 break;
//             }
//         }  // ⭐ free the cache_lock;
//
//         // Step 2: write back to the ORAM (use the new version of WriteBackToORAM);
//         if (chain_to_evict) {
//             WriteBackToORAM(partition_id, evict_key, chain_to_evict);
//
//             // Step 3: clean the version chain;
//             {
//                 std::lock_guard<std::mutex> chain_lock(chain_to_evict->chain_mutex);
//                 ObjectVersion* v = chain_to_evict->head;
//                 while (v) {
//                     ObjectVersion* next = v->next;
//                     delete v;
//                     v = next;
//                 }
//             }
//             delete chain_to_evict;
//         }
//     }
//
//     std::cout << "[SafeEvict] Eviction complete for partition " << partition_id
//               << " (final stash size: " << pinfo->version_cache.size() << ")" << std::endl;
// }
//
// void ObladiProxy::UpdateReadMarker(uint32_t partition_id,
//                                    uint32_t key,
//                                    uint32_t read_ts) {
//     if (partition_id >= partitions_.size()) return;
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo) return;
//
//     std::lock_guard<std::mutex> cache_lock(pinfo -> cache_mutex);
//
//     auto it = pinfo -> version_cache.find(key);
//     if (it == pinfo -> version_cache.end()) return;
//
//     VC* chain = it -> second;
//     std::lock_guard<std::mutex> chain_lock(chain -> chain_mutex);
//
//     ObjectVersion* v = chain -> head;
//     while (v && v -> timestamp > read_ts) {
//         v = v -> next;
//     }
//     if (!v) return;
//
//     if (read_ts > v -> max_read_ts) {
//         v -> max_read_ts = read_ts;
//     }
// }
//
// // ============================================
// // Phase 3: Batching and Deduplication
// // ============================================
// void ObladiProxy::DeduplicateAndBatch(uint32_t partition_id) {
//     if (partition_id >= partitions_.size()) {
//         std::cerr << "[Dedup] ERROR: invalid partition_id "
//                   << partition_id << " (max: " << partitions_.size() - 1 << ")"
//                   << std::endl;
//         return;
//     }
//
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo) {
//         std::cerr << "[Dedup] ERROR: null partition info for partition "
//                   << partition_id << std::endl;
//         return;
//     }
//
//     std::cout << "[Dedup] Deduplicating batches for partition "
//               << partition_id << std::endl;
//
//     // =====================================================
//     // 1) Read batches: dedup + record the data source;
//     // =====================================================
//     std::cout << "[Dedup] Processing " << pinfo->current_read_batches.size()
//               << " read batches..." << std::endl;
//
//     for (auto& batch_ptr : pinfo->current_read_batches) {
//         Bat* batch = batch_ptr.get();
//         if (!batch) {
//             std::cerr << "[Dedup] WARNING: null read batch pointer!" << std::endl;
//             continue;
//         }
//
//         std::unordered_map<uint32_t, size_t> key_to_first_idx;
//         std::vector<BatchRequest> deduplicated;
//         deduplicated.reserve(batch->requests.size());
//
//         for (size_t i = 0; i < batch->requests.size(); ++i) {
//             const auto& req = batch->requests[i];
//
//             // dummy request keep the same;
//             if (req.is_dummy) {
//                 deduplicated.push_back(req);
//                 continue;
//             }
//
//             // check whether it has this key;
//             auto it = key_to_first_idx.find(req.key);
//             if (it != key_to_first_idx.end()) {
//                 // dedup key：mark the dummy，and check the data recourse;
//                 BatchRequest dummy_req = req;
//                 dummy_req.is_dummy = true;
//                 dummy_req.source_index = it->second;
//                 deduplicated.push_back(dummy_req);
//
//                 std::cout << "[Dedup] Read batch " << batch->batch_id
//                           << ": duplicate key " << req.key
//                           << " at index " << deduplicated.size() - 1
//                           << " -> source index " << it->second << std::endl;
//             } else {
//                 // first happen：remain and record the recourse;
//                 key_to_first_idx[req.key] = deduplicated.size();
//                 deduplicated.push_back(req);
//             }
//         }
//
//         batch->requests = std::move(deduplicated);
//         PadBatch(batch);
//
//         std::cout << "[Dedup] Read batch " << batch->batch_id
//                   << " now has " << batch->requests.size()
//                   << " requests (after read-dedup + padding)"
//                   << std::endl;
//     }
//
//     // =====================================================
//     // 2) Write batches: in the same batch，same key only keeps the last write op;
//     // =====================================================
//     std::cout << "[Dedup] Processing " << pinfo->current_write_batches.size()
//               << " write batches..." << std::endl;
//
//     for (auto& batch_ptr : pinfo->current_write_batches) {
//         Bat* wbatch = batch_ptr.get();
//         if (!wbatch) {
//             std::cerr << "[Dedup] WARNING: null write batch pointer!" << std::endl;
//             continue;
//         }
//
//         std::unordered_map<uint32_t, size_t> key_to_last_idx;
//         std::vector<BatchRequest> deduplicated;
//         deduplicated.reserve(wbatch->requests.size());
//
//         for (size_t i = 0; i < wbatch->requests.size(); ++i) {
//             const auto& req = wbatch->requests[i];
//
//             // dummy request keeps the same;
//             if (req.is_dummy) {
//                 deduplicated.push_back(req);
//                 continue;
//             }
//
//             // check whether we have this key;
//             auto it = key_to_last_idx.find(req.key);
//             if (it != key_to_last_idx.end()) {
//                 // if we find the repeated key，mark dummy;
//                 deduplicated[it -> second].is_dummy = true;
//             }
//
//             // record current key's position;
//             key_to_last_idx[req.key] = deduplicated.size();
//             deduplicated.push_back(req);
//         }
//
//         // replace to the dedup requests;
//         wbatch->requests = std::move(deduplicated);
//
//         // Padding to the fixed size;
//         PadBatch(wbatch);
//
//         std::cout << "[Dedup] Write batch " << wbatch->batch_id
//                   << " now has " << wbatch->requests.size()
//                   << " requests (after write-dedup + padding)"
//                   << std::endl;
//     }
//
//     std::cout << "[Dedup] Deduplication complete for partition "
//               << partition_id << std::endl;
// }
//
// void ObladiProxy::PadBatch(Bat* batch) {
//     if (!batch) {
//         std::cerr << "[PadBatch] ERROR: null batch pointer!" << std::endl;
//         return;
//     }
//
//     if (batch->partition_id >= partitions_.size()) {
//         std::cerr << "[PadBatch] ERROR: invalid partition_id "
//                   << batch->partition_id << " (max: " << partitions_.size() - 1 << ")"
//                   << std::endl;
//         return;
//     }
//
//     PartitionInfo* pinfo = partitions_[batch->partition_id].get();
//     if (!pinfo) {
//         std::cerr << "[PadBatch] ERROR: null partition info for partition "
//                   << batch->partition_id << std::endl;
//         return;
//     }
//
//     // get the target size;
//     uint32_t target_size = pinfo->batch_size;
//
//     // if it reaches the fixed size，don't need to do the padding;
//     if (batch->requests.size() >= target_size) {
//         std::cout << "[Pad] Batch already at target size: "
//                   << batch->requests.size() << " >= " << target_size << std::endl;
//         return;
//     }
//
//     // fill in dummy requests;
//     while (batch->requests.size() < target_size) {
//         BatchRequest dummy;
//         dummy.is_dummy = true;
//         dummy.source_index = -1;
//         dummy.key = static_cast<uint32_t>(-1);  // Dummy indicator
//         dummy.op_type = batch->is_read_batch ? OpType::READ : OpType::UPDATE;
//         batch->requests.push_back(dummy);
//     }
//
//     std::cout << "[Pad] Padded batch to " << batch->requests.size()
//               << " requests (target: " << target_size << ")" << std::endl;
// }
//
// // ============================================
// // Phase 4: ORAM Execution
// // ============================================
// void ObladiProxy::ExecuteReadBatch(uint32_t partition_id, Bat* batch,
//                                    std::atomic<uint64_t>& total_oram_time_us) {
//     if (partition_id >= partitions_.size()) {
//         return;
//     }
//
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     std::cout << "[ExecRead] Executing read batch " << batch->batch_id
//               << " for partition " << partition_id << std::endl;
//
//     std::vector<std::string> results;
//     results.reserve(batch->requests.size());
//
//     // ⭐ time every request independently;
//     for (const auto& req : batch->requests) {
//         std::string value;
//
//         // timer starts;
//         auto start = std::chrono::high_resolution_clock::now();
//
//         if (req.is_dummy) {
//             // Execute dummy read
//             pinfo->oram->access(static_cast<uint32_t>(-1), OpType::READ, "");
//             value = "";
//         } else {
//             // Execute real read
//             value = pinfo->oram->access(req.key, OpType::READ, "");
//         }
//
//         // timer ends;
//         auto end = std::chrono::high_resolution_clock::now();
//         auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
//             end - start).count();
//
//         total_oram_time_us.fetch_add(duration_us);
//
//         results.push_back(value);
//     }
//
//     ProcessBatchResults(partition_id, batch, results);
//
//     std::cout << "[ExecRead] Completed read batch " << batch->batch_id << std::endl;
// }
//
// void ObladiProxy::ExecuteWriteBatch(uint32_t partition_id, Bat* bat,
//                                     std::atomic<uint64_t>& total_oram_time_us) {
//     if (partition_id >= partitions_.size()) {
//         std::cerr << "[ExecWrite] Invalid partition " << partition_id << std::endl;
//         return;
//     }
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo) {
//         std::cerr << "[ExecWrite] Null PartitionInfo for partition "
//                   << partition_id << std::endl;
//         return;
//     }
//
//     std::cout << "[ExecWrite] Executing write batch for partition "
//               << partition_id << std::endl;
//
//     // ⭐ time every request independently;
//     for (const auto& req : bat->requests) {
//         // timer starts;
//         auto start = std::chrono::high_resolution_clock::now();
//
//         if (req.is_dummy) {
//             // Dummy write
//             pinfo->oram->access(static_cast<uint32_t>(-1), OpType::WRITE, "");
//         } else {
//             // normal write：INSERT or WRITE;
//             OpType op_type = req.op_type;
//             if (op_type != OpType::INSERT && op_type != OpType::WRITE) {
//                 op_type = OpType::WRITE;
//             }
//             pinfo->oram->access(req.key, op_type, req.data);
//         }
//
//         // timer ends;
//         auto end = std::chrono::high_resolution_clock::now();
//         auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
//             end - start).count();
//         total_oram_time_us.fetch_add(duration_us);
//     }
//
//     std::cout << "[ExecWrite] Completed write batch" << std::endl;
// }
//
// void ObladiProxy::ProcessBatchResults(uint32_t partition_id,
//                                       Bat* batch,
//                                       const std::vector<std::string>& results) {
//     // Validate inputs
//     if (partition_id >= partitions_.size()) {
//         std::cerr << "[ProcessBatchResults] ERROR: Invalid partition_id "
//                   << partition_id << std::endl;
//         return;
//     }
//
//     PartitionInfo* pinfo = partitions_[partition_id].get();
//     if (!pinfo) {
//         std::cerr << "[ProcessBatchResults] ERROR: Null partition info" << std::endl;
//         return;
//     }
//
//     if (!batch) {
//         std::cerr << "[ProcessBatchResults] ERROR: Null batch pointer" << std::endl;
//         return;
//     }
//
//     std::cout << "[ProcessBatchResults] Processing " << results.size()
//               << " results for partition " << partition_id << std::endl;
//
//     // ============================================
//     // ✅ Step 1: 处理所有真实的读请求，存储结果
//     // ============================================
//     // 使用 map 记录每个索引位置读到的值
//     // key: batch 中的索引位置
//     // value: 从 ORAM 读到的数据
//     std::unordered_map<size_t, std::string> index_to_value;
//
//     for (size_t i = 0; i < batch->requests.size() && i < results.size(); ++i) {
//         const auto& req = batch->requests[i];
//
//         // 只处理非 dummy 的真实读请求
//         if (!req.is_dummy && !results[i].empty()) {
//             // 添加到 version chain（供事务后续读取）
//             AddToVersionChain(partition_id, req.key, SYSTEM_TXN_ID,
//                             results[i], SYSTEM_TS);
//
//             // ⭐ 记录这个索引位置的值（用于后续复制给去重的请求）
//             index_to_value[i] = results[i];
//
//             std::cout << "[ProcessBatchResults] Real read at index " << i
//                       << ": key=" << req.key
//                       << ", value_size=" << results[i].size() << " bytes"
//                       << std::endl;
//         }
//     }
//
//     // ============================================
//     // ✅ Step 2: 处理去重的请求（is_dummy=true 且 source_index >= 0）
//     // ============================================
//     size_t num_copies = 0;
//
//     for (size_t i = 0; i < batch->requests.size(); ++i) {
//         const auto& req = batch->requests[i];
//
//         // 识别去重的请求:
//         // 1. is_dummy = true (被标记为 dummy)
//         // 2. source_index >= 0 (有数据来源)
//         if (req.is_dummy && req.source_index >= 0) {
//             size_t source_idx = static_cast<size_t>(req.source_index);
//
//             // 验证 source_index 的有效性
//             if (source_idx >= batch->requests.size()) {
//                 std::cerr << "[ProcessBatchResults] ERROR: Invalid source_index "
//                           << source_idx << " (max: " << batch->requests.size() - 1 << ")"
//                           << " at index " << i << std::endl;
//                 continue;
//             }
//
//             // 从 source_index 位置获取数据
//             auto it = index_to_value.find(source_idx);
//             if (it != index_to_value.end()) {
//                 // ⭐ 找到数据，复制到 version chain
//                 const std::string& copied_value = it->second;
//
//                 AddToVersionChain(partition_id, req.key, SYSTEM_TXN_ID,
//                                 copied_value, SYSTEM_TS);
//
//                 num_copies++;
//
//                 std::cout << "[ProcessBatchResults] ✅ Copied read at index " << i
//                           << " from source index " << source_idx
//                           << ": key=" << req.key
//                           << ", value_size=" << copied_value.size() << " bytes"
//                           << std::endl;
//             } else {
//                 // ❌ 数据来源不存在（这不应该发生）
//                 std::cerr << "[ProcessBatchResults] ERROR: source_index " << source_idx
//                           << " not found in index_to_value map!"
//                           << " Request at index " << i
//                           << " (key=" << req.key << ") cannot get data"
//                           << std::endl;
//
//                 // Debug: 打印 index_to_value 的内容
//                 std::cerr << "  Available indices in index_to_value: ";
//                 for (const auto& kv : index_to_value) {
//                     std::cerr << kv.first << " ";
//                 }
//                 std::cerr << std::endl;
//             }
//         }
//     }
//
//     std::cout << "[ProcessBatchResults] Completed: "
//               << index_to_value.size() << " real reads, "
//               << num_copies << " deduplicated copies"
//               << std::endl;
// }
//
// // ============================================
// // Concurrency Control (MVTSO)
// // ============================================
// uint32_t ObladiProxy::AssignTimestamp() {
//     return global_timestamp_counter_.fetch_add(1);
// }
//
// // ============================================
// // Epoch Management
// // ============================================
// void ObladiProxy::StartNewEpoch() {
//     std::lock_guard<std::mutex> lock(epoch_mutex_);
//
//     std::cout << "[StartNewEpoch] Starting epoch " << epoch_counter_ << std::endl;
//
//     current_epoch_ = std::make_unique<Epoch>(epoch_counter_++);
//     current_epoch_->is_active.store(true);
//
//     // reset the epoch's status;
//     {
//         std::lock_guard<std::mutex> state_lock(epoch_state_mutex_);
//         epoch_state_ = EpochState::ACCEPTING_TXN;
//     }
//
//     // reset the time;
//     epoch_start_time_ = std::chrono::steady_clock::now();
//
//     // restart the timer;
//     if (fixed_time_interval_ms_ > 0) {
//         StartEpochTimer();
//     }
//
//     std::cout << "[StartNewEpoch] Epoch " << (epoch_counter_ - 1)
//               << " started at "
//               << std::chrono::duration_cast<std::chrono::milliseconds>(
//                   epoch_start_time_.time_since_epoch()).count()
//               << " ms" << std::endl;
// }
// // ============================================
// // Trigger Repartitioning Process
// // ============================================
// bool ObladiProxy::ShouldRepartition() const {
//     return insert_count_.load() >= repartition_threshold_;
// }
//
// void ObladiProxy::TriggerRepartitioning() {
//     std::cout << "[Repartition] Starting repartitioning process..." << std::endl;
//     std::cout << "[Repartition] Current insert count: " << insert_count_.load() << std::endl;
//     std::cout << "[Repartition] Threshold: " << repartition_threshold_ << std::endl;
//
//     try {
//         // Step 1: Collect all data
//         std::vector<uint32_t> all_keys;
//         std::vector<std::string> all_values;
//         CollectAllData(all_keys, all_values);
//
//         if (all_keys.empty()) {
//             std::cerr << "[Repartition] WARNING: No data collected, aborting" << std::endl;
//             return;
//         }
//
//         // ✅ Validate data consistency
//         if (all_keys.size() != all_values.size()) {
//             throw std::runtime_error("[Repartition] Keys/values size mismatch");
//         }
//
//         std::cout << "[Repartition] Collected " << all_keys.size() << " objects" << std::endl;
//
//         // Step 2: Rebuild partitions with collected data
//         // RebuildPartitions will call Repartition_Main internally
//         RebuildPartitions(all_keys, all_values);
//
//         // Step 3: Reset insert counter
//         insert_count_.store(0);
//
//         std::cout << "[Repartition] Repartitioning complete!" << std::endl;
//
//     } catch (const std::exception& e) {
//         std::cerr << "[Repartition][ERROR] Repartitioning failed: "
//                   << e.what() << std::endl;
//         // Don't rethrow - system should continue operating
//     }
// }
//
// // ============================================
// // Collect All Data from All Partitions
// // ============================================
// void ObladiProxy::CollectAllData(std::vector<uint32_t>& keys,
//                                  std::vector<std::string>& values) {
//     keys.clear();
//     values.clear();
//
//     // ✅ Reserve with safety check
//     size_t estimated_size = key_to_partition_.size();
//     if (estimated_size > 0 && estimated_size < 1000000000) {  // Sanity check
//         keys.reserve(estimated_size);
//         values.reserve(estimated_size);
//     }
//
//     std::cout << "[CollectData] Collecting data from " << partitions_.size()
//               << " partitions" << std::endl;
//
//     // ✅ Bounds check for partitions
//     if (partitions_.size() > 1000000) {
//         throw std::overflow_error("[CollectData] Unreasonable number of partitions: " +
//                                  std::to_string(partitions_.size()));
//     }
//
//     for (size_t pid = 0; pid < partitions_.size(); ++pid) {
//         PartitionInfo* pinfo = partitions_[pid].get();
//         if (!pinfo || !pinfo->oram) {
//             std::cerr << "[CollectData] WARNING: null partition " << pid << std::endl;
//             continue;
//         }
//
//         std::cout << "[CollectData] Collecting from partition " << pid << std::endl;
//
//         // Collect keys from this partition
//         std::vector<uint32_t> partition_keys;
//         for (const auto& kv : key_to_partition_) {
//             if (kv.second == static_cast<uint32_t>(pid)) {
//                 partition_keys.push_back(kv.first);
//             }
//         }
//
//         // Read each key from ORAM
//         for (uint32_t key : partition_keys) {
//             try {
//                 std::string value = pinfo->oram->access(key, OpType::READ, "");
//                 keys.push_back(key);
//                 values.push_back(value);
//             } catch (const std::exception& e) {
//                 std::cerr << "[CollectData] Failed to read key " << key
//                           << " from partition " << pid << ": " << e.what() << std::endl;
//             }
//         }
//     }
//
//     std::cout << "[CollectData] Collected " << keys.size()
//               << " objects total" << std::endl;
// }
//
// // ============================================
// // Rebuild Partitions with New Assignment
// // ============================================
// // keys / values 是 CollectAllData() 得到的全局快照
// void ObladiProxy::RebuildPartitions(const std::vector<uint32_t>& keys,
//                                     const std::vector<std::string>& values) {
//     // ✅ Basic sanity check
//     if (keys.size() != values.size()) {
//         throw std::runtime_error("RebuildPartitions: keys.size() != values.size()");
//     }
//
//     // ✅ Validate inputs
//     if (bin_count_ == 0) {
//         std::cerr << "[RebuildPartitions] WARNING: bin_count_ = 0; nothing to rebuild."
//                   << std::endl;
//         return;
//     }
//
//     std::cout << "[RebuildPartitions] Rebuilding partitions with "
//               << keys.size() << " keys" << std::endl;
//
//     // ============================================
//     // Step 1: 生成 synthetic data（类似 PerformPartitioning）
//     // ============================================
//     std::vector<std::string> synthetic_data;
//     synthetic_data.reserve(keys.size());
//
//     std::cout << "[RebuildPartitions] Generating synthetic data based on keys..." << std::endl;
//
//     // 根据 attribute_indices_ 决定如何生成数据
//     // 如果只有一个属性（bin_id），就直接用 bin_id
//     for (uint32_t key : keys) {
//         if (bin_count_ == 0) {
//             throw std::invalid_argument("[RebuildPartitions] bin_count_ is zero");
//         }
//         uint32_t bin = key % bin_count_;
//
//         // 简单格式: 每行一个 bin_id
//         // 如果需要多属性，可以扩展为 "bin_id,attr2,attr3"
//         synthetic_data.push_back(std::to_string(bin));
//     }
//
//     std::cout << "[RebuildPartitions] Generated " << synthetic_data.size()
//               << " synthetic data points" << std::endl;
//
//     // ============================================
//     // Step 2: Call Repartition_Main 使用成员变量参数
//     // ============================================
//     std::cout << "[RebuildPartitions] Calling Repartition_Main with:" << std::endl;
//     std::cout << "  - Schema: " << schema_str_ << std::endl;
//     std::cout << "  - Attributes: [";
//     for (size_t i = 0; i < attribute_indices_.size(); ++i) {
//         std::cout << attribute_indices_[i];
//         if (i < attribute_indices_.size() - 1) std::cout << ", ";
//     }
//     std::cout << "]" << std::endl;
//     std::cout << "  - Epsilon (synopsis): " << epsilon_synopsis_ << std::endl;
//     std::cout << "  - Epsilon (threshold): " << epsilon_threshold_budget_ << std::endl;
//     std::cout << "  - Threshold (pub): " << threshold_pub_ << std::endl;
//     std::cout << "  - Mode: " << (use_fixed_partition_number_ ? "Fixed partition number" : "Fixed threshold") << std::endl;
//     if (use_fixed_partition_number_) {
//         std::cout << "  - Target partitions: " << max_partitions_ << std::endl;
//     } else {
//         std::cout << "  - Fixed threshold: " << fixed_threshold_value_ << std::endl;
//     }
//
//     try {
//         partitions_metadata_ = repartitioner_->Repartition_Main(
//             synthetic_data,              // 1. 实际数据
//             schema_str_,                 // 2. schema（从成员变量）
//             attribute_indices_,          // 3. 属性索引（从成员变量）
//             epsilon_synopsis_,           // 4. epsilon for synopsis（从成员变量）
//             epsilon_threshold_budget_,   // 5. epsilon for threshold（从成员变量）
//             threshold_pub_,              // 6. threshold（从成员变量）
//             use_fixed_partition_number_, // 7. 是否固定分区数（从成员变量）
//             max_partitions_              // 9. 固定分区数（从成员变量）
//         );
//     } catch (const std::exception& e) {
//         std::cerr << "[RebuildPartitions] Repartition_Main failed: " << e.what() << std::endl;
//         throw;
//     }
//
//     std::cout << "[RebuildPartitions] Repartition_Main returned "
//               << partitions_metadata_.size() << " partitions" << std::endl;
//
//     // output partition;
//     for (size_t i = 0; i < partitions_metadata_.size(); ++i) {
//         const auto& meta = partitions_metadata_[i];
//         std::cout << "  Partition " << i << ": "
//                   << meta.synopsis << " real items, "
//                   << meta.noisy_synopsis << " noisy synopsis, "
//                   << meta.dummy_num << " dummy items, "
//                   << meta.index.size() << " bins" << std::endl;
//     }
//
//     // ============================================
//     // Step 3: clear the structure;
//     // ============================================
//     partitions_.clear();
//     key_to_partition_.clear();
//
//     // ============================================
//     // Step 4: build key -> bin and bin -> partition mapping;
//     // ============================================
//
//     // 4.1 Pre-compute key -> bin mapping
//     std::unordered_map<uint32_t, uint32_t> key_to_bin;
//     key_to_bin.reserve(keys.size());
//     for (uint32_t k : keys) {
//         uint32_t bin = k % bin_count_;
//         key_to_bin.emplace(k, bin);
//     }
//
//     // 4.2 Build bin -> partition mapping
//     std::vector<int32_t> bin_to_pid(bin_count_, -1);
//     for (std::size_t pid = 0; pid < partitions_metadata_.size(); ++pid) {
//         const auto& meta = partitions_metadata_[pid];
//         // ✅ meta.index 现在是 vector<string>，需要转换为 uint32_t
//         for (const std::string& bin_str : meta.index) {
//             uint32_t bin = std::stoul(bin_str);  // Convert string to uint32_t
//             // ✅ Bounds check
//             if (bin >= bin_count_) {
//                 std::cerr << "[RebuildPartitions] WARNING: bin index "
//                           << bin << " out of range (bin_count_="
//                           << bin_count_ << ")" << std::endl;
//                 continue;
//             }
//             bin_to_pid[bin] = static_cast<int32_t>(pid);
//         }
//     }
//
//     // ============================================
//     // Step 5: construct PartitionInfo + RingORAM;
//     // ============================================
//     partitions_.reserve(partitions_metadata_.size());
//
//     for (std::size_t pid = 0; pid < partitions_metadata_.size(); ++pid) {
//         const auto& meta = partitions_metadata_[pid];
//
//         // ✅ Check for overflow in capacity
//         if (OverflowCheck::WouldAddOverflow(meta.synopsis, meta.dummy_num)) {
//             throw std::overflow_error("[RebuildPartitions] Capacity overflow at partition " +
//                                      std::to_string(pid));
//         }
//         uint32_t capacity = meta.synopsis + meta.dummy_num;
//
//         // ✅ Validate capacity
//         if (capacity == 0) {
//             throw std::invalid_argument("[RebuildPartitions] Zero capacity at partition " +
//                                        std::to_string(pid));
//         }
//
//         uint32_t read_ops  = capacity;
//         uint32_t write_ops = capacity;
//
//         auto pinfo = std::make_unique<PartitionInfo>(
//             static_cast<uint32_t>(pid),
//             meta.index,
//             meta.synopsis,
//             meta.dummy_num,
//             read_ops,
//             write_ops,
//             batch_size_
//         );
//
//         pinfo->batch_size = batch_size_;
//
//         // Assign connector
//         if (connectors_.empty()) {
//             throw std::runtime_error("[RebuildPartitions] connectors_ is empty!");
//         }
//         // ✅ Safe modulo
//         size_t conn_idx = pid % connectors_.size();
//         pinfo->connector = connectors_[conn_idx];
//
//         // Create ORAM for this partition
//         std::string oram_name = "partition_" + std::to_string(pid);
//         try {
//             // ✅ Safe bucket_size calculation
//             uint32_t bucket_size = OverflowCheck::SafeAdd(Z_param_, S_param_, "bucket_size");
//
//             pinfo->oram = std::make_unique<RingORAM>(
//                 capacity,
//                 bucket_size,
//                 oram_name,
//                 block_length_,
//                 pinfo->connector,
//                 S_param_
//             );
//
//             std::cout << "[RebuildPartitions] Created ORAM for partition "
//                       << pid << " (capacity=" << capacity
//                       << ", connector_idx=" << conn_idx << ")" << std::endl;
//         } catch (const std::exception& e) {
//             std::cerr << "[RebuildPartitions][ERROR] Failed to create ORAM for partition "
//                       << pid << ": " << e.what() << std::endl;
//             throw;
//         }
//
//         // Initialize batch queues
//         uint32_t epoch_id = current_epoch_ ? current_epoch_->epoch_id : epoch_counter_;
//
//         // ✅ Validate batch count
//         if (pinfo->num_read_batches > 1000000) {
//             throw std::overflow_error("[RebuildPartitions] Unreasonable num_read_batches");
//         }
//
//         // Read batches
//         pinfo->current_read_batches.clear();
//         pinfo->current_read_batches.reserve(pinfo->num_read_batches);
//         for (uint32_t b = 0; b < pinfo->num_read_batches; ++b) {
//             pinfo->current_read_batches.push_back(
//                 std::make_unique<Bat>(
//                     b,
//                     epoch_id,
//                     static_cast<uint32_t>(pid),
//                     /*is_read=*/true,
//                     pinfo->batch_size
//                 )
//             );
//         }
//         pinfo->current_read_batch_idx = 0;
//
//         // Write batches
//         pinfo->current_write_batches.clear();
//         pinfo->current_write_batch_idx = 0;
//         pinfo->current_write_batches.push_back(
//             std::make_unique<Bat>(
//                 0,
//                 epoch_id,
//                 static_cast<uint32_t>(pid),
//                 /*is_read=*/false,
//                 pinfo->batch_size
//             )
//         );
//
//         partitions_.push_back(std::move(pinfo));
//     }
//
//     // ============================================
//     // Step 6: put all keys into the partitions;
//     // ============================================
//     std::cout << "[RebuildPartitions] Inserting " << keys.size()
//               << " keys into new partitions..." << std::endl;
//
//     for (std::size_t i = 0; i < keys.size(); ++i) {
//         uint32_t key = keys[i];
//         const std::string& value = values[i];
//
//         auto it_bin = key_to_bin.find(key);
//         if (it_bin == key_to_bin.end()) {
//             std::cerr << "[RebuildPartitions] WARNING: Key " << key
//                       << " not in key_to_bin mapping" << std::endl;
//             continue;
//         }
//         uint32_t bin = it_bin->second;
//
//         // ✅ Bounds check
//         if (bin >= bin_to_pid.size()) {
//             std::cerr << "[RebuildPartitions] ERROR: bin " << bin
//                       << " >= bin_to_pid.size() " << bin_to_pid.size() << std::endl;
//             continue;
//         }
//
//         int32_t pid = bin_to_pid[bin];
//
//         // ✅ Validate partition ID
//         if (pid < 0 || static_cast<std::size_t>(pid) >= partitions_.size()) {
//             std::cerr << "[RebuildPartitions] WARNING: Invalid partition " << pid
//                       << " for key " << key << std::endl;
//             continue;
//         }
//
//         PartitionInfo* pinfo = partitions_[pid].get();
//         if (!pinfo || !pinfo->oram) {
//             throw std::runtime_error("RebuildPartitions: null partition or ORAM");
//         }
//
//         try {
//             pinfo->oram->access(key, OpType::INSERT, value);
//         } catch (const std::exception& e) {
//             std::cerr << "[RebuildPartitions][ERROR] Exception while inserting key "
//                       << key << " into partition " << pid
//                       << ": " << e.what() << std::endl;
//         }
//
//         // Update global mapping
//         key_to_partition_[key] = static_cast<uint32_t>(pid);
//     }
//
//     std::cout << "[RebuildPartitions] ✅ Successfully rebuilt "
//               << partitions_.size() << " partitions with "
//               << keys.size() << " keys" << std::endl;
// }
//
// // ============================================
// // Statistics
// // ============================================
// void ObladiProxy::PrintStatistics() const {
//     std::lock_guard<std::mutex> lock(stats_mutex_);
//
//     std::cout << "\n========== Proxy Statistics ==========" << std::endl;
//     std::cout << "Total Commits:      " << total_commits_.load() << std::endl;
//     std::cout << "Total Aborts:       " << total_aborts_.load() << std::endl;
//     std::cout << "Total Reads:        " << total_reads_.load() << std::endl;
//     std::cout << "Total Writes:       " << total_writes_.load() << std::endl;
//     std::cout << "Current Epoch:      " << epoch_counter_ << std::endl;
//     std::cout << "Num Partitions:     " << partitions_.size() << std::endl;
//     std::cout << "Insert Partition:   " << insert_count_.load() << " objects" << std::endl;
//     std::cout << "Repartition Threshold: " << repartition_threshold_ << std::endl;
//
//     double abort_rate = 0.0;
//     // ✅ Check for overflow in addition
//     uint64_t commits = total_commits_.load();
//     uint64_t aborts = total_aborts_.load();
//
//     if (!OverflowCheck::WouldAddOverflow64(commits, aborts)) {
//         uint64_t total = commits + aborts;
//         if (total > 0) {
//             abort_rate = (double)aborts / total * 100.0;
//         }
//     } else {
//         std::cerr << "[Statistics] WARNING: Total transaction count overflow" << std::endl;
//     }
//
//     std::cout << "Abort Rate:         " << abort_rate << "%" << std::endl;
//
//     // Print per-partition statistics
//     std::cout << "\n--- Per-Partition Statistics ---" << std::endl;
//     for (size_t i = 0; i < partitions_.size(); ++i) {
//         const auto& pinfo = partitions_[i];
//         if (pinfo) {
//             std::cout << "  Partition " << i << ": "
//                       << pinfo->real_count << " real, "
//                       << pinfo->dummy_count << " dummy, "
//                       << pinfo->bin_indices.size() << " bins" << std::endl;
//         }
//     }
//
//     if (insert_partition_) {
//         std::cout << "  Insert Partition: "
//                   << insert_count_.load() << " objects" << std::endl;
//     }
//
//     std::cout << "======================================\n" << std::endl;
// }
