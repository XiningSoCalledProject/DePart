//
// Created by Xining Yuan on 6/11/25.
//

//
// Created by Xining Yuan on 4/28/25.
//

#include "RingORAM.h"
#include <cmath>
#include <cryptopp/osrng.h>
#include <string>
#include "iostream"
#include "NetIOConnector.h"
#include <string>
#include <algorithm>
#include <unordered_set>
#include <vector>
#include <random>

using namespace CryptoPP;


RingORAM::RingORAM(const uint32_t &n, const uint32_t &buck_size,
                   const std::string &pathoram_name, const uint32_t &block_length,
                   NetIOConnector* ORAMO, const uint32_t &S_input,
                   const uint32_t permanent_stash_reserve) {
    // permanent_stash_reserve: number of real records that permanently live in
    // the stash because the ORAM tree is sized to noisy_synopsis < real_count
    // (Case B privacy fix: server always sees a noisy-sized tree, not real-sized).
    // These records are never evicted into the tree; the stash is pre-enlarged to
    // accommodate them so no overflow abort is ever triggered.
    // parameters check;
    if (block_length <= sizeof(uint32_t)) {
        throw std::invalid_argument("tuple_length must be > 4");
    }
    if (buck_size == 0) {
        throw std::invalid_argument("buck_size must be > 0");
    }
    // ── FIX (4/26/26): guard against n=0 which makes log2(n) undefined.
    // For TPC-C with extreme epsilon, noisy_synopsis can theoretically be
    // 0 if Laplace noise is large negative; without this guard, height
    // becomes UB (cast from -infinity to uint32_t).
    if (n == 0) {
        throw std::invalid_argument(
            "RingORAM: n must be >= 1 (got 0). "
            "If this is a DP-noisy partition with zero records, "
            "use Math.max(1, noisy_synopsis) at the caller.");
    }

    // initialize the basic parameters;
    round = 0;
    bucket_size = buck_size;
    S = S_input;
    permanent_stash_reserve_ = permanent_stash_reserve;
    if (S > buck_size) {
        throw std::invalid_argument("S (dummy slots) must be <= bucket size (total slots)");
    }
    blocks_Z = buck_size - S;
    name = pathoram_name;
    n_blocks = n;
    height = static_cast<uint32_t>(ceil(log2((double) n))) + 1;
    std::cout << "The height of the ORAM is " << height << std::endl;
    num_leaves = 1 << (height - 1);  // the bottom leaves number;
    std::cout << " The number of leaves is " << num_leaves << std::endl;
    // Eviction frequency A: how many accesses between consecutive EvictPath() calls.
    // RingORAM paper recommends A = O(log N) = height.
    A = height;
    std::cout << "The A (eviction period) is " << A << std::endl;

    // stash_limit: absolute safe upper bound for stash occupancy.
    //
    // Two components:
    //   (a) Normal ORAM stash bound: max(6 × h × Z, 2 × N + 50)
    //       This covers steady-state stash occupancy and transient spikes from
    //       EarlyReshuffle (up to height × bucket_size blocks pushed at once).
    //
    //   (b) permanent_stash_reserve_: records that permanently live in the stash
    //       because the ORAM tree is sized to noisy_synopsis < real_count (Case B
    //       privacy fix).  These never evict into the tree, so the limit must
    //       account for them on top of the normal steady-state bound.
    stash_limit = std::max(6u * height * blocks_Z, 2u * n_blocks + 50u)
                  + permanent_stash_reserve_;

    std::cerr << "[Init] ORAM height = " << height
              << ", num_leaves = " << num_leaves
              << ", blocks_Z = " << blocks_Z
              << ", A (eviction period) = " << A
              << ", stash_limit = " << stash_limit
              << "  (n_blocks=" << n_blocks
              << ", permanent_stash_reserve=" << permanent_stash_reserve_ << ")" << std::endl;

    // initialize the encryption;
    ecrypt_key = new byte[Util::key_length];
    Util::prng.GenerateBlock(ecrypt_key, Util::key_length);


    // network setting;
    conn = ORAMO;
    block_size = block_length;
    conn -> setTupleLength(pathoram_name, block_size);  // set the fixed tuple length;

    std::cerr << "[Init] Fixed block length (B) = " << block_size
              << ", S = " << S << " dummy per bucket\n";

    // initialize the pos_map, 1-multi pos map; (every block_id to vector<ChunkPos>);
    block_chunks_pos.clear();  // use Put to fill in;

    // calculate the bucket count（binary tree structure）
    uint32_t total_buckets = (1 << height) - 1;
    std::cerr << "[Init] Total buckets = " << total_buckets << std::endl;

    buckets.clear();
    bucket_blockdata.clear();

    // initialize bucket and metadata，and fill in the dummy date;
    for (uint32_t i = 0; i < total_buckets; ++i) {
        BucketBlock Block;
        RingBucket bucket(bucket_size);

        Block.valids.resize(bucket_size, true);             // all valid
        std::unordered_set<uint32_t> real_slots;
        // randomly choose Z slots to put the real slots;
        while (real_slots.size() < blocks_Z) {
            real_slots.insert(Util::rand_int(bucket_size));
        }
        for (const auto& slot : real_slots) {
            Block.ptrs.push_back(slot);
    /* quieted: std::cout << "The real slot in the bucket " << i << " is " << slot  << std::endl; */
        }
        Block.count = 0;

        for (uint32_t slot = 0; slot < bucket_size; slot++) {
            // this slot is not in the real slot set or the selected_real_slots; put the dummy slot;
            std::string cipher;

            // fill in all slots with dummy data;
            std::string dummy_payload = Util::generate_random_block(block_size - Util::aes_block_size - sizeof(uint32_t));
            int32_t dummy_id = -1;
            std::string block_id_str(reinterpret_cast<char*>(&dummy_id), sizeof(uint32_t));
            std::string plain = block_id_str + dummy_payload;

            //std::cout << "The plain text for this dummy block is " << plain << std::endl;

            Util::aes_encrypt(plain, ecrypt_key, cipher);

            uint32_t global_slot_id = i * bucket_size * block_size + slot * block_size;

            conn -> insert_slot(global_slot_id, cipher, name, block_size);

        }

        // save the information on the client;
        bucket_blockdata[i] = Block;
    }
    std::cerr << "[Init Done] RingORAM initialization complete.\n";
}


RingORAM::~RingORAM() {
    std::cout << "[~RingORAM] Destructor called. Peak stash size = "
              << stash_high_watermark << " / " << stash_limit << std::endl;
}

// Post-initialization eviction.
//
// After inserting all records during init, the stash may hold more than A
// entries.  In Case B (noisy < real), permanent_stash_reserve_ records will
// never evict into the ORAM tree because the tree is sized to noisy_synopsis;
// these permanently live in the stash and are accessed via the normal stash
// read path.  We therefore drain to (A + permanent_stash_reserve_), not to A.
//
// We stop at the target (not 0) because RingORAM is designed to tolerate
// up to A stash entries between eviction cycles; draining further would
// waste the reverse-lexicographic path budget unnecessarily.
void RingORAM::flush_stash_if_needed() {
    uint32_t target = A + permanent_stash_reserve_;
    if (stash.size() <= target) {
        std::cout << "[flush_stash] Stash size " << stash.size()
                  << " ≤ target=" << target
                  << " (A=" << A << " + reserve=" << permanent_stash_reserve_
                  << "), no extra eviction needed.\n";
        return;
    }
    uint32_t evict_count = 0;
    uint32_t initial_size = static_cast<uint32_t>(stash.size());
    while (stash.size() > target) {
        EvictPath();
        ++evict_count;
        // Safety: if stash isn't shrinking after many passes, the tree is
        // full and further eviction won't help.  Break to avoid infinite loop.
        // (For Case B this is expected once the tree fills to noisy capacity.)
        if (evict_count > 2 * num_leaves) {
            std::cerr << "[flush_stash] WARNING: stash still has "
                      << stash.size() << " entries after " << evict_count
                      << " extra evictions (expected for Case B if tree is full).\n";
            break;
        }
    }
    std::cout << "[flush_stash] " << evict_count << " extra eviction(s): "
              << initial_size << " → " << stash.size()
              << " stash entries  (target ≤ " << target << ")\n";
}

// Internal helper: insert one (block_id, chunk_id) → data entry into the stash.
// Enforces the fixed stash_limit and updates the high-watermark counter.
inline void RingORAM::stash_insert(uint32_t block_id, uint32_t chunk_id, const std::string& data) {
    stash[{block_id, chunk_id}] = data;   // overwrite if already present (READ re-inserts same key)
    uint32_t cur = static_cast<uint32_t>(stash.size());
    if (cur > stash_high_watermark) stash_high_watermark = cur;
    if (cur > stash_limit) {
        // This should be unreachable: stash_limit = max(6*h*Z, 2*N+50),
        // which is a true hard ceiling (there are only N blocks in the system,
        // each contributing at most 2 stash entries).
        // If we ever get here it indicates a bug in block accounting.
        std::cerr << "[STASH OVERFLOW – BUG] size=" << cur
                  << " exceeds hard ceiling=" << stash_limit
                  << "  (block_id=" << block_id << ", chunk_id=" << chunk_id << ")\n";
        std::abort();
    }
}

std::string RingORAM::get(const uint32_t & key) {
    std::string res;
    uint32_t int_key = key;
    std::string block = access(int_key, OpType::READ, res);
    return block;
}

std::string RingORAM::access(uint32_t block_id, OpType op, const std::string& data_in) {
    if (op == OpType::INSERT){
    /* quieted: std::cout << "We want to put " << data_in << " in the database." << std::endl; */
        //Step 1: encrypt the data block;
        std::string cipher;
        Util::aes_encrypt(data_in, ecrypt_key, cipher);
    /* quieted: std::cout << "After the encryption, the data block is " << cipher << std::endl; */
        size_t total = cipher.size();
        // Step 2: split the data block into the various chunks;
        size_t offset = 0;
        uint32_t chunk_id = 0;
        while (offset < total) {
            size_t take = std::min<size_t>(static_cast<size_t>(block_size), total - offset);
            std::string chunk = cipher.substr(offset, take);
            chunk.resize(block_size, '\0');  // keep the same length of the block size;
            // randomly choose a path for every chunk;
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distrib(0, num_leaves - 1);
            uint32_t L = distrib(gen);
    /* quieted: std::cout << "The path ID is " << L << std::endl; */
            block_chunks_pos[block_id][chunk_id] = L;
            stash_insert(block_id, chunk_id, chunk);
            chunk_id += 1;
            offset += take;
        }
        round = (round + 1) % A;
        if (round == 0) {
            std::cout << "[EvictPath]: Triggers the EvictPath()." << std::endl;
            EvictPath();
        }
        return "";
    }

    // Step 1: change/ give the new position of the block_id;
    // ── FIX (4/26/26): defensive check for unknown block_id ───────────────
    // If proxy passes a block_id that was never INSERTed (typically a bug
    // in proxy's external_pk → block_id mapping), block_chunks_pos[block_id]
    // would silently insert an empty entry via operator[].  Then the access
    // loop runs 0 times, reassembled_data is empty, and Util::aes_decrypt("")
    // throws inside CryptoPP — child crashes, parent sees EOF, hard to debug.
    //
    // Instead: detect the missing block_id, log once per block_id, and return
    // empty string.  Proxy can check `per_op_results[i].empty()` and surface
    // it as a logical error (e.g., "primary_key 42 → block_id mapping wrong").
    {
        auto bcp_it = block_chunks_pos.find(block_id);
        if (bcp_it == block_chunks_pos.end() || bcp_it->second.empty()) {
            static std::unordered_map<uint32_t, uint64_t> miss_counts;
            uint64_t& cnt = miss_counts[block_id];
            cnt++;
            if (cnt == 1 || cnt % 1000 == 0) {
                std::cerr << "[access] WARNING: block_id=" << block_id
                          << " not found in pos_map (op="
                          << static_cast<int>(op) << ", cnt=" << cnt
                          << ").  Proxy mapping likely wrong.  "
                          << "Returning empty string.\n";
            }
            return "";   // graceful return; proxy detects via empty plaintext
        }
    }

    std::map<uint32_t, std::map<uint32_t, std::string>> block_to_chunks;
    for (const auto &[chunk_id, pos_id] : block_chunks_pos[block_id]) {
        uint32_t current_path = pos_id;
    /* quieted: std::cout << "The current path number is " << current_path << std::endl; */
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(0, num_leaves - 1);
        uint32_t L = distrib(gen);
    /* quieted: std::cout << "The path ID is " << L << std::endl; */
        // update the pos_map;
        block_chunks_pos[block_id][chunk_id] = L;

        // Step 2: Get the chunk in stash or the server;
        std::pair<uint32_t, uint32_t> block_chunk = {block_id, chunk_id};
        std::string corres_data = ReadPath(current_path, block_chunk);
        if (corres_data.empty()) {
            // this data must be in the stash, so find this data in the stash;
            corres_data = stash[{block_id, chunk_id}];
    /* quieted: std::cout << "The encrypted block is " << corres_data << std::endl; */
            block_to_chunks[block_id][chunk_id] = corres_data;
        } else {
            block_to_chunks[block_id][chunk_id] = corres_data;
        }
        EarlyReshuffle(current_path);
    }
    std::string reassembled_data;
    for (const auto &[chunk_id, encrypted_chunk]: block_to_chunks[block_id]) {
        reassembled_data += encrypted_chunk;
    }

    // ── FIX #6 (latent): multi-chunk reassembly safety check ─────────────
    // Each chunk was padded to block_size on INSERT.  If cipher.size() was
    // > block_size (multi-chunk), then reassembled_data now contains the
    // padding bytes from each chunk and AES decrypt will fail on the trailing
    // \0 bytes.  This is a known limitation: current callers use block_size
    // big enough to hold cipher in one chunk, so this branch is normally not
    // hit.  We assert here so any future caller hitting it gets a loud error
    // instead of silent garbage.
    if (block_to_chunks[block_id].size() > 1) {
        std::cerr << "[access] WARNING: multi-chunk read for block_id="
                  << block_id << " (chunks=" << block_to_chunks[block_id].size()
                  << "); reassembly does not strip per-chunk padding.  "
                  << "If decryption fails, encode the original cipher length "
                  << "in chunk 0 and truncate before decrypt.\n";
    }
    // ─────────────────────────────────────────────────────────────────────

    if (op == OpType::READ) {
        std::string plain;
        Util::aes_decrypt(reassembled_data, ecrypt_key, plain);
    /* quieted: std::cout << "The plain block in the stash is " << plain << std::endl; */
        // Step 4: If the operations is READ, return the block directly;
    /* quieted: std::cout << "I have already read back from the stash or the database and needs to check the reshuffle." << std::endl; */
        return plain;
    }
        // Step 5: If the operation is WRITE, modify the data and put the block to the correct position (stash or server);
    else if (op == OpType::UPDATE) {
    /* quieted: std::cout << "We want to put " << data_in << " in the database." << std::endl; */
        std::string cipher;
        Util::aes_encrypt(data_in, ecrypt_key, cipher);
        reassembled_data = cipher;
        // Step 2: split the data block into the various chunks;
        size_t total = cipher.size();
        size_t offset = 0;
        uint32_t chunk_id = 0;
        while (offset < total) {
            size_t take = std::min<size_t>(static_cast<size_t>(block_size), total - offset);
            std::string chunk = cipher.substr(offset, take);
            chunk.resize(block_size, '\0');  // keep the same length of the block size;
            // randomly choose a path for every chunk;
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distrib(0, num_leaves - 1);
            uint32_t L = distrib(gen);
    /* quieted: std::cout << "The path ID is " << L << std::endl; */
            block_chunks_pos[block_id][chunk_id] = L;
            stash_insert(block_id, chunk_id, chunk);
            chunk_id += 1;
            offset += take;
        }
    /* quieted: std::cout << "I can run here, and put the data in the stash successfully." << std::endl; */
        // if the number of accessing the database is the multiple of the A, EvictPath;
        round = (round + 1) % A;
        if (round == 0) {
    /* quieted: std::cout << "Try to evict the data to the database." << std::endl; */
            EvictPath();
        }
    }

    return "";
}

// use the BucketBlock data structure on the client:
// check the bucket through bucket_idx, using the corresponding target_block_id, return the data;
// if no target block_id slot, return the dummy slot;
std::pair<uint32_t, bool> RingORAM::GetBlockOffset(uint32_t bucket_idx, std::pair <uint32_t, uint32_t> block_chunk_id) {
    BucketBlock& Block = bucket_blockdata[bucket_idx];
    std::vector<uint32_t> dummy_slot_id_to_pos;
    for (uint32_t i = 0; i < Block.addrs.size(); ++i) {
        if (Block.addrs[i] == block_chunk_id && Block.valids[Block.ptrs[i]]) {
    /* quieted: std::cout << "I find the target data block in the bucket " << bucket_idx << " slot " << Block.ptrs[i] << std::endl; */
            Block.valids[Block.ptrs[i]] = false;
            return {Block.ptrs[i], true};
        }
    }

    std::unordered_set<uint32_t> real_slots;

    for (uint32_t i = 0; i < Block.ptrs.size(); ++i) {
        real_slots.insert(Block.ptrs[i]);
    }

    for (uint32_t slot = 0; slot < bucket_size; ++slot) {
        if (real_slots.find(slot) == real_slots.end() && Block.valids[slot]) {
            dummy_slot_id_to_pos.push_back(slot);
    /* quieted: std::cout << "The dummy slot is " << slot << std::endl; */
        }
    }

    if (!dummy_slot_id_to_pos.empty()) {
        uint32_t idx = Util::rand_int(dummy_slot_id_to_pos.size());
        uint32_t chosen_slot = dummy_slot_id_to_pos[idx];
    /* quieted: std::cout << "We finally choose dummy slot " << dummy_slot_id_to_pos[idx] << " to get." << std::endl; */
        Block.valids[chosen_slot] = false;
        return {chosen_slot, false};
    }
    std::cerr << "[ERROR] No valid dummy slots or real blocks found in bucket " << bucket_idx << std::endl;
    exit(1);
}

// Input: the block with block_id that I want and the PathID this block is in;
// Output: return the encrypted data block;
std::string RingORAM::ReadPath(uint32_t PathID, std::pair <uint32_t, uint32_t> block_chunk_id) {
    std::string return_encrypted_block;
    // Step 2: if the data is in the RingORAM, find in the RingORAM;
    std::vector<std::string> encrypted_blocks;
    // find all buckets along this PathID;
    std::vector<uint32_t> access_path;
    std::vector<std::pair<uint32_t, bool>> bucket_slot_is_real;
    for (uint32_t i = 0; i < height; ++i) {
        uint32_t idx = (1 << i) - 1 + (PathID >> (height - 1 - i));
        access_path.push_back(idx);
    /* quieted: std::cout << "The bucket id is " << idx << std::endl; */
    }

    uint32_t block_id = block_chunk_id.first;
    uint32_t chunk_id = block_chunk_id.second;
    // Step 3: Organize all slots along the Path and send them to the RingORAM;
    for (size_t j = 0; j < access_path.size(); ++j) {
        uint32_t bucket_id = access_path[j];
    /* quieted: std::cout << "The bucket id is " << bucket_id << std::endl; */
        auto [slot_id, is_real] = GetBlockOffset(bucket_id, block_chunk_id);
        uint32_t global_slot_id = bucket_id * bucket_size * block_size + slot_id * block_size;
    /* quieted: std::cout << "The global slot id is " << global_slot_id << std::endl; */
        BucketBlock& Block = bucket_blockdata[bucket_id];
        Block.count += 1;
    /* quieted: std::cout << "The count of the bucket " << bucket_id << " is " << Block.count << std::endl; */
        std::string encrypted = conn -> find_slot(bucket_id, global_slot_id, name, block_size);
        if (is_real) {
            stash_insert(block_id, chunk_id, encrypted);
            return_encrypted_block = encrypted;
        }
    }
    return return_encrypted_block;
}


// input: the bucket that we want to read;
// Put Z blocks in the stash;
void RingORAM::ReadBucket(uint32_t bucket_idx) {
    /* quieted: std::cout << "[ReadBucket] Reading bucket " << bucket_idx << std::endl; */
    BucketBlock& Block = bucket_blockdata[bucket_idx];
    std::vector<uint32_t> return_slots;

    // Step 1: find remaining Z blocks in the bucket;
    for (uint32_t i = 0; i < bucket_size; ++i) {
        if (Block.valids[i]) {
            return_slots.push_back(i);
    // quieted: std::cout << "The slot in the bucket " << bucket_idx
                      // << " that needs to read is " << i << std::endl;
        }
    }

    std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> real_slot_id;
    for (uint32_t k = 0; k < Block.addrs.size(); ++k) {
        real_slot_id[Block.ptrs[k]] = Block.addrs[k];
    }
    // Step 3: Read Z slots from the corresponding bucket;
    for (uint32_t z = 0; z < return_slots.size(); ++z) {
        uint32_t slot_id = return_slots[z];
        uint32_t global_slot_number = bucket_idx * bucket_size * block_size + slot_id * block_size;
        std::string encrypted = conn -> find_slot(bucket_idx, global_slot_number, name, block_size);
        if (real_slot_id.find(slot_id) != real_slot_id.end()){
            auto [bid, cid] = real_slot_id[slot_id];
            stash_insert(bid, cid, encrypted);
        }
    }
}


// When the access S times for one bucket, call the early reshuffling function;
// input: the bucket_idx that we need to do the early reshuffle;
// Reassign the slots order; reset the count as 0; reset the valid;
void RingORAM::EarlyReshuffle(uint32_t Path_ID) {
    // Step 1: Check every bucket along this Path;
    std::vector<uint32_t> buckets_need_early_reshuffle;
    for (size_t i = 0; i < height; ++i) {
        uint32_t bucket_idx = (1 << i) - 1 + (Path_ID >> (height - 1 - i));
    /* quieted: std::cout << "[Early Reshuffle Check]: The checking bucket is " << bucket_idx << std::endl; */
        BucketBlock& Block = bucket_blockdata[bucket_idx];
    /* quieted: std::cout << "The bucket count is " << Block.count << std::endl; */
        if(Block.count < S){
    /* quieted: std::cout << "The path " << i << " does not need to early reshuffle." << std::endl; */
            continue;
        } else {
            buckets_need_early_reshuffle.push_back(bucket_idx);

        }
    }
    std::reverse(buckets_need_early_reshuffle.begin(), buckets_need_early_reshuffle.end());
    if (buckets_need_early_reshuffle.size() == 0) {
        return;
    }

    // Step 2: EarlyReshuffle all buckets that count reaches S;
    for (size_t j = 0; j < buckets_need_early_reshuffle.size(); ++j){
        ReadBucket(buckets_need_early_reshuffle[j]);
        WriteBucket(buckets_need_early_reshuffle[j]);
    }
}

// input: bucket_idx: which bucket we have to write the data in；local_stash:
// put blocks that belongs to this buket in this bucket;
void RingORAM::WriteBucket(uint32_t bucket_idx) {
    // <block_id, chunk_id>, block> pair;
    std::unordered_map<std::pair<uint32_t, uint32_t>, std::string, pair_hash> block_chunk_string;
    uint32_t realblocks_count = 0;

    // ── FIX #1 (OOM): in-place iterate stash and erase, NO full copy ──────
    // Old code: `auto temp_stash = stash;` copied the entire stash (could be
    //   hundreds of MB after bulk_load_stash).  EvictPath calls WriteBucket
    //   `height` times → quadratic transient memory.
    // New code: walk stash with iterator, use `stash.erase(it)` which returns
    //   the next iterator (C++17 guarantees only the erased element's iterator
    //   is invalidated for unordered_map).
    //   Memory cost: O(blocks_Z) for `block_chunk_string` only.
    // ──────────────────────────────────────────────────────────────────────
    for (auto it = stash.begin();
         it != stash.end() && realblocks_count < blocks_Z; ) {
        const auto& key = it->first;          // {block_id, chunk_id}

        // Defensive: avoid block_chunks_pos[…][…] = … operator[] insertion
        // side-effect.  Only consider entries that already have a position.
        auto bid_it = block_chunks_pos.find(key.first);
        if (bid_it == block_chunks_pos.end()) { ++it; continue; }
        auto cid_it = bid_it->second.find(key.second);
        if (cid_it == bid_it->second.end())   { ++it; continue; }
        uint32_t leaf = cid_it->second;

        // Each path × bucket has at most one intersection level; walk levels
        // and check.  (For a given bucket_idx, only the level matching its
        // depth can yield bucket_idx, so this loop exits early.)
        bool matched = false;
        for (uint32_t level = 0; level < height; ++level) {
            uint32_t bucket_index =
                (1u << level) - 1u + (leaf >> (height - 1 - level));
            if (bucket_index == bucket_idx) {
                block_chunk_string[key] = std::move(it->second);
                it = stash.erase(it);
                ++realblocks_count;
                matched = true;
                break;
            }
        }
        if (!matched) ++it;
    }

    /* quieted: std::cout << "I can run here successfully." << std::endl; */
    // Step 2: permutation all slots and add dummy slots;
    // initialize bucket and metadata，and fill in the dummy date;
    BucketBlock& Block = bucket_blockdata[bucket_idx];
    RingBucket bucket(bucket_size);
    Block.valids.assign(bucket_size, true);          // all valid
    for (uint32_t i = 0; i < Block.valids.size(); ++i) {
    /* quieted: std::cout << "The slot " << i << " of the bucket " << bucket_idx << " is " << Block.valids[i] << std::endl; */
    }
    Block.ptrs.clear();
    Block.addrs.clear();
    std::unordered_set<uint32_t> real_slots;
    // randomly choose Z slots to put the real slots;
    while (real_slots.size() < blocks_Z) {
        real_slots.insert(Util::rand_int(bucket_size));
    }
    for (const auto& slot : real_slots) {
    /* quieted: std::cout << "The real slot in the bucket " << bucket_idx << " is " << slot << std::endl; */
        Block.ptrs.push_back(slot);
    }

    for (uint32_t i = 0; i < Block.ptrs.size(); ++i) {
    /* quieted: std::cout << "The real slot in the pointer vector is " << Block.ptrs[i] << std::endl; */
    }
    Block.count = 0;
    // slot_id, <block_id, chunk_id>
    std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> real_slot_id;
    for (const auto& [key, value] : block_chunk_string) {
        uint32_t block_id = key.first;
        uint32_t chunk_id = key.second;
        Block.addrs.push_back(std::make_pair(block_id, chunk_id));
    }

    // Write the real slots chunks in the server;
    for (uint32_t j = 0; j < Block.addrs.size(); ++j) {
        real_slot_id[Block.ptrs[j]] = std::make_pair(Block.addrs[j].first, Block.addrs[j].second);
        //uint32_t global_slot_id =  bucket_idx * block_size * bucket_size + Block.ptrs[j] * block_size;
        //conn -> insert_slot(global_slot_id, block_chunk_string[real_slot_id[Block.ptrs[j]]], name, block_size);
    }

    // Set S dummy blocks in the bucket;
    for (uint32_t k = 0; k < bucket_size; ++k) {
        if (real_slot_id.find(k) == real_slot_id.end()) {
            std::string cipher;
    /* quieted: std::cout << "The dummy slot in the bucket " << bucket_idx << " is " << k << std::endl; */
            std::string dummy_payload = Util::generate_random_block(block_size - Util::aes_block_size - sizeof(uint32_t));
            int32_t dummy_id = -1;
            std::string block_id_str(reinterpret_cast<char*>(&dummy_id), sizeof(uint32_t));
            std::string plain = block_id_str + dummy_payload;

            Util::aes_encrypt(plain, ecrypt_key, cipher);

            // ── FIX #2: write each dummy slot exactly ONCE ─────────────────
            // Old code did the chunk-loop write THEN a second insert_slot at
            // the same global_slot_id, causing every dummy slot to be written
            // twice (extra RTT + bandwidth).  Each dummy occupies exactly one
            // slot of size block_size, so we just pad the cipher to block_size
            // and do a single write.
            // ───────────────────────────────────────────────────────────────
            if (cipher.size() > block_size) {
                std::cerr << "[WriteBucket] cipher size " << cipher.size()
                          << " > block_size " << block_size
                          << "  (dummy must fit in one slot)\n";
                std::abort();
            }
            cipher.resize(block_size, '\0');
            bucket.block[k] = cipher;
            uint32_t global_slot_id = bucket_idx * bucket_size * block_size + k * block_size;
            conn->insert_slot(global_slot_id, cipher, name, block_size);
        }
        else {
            auto it = real_slot_id.find(k);
            const auto& key = it->second; // pair<block_id, chunk_id>
            auto p = block_chunk_string.find(key);
            if (p == block_chunk_string.end()) {
                std::cerr << "[WriteBucket] Missing cipher for ("
                          << key.first << "," << key.second << ")\n";
                continue; // 或者 assert
            }
            const std::string& cipher = p->second;

            bucket.block[k] = cipher;
            uint32_t global_slot_id = bucket_idx * bucket_size * block_size + k * block_size;
            conn->insert_slot(global_slot_id, cipher, name, block_size);
        }
    }
}

// helper function: reverse-lexicographic order;
uint32_t RingORAM::reverse_bits(uint32_t val, uint32_t bitlen) {
    uint32_t res = 0;
    val &= ((1 << bitlen) - 1); // mask to keep only lower `bitlen` bits
    for (uint32_t i = 0; i < bitlen; ++i) {
        if (val & (1 << i)) {
            res |= (1 << (bitlen - 1 - i));
        }
    }
    return res;
}

//524286


void RingORAM::EvictPath() {
    std::cout << "[EvictPath]: We need to evict data from stash to the RingORAM." << std::endl;
    uint32_t L = height - 1;
    uint32_t l = reverse_bits(G, L);  // reverse-lexicographic index
    /* quieted: std::cout << "The path id we choose is " << l << std::endl; */
    G = (G + 1) % (1 << L);           // increment with wrap-around
    // Step 1: Collect all bucket indices on the path from root to leaf;
    std::vector<uint32_t> path_buckets;
    for (uint32_t level = 0; level < height; ++level) {
        uint32_t bucket_idx = (1 << level) - 1 + (l >> (height - 1 - level));
        path_buckets.push_back(bucket_idx);
    }

    std::reverse(path_buckets.begin(), path_buckets.end());
    // Step 2: Read the bucket to move data to stash
    for (uint32_t i = 0; i < path_buckets.size(); ++i) {
        ReadBucket(path_buckets[i]);
        WriteBucket(path_buckets[i]);
    }
}

// ============================================================================
//  Bulk-load fast path (4/20/26) — for TPC-C ServerInitializationBulk only
// ============================================================================
//
//  Put all records directly into stash (no access(), no eviction, no network).
//  Tree remains full of dummies. Reads will hit stash-first path automatically.
//
//  INVARIANT PRESERVED:
//    - For every (block_id, chunk_id) inserted: block_chunks_pos is set.
//    - stash contains the encrypted record content.
//    - bucket_blockdata.addrs stays empty (no real records in tree).
//
//  Tree-side consistency: after this, caller MUST call
//    conn->bulk_init_tree(name, block_size, generate_dummy_tree_payload())
//  to fill the server-side with the same encrypted dummies that a normal
//  RingORAM constructor would have sent.
// ============================================================================

void RingORAM::bulk_load_stash(
    const std::vector<std::pair<uint32_t, std::string>>& records)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> leaf_dist(0, num_leaves - 1);

    // Raise stash_limit to accommodate all records at once.
    // Normal stash_limit is set for steady-state operation (~6h*Z + permanent).
    // Bulk load exceeds this briefly; eviction during runtime will reduce it.
    stash_limit = std::max(stash_limit,
                            static_cast<uint32_t>(records.size()) +
                            6u * height * blocks_Z + 50u);

    for (const auto& [block_id, plain_data] : records) {
        // ── Encrypt the plaintext using this ORAM's key ──────────────────
        std::string cipher;
        Util::aes_encrypt(plain_data, ecrypt_key, cipher);

        // ── Split cipher into chunks the same way access(INSERT) does ────
        size_t total = cipher.size();
        size_t offset = 0;
        uint32_t chunk_id = 0;

        while (offset < total) {
            size_t take = std::min<size_t>(static_cast<size_t>(block_size),
                                            total - offset);
            std::string chunk = cipher.substr(offset, take);
            chunk.resize(block_size, '\0');

            // Assign a random leaf path (same randomization as access INSERT)
            uint32_t L = leaf_dist(gen);
            block_chunks_pos[block_id][chunk_id] = L;

            // Direct stash insertion — bypass stash_insert to skip its
            // per-element size assertion which would fire on large bulk loads
            stash[{block_id, chunk_id}] = chunk;

            chunk_id++;
            offset += take;
        }
    }

    uint32_t cur_size = static_cast<uint32_t>(stash.size());
    if (cur_size > stash_high_watermark) stash_high_watermark = cur_size;

    std::cout << "[bulk_load_stash] Loaded " << records.size()
              << " records into stash; stash size now " << cur_size
              << " (limit " << stash_limit << ")\n";
}

// ============================================================================
//  generate_dummy_tree_payload — DEPRECATED, kept for backward compat only.
//
//  ⚠️  This function allocates a vector with ALL dummy slots at once.  For an
//  ORDER_LINE-sized tree (millions of slots × block_size bytes each) this
//  reaches several GB and triggers the OOM killer under multi-table parallel
//  init.
//
//  Prefer the streaming variant: get_total_slot_count() + generate_one_dummy_slot().
//
//  This wrapper now delegates to generate_one_dummy_slot() so that at least the
//  per-slot generation logic is shared (single source of truth).  Callers that
//  still hit this path on small trees keep working; large trees should switch.
// ============================================================================
std::vector<SlotPayload> RingORAM::generate_dummy_tree_payload() {
    uint32_t total = get_total_slot_count();

    // Heuristic guard: refuse to materialize > 256 MB worth of payload.
    // (This is approximate; SlotPayload wraps a string of ~block_size bytes.)
    constexpr size_t kSafeMaxBytes = 256ull * 1024 * 1024;
    size_t expected_bytes = static_cast<size_t>(total) * block_size;
    if (expected_bytes > kSafeMaxBytes) {
        std::cerr << "[generate_dummy_tree_payload] REFUSING — payload would be "
                  << (expected_bytes / (1024 * 1024)) << " MB.  "
                  << "Use bulk_init_tree_stream + generate_one_dummy_slot instead.\n";
        std::abort();
    }

    std::vector<SlotPayload> payload;
    payload.reserve(total);
    for (uint32_t i = 0; i < total; ++i) {
        payload.push_back(generate_one_dummy_slot(i));
    }

    std::cout << "[generate_dummy_tree_payload] Generated " << payload.size()
              << " dummy slots\n";
    return payload;
}

// ============================================================================
//  Streaming dummy generation (v4, 4/20/26) — OOM-safe alternative
// ============================================================================
//
//  generate_dummy_tree_payload() allocates a std::vector with ALL slots
//  pre-generated in memory (2-5 GB for ORDER_LINE-sized tables). Under
//  8-table parallel init, peak memory across children exceeds 32 GB →
//  OOM killer kills children → init fails.
//
//  These two methods let the caller stream slots one at a time:
//
//    uint32_t N = oram->get_total_slot_count();
//    conn->bulk_init_tree_stream(name, bs, N,
//      [&](uint32_t i){ return oram->generate_one_dummy_slot(i); });
//
//  Peak memory per child: ~10 MB (just the network chunk buffer), regardless
//  of tree size. Safe for 8-table parallel init on 32 GB hosts.
//
//  Wire-format output is IDENTICAL to generate_dummy_tree_payload() +
//  bulk_init_tree() — server sees the exact same byte stream.
// ============================================================================

uint32_t RingORAM::get_total_slot_count() const {
    uint32_t total_buckets = (1u << height) - 1;
    return total_buckets * bucket_size;
}

SlotPayload RingORAM::generate_one_dummy_slot(uint32_t slot_index) {
    uint32_t bucket_idx    = slot_index / bucket_size;
    uint32_t slot_in_bucket = slot_index % bucket_size;

    // On the FIRST slot of each bucket, populate bucket_blockdata for this
    // bucket exactly once. This mirrors the constructor's per-bucket setup:
    //   - blocks_Z of bucket_size slots are marked as "real slot positions"
    //     (Block.ptrs)
    //   - Block.addrs stays empty (no real records live in the tree initially;
    //     all real data lives in stash after bulk_load_stash)
    //   - Block.valids starts all true
    //
    // Silent-mode constructor does NOT run the per-bucket setup loop, so
    // bucket_blockdata[i].valids is empty on first touch here. This mirrors
    // exactly what generate_dummy_tree_payload() does.
    {
        BucketBlock& Block = bucket_blockdata[bucket_idx];
        if (Block.valids.empty()) {
            Block.valids.resize(bucket_size, true);
            std::unordered_set<uint32_t> real_slots;
            while (real_slots.size() < blocks_Z) {
                real_slots.insert(Util::rand_int(bucket_size));
            }
            for (uint32_t slot : real_slots) {
                Block.ptrs.push_back(slot);
            }
            Block.count = 0;
        }
    }

    // Build this slot's encrypted dummy content
    std::string dummy_payload = Util::generate_random_block(
        block_size - Util::aes_block_size - sizeof(uint32_t));
    int32_t dummy_id = -1;
    std::string bid_str(reinterpret_cast<char*>(&dummy_id), sizeof(uint32_t));
    std::string plain = bid_str + dummy_payload;

    SlotPayload sp;
    sp.global_slot_id = bucket_idx * bucket_size * block_size
                      + slot_in_bucket * block_size;
    Util::aes_encrypt(plain, ecrypt_key, sp.cipher);
    return sp;
}