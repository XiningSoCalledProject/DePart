//
// Created by Xining Yuan on 6/11/25.
//

#ifndef SEAL_ORAM_NETIO_RINGORAM_H
#define SEAL_ORAM_NETIO_RINGORAM_H

#endif //SEAL_ORAM_NETIO_RINGORAM_H

#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include "NetIOConnector.h"
#include "Util.h"

struct BucketBlock {
    uint32_t count = 0;
    std::vector<bool> valids;
    std::vector<std::pair <uint32_t, uint32_t>> addrs;                // Address for each of the Z real blocks;
    std::vector<uint32_t> leaves;               // leaf label for each of the Z real blocks;
    std::vector<uint32_t> ptrs;                 // offset in the bucket for each of the Z real blocks;
};

// RingBucket.h
struct RingBucket {
    std::vector<std::string> block;  // Encrypted blocks (Z + S)

    RingBucket() {}

    RingBucket(uint32_t bucket_size) {
        block.resize(bucket_size);  // 初始化 Z + S 个空 block
    }
};

enum class OpType {
    READ,
    UPDATE,
    INSERT
};

class RingORAM {
public:
    ~RingORAM();
    RingORAM(const uint32_t &n, const uint32_t &buck_size,
             const std::string &pathoram_name, const uint32_t &block_length,
             NetIOConnector* ORAMO, const uint32_t &S_input,
             uint32_t permanent_stash_reserve = 0);
             // permanent_stash_reserve: Case B privacy fix — number of real records
             // that permanently live in the stash because the ORAM tree is sized to
             // noisy_synopsis < real_count.  Default 0 = no permanent stash entries
             // (Case A, or any caller that doesn't use the DP partitioning path).

    // core ORAM interface
    std::string access(uint32_t block_id, OpType op, const std::string& data_in);
    void insert(string block, uint32_t block_id);
    void EarlyReshuffle(uint32_t Path_ID);
    void EvictPath();
    std::string prepare_block_data(uint32_t block_id, const std::string& value);
    void WriteBucket(uint32_t bucket_idx);
    void ReadBucket(uint32_t bucket_idx);
    std::string ReadPath(uint32_t PathID, std::pair <uint32_t, uint32_t> block_chunk_id);
    uint32_t get_leaf_from_pos_map(uint32_t block_id);

    // utility interface
    uint32_t reverse_bits(uint32_t val, uint32_t bitlen);
    uint32_t GetNodeOnPath(uint32_t leaf, uint32_t level);
    std::pair<uint32_t, bool> GetBlockOffset(uint32_t bucket_idx, std::pair <uint32_t, uint32_t> block_chunk_id);
    std::string get(const uint32_t & key);

    // optional API
    //void Put(uint32_t block_idx, const std::string& tuple);
    //void put(const std::string &key, const std::string &value);
    uint32_t global_block_counter;
    uint32_t stash_high_watermark = 0;   // tracks peak stash occupancy across the lifetime

    // Post-initialization eviction helper.
    // After inserting all records, the stash may hold more than A entries
    // (especially when stash_overflow records were kept in stash intentionally).
    // Call this once after initialization to drain the stash down to ≤ A entries
    // before the ORAM starts serving live queries.
    void flush_stash_if_needed();

    // Read-only stash size (for external monitoring / assertions).
    uint32_t get_stash_size() const {
        return static_cast<uint32_t>(stash.size());
    }

    // ── Accessors for bulk-init fast path (4/18/26) ────────────────────
    // These expose already-computed fields for use by TPC-C's
    // ServerInitializationBulk path. They do NOT change any algorithm.
    // The insert-only RingORAM path does not call these.
    uint32_t get_height()         const { return height; }
    byte*    get_encryption_key() const { return ecrypt_key; }
    uint32_t get_block_size()     const { return block_size; }
    // ────────────────────────────────────────────────────────────────────

    // ── Bulk-load entry point for TPC-C init (4/20/26) ─────────────────
    //
    // Loads all real records directly into the stash, bypassing the
    // access(INSERT) + eviction loop that ordinarily handles record
    // insertion. This is semantically equivalent but O(N) in CPU instead
    // of O(N * log N * bucket_size) in network RTTs.
    //
    // After calling this, the RingORAM is in a valid state:
    //   - All records live in stash (accessible via access(READ/UPDATE))
    //   - bucket_blockdata has empty .addrs (no real records in tree yet)
    //   - Server side holds only dummy buckets (must be uploaded separately
    //     via conn->bulk_init_tree() — that's ServerInitializationBulk's job)
    //
    // Reads (access READ) work correctly: ReadPath returns dummy encrypted
    // data from tree (won't match target), falls through to stash lookup.
    //
    // `records` is a list of (block_id, plaintext_data) pairs.
    // plaintext_data will be AES-encrypted internally using this ORAM's key.
    void bulk_load_stash(
        const std::vector<std::pair<uint32_t, std::string>>& records);

    // Generate all dummy buckets for the tree as SlotPayload list.
    // Used by ServerInitializationBulk to upload via bulk_init_tree.
    // Same format & randomness as the original constructor's dummy init loop.
    std::vector<SlotPayload> generate_dummy_tree_payload();

    // ── Streaming variants (v4, 4/20/26) — MEMORY-SAFE for big trees ──────
    //
    // Instead of materializing all dummy slots in a vector (2+ GB for ORDER_LINE,
    // causing OOM under 8-table parallel init on 32 GB hosts), these two
    // methods compute exactly ONE slot at a time on demand.
    //
    // Use like:
    //   uint32_t total_slots = oram->get_total_slot_count();
    //   conn->bulk_init_tree_stream(name, block_size, total_slots,
    //       [&](uint32_t i) { return oram->generate_one_dummy_slot(i); });
    //
    // Peak memory ~10 MB (the network chunk buffer), regardless of total slots.
    uint32_t   get_total_slot_count() const;
    SlotPayload generate_one_dummy_slot(uint32_t slot_index);
    // ────────────────────────────────────────────────────────────────────
    // ────────────────────────────────────────────────────────────────────

    std::vector<uint32_t> get_path(uint32_t path_id, uint32_t height);
    std::vector<uint32_t> path_intersection(uint32_t path1, uint32_t path2);
    std::unordered_map<uint32_t, std::string> ConstructBucket(uint32_t bucket_id, std::unordered_map<uint32_t, std::string> input_stash);

private:
    uint32_t height;
    // number of real objects;
    uint32_t n_blocks;
    // number of real slots;
    uint32_t blocks_Z;
    // data block size;
    uint32_t block_size = 64;         // total bucket capacity in bytes
    std::string name;
    uint32_t round;
    // eviction rate;
    uint32_t A;
    uint32_t num_leaves;
    // number of dummy slots in the bucket;
    uint32_t S;                   // reshuffle threshold (initialized in ctor)
    uint32_t G = 0;
    // total slots number in the bucket;
    uint32_t bucket_size;
    byte* ecrypt_key;
    std::string* sbuffer;
    // block_id --> list of chunks_id --> list of pos_id;
    std::map<uint32_t, std::map<uint32_t, uint32_t>> block_chunks_pos;
    // tuple_id --> block_id(s);
    std::unordered_map<uint32_t, uint32_t> tuple_to_blocks;
    NetIOConnector* conn;
    // <block_id, chunk_id> --> encrypted block;
    struct pair_hash {
        template <class T1, class T2>
        std::size_t operator()(const std::pair<T1, T2>& p) const {
            return std::hash<T1>{}(p.first) ^ (std::hash<T2>{}(p.second) << 1);
        }
    };
    std::unordered_map<std::pair<uint32_t, uint32_t>, std::string, pair_hash> stash;
    std::unordered_map<uint32_t, RingBucket> buckets;
    std::unordered_map<uint32_t, BucketBlock> bucket_blockdata;
    uint32_t stash_limit;
    uint32_t permanent_stash_reserve_ = 0;  // Case B: records that permanently live
                                             // in the stash (tree sized to noisy < real)
    void stash_insert(uint32_t block_id, uint32_t chunk_id, const std::string& data);
};