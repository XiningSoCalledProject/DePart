//
// Created by Xining Yuan on 2/24/26.
//

#include <vector>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include <cstdint>
#include <utility>
#include <set>
#include <algorithm>
#include <iostream>
#include "Queries_Receiving.h"

std::mutex              queue_mu_;
std::vector<txn>        incoming_queue_;   // client handlers push to here;
std::atomic<bool>       accepting_{false}; // check whether is open;

std::vector<txn> QueriesReceiving::collect_epoch(uint64_t interval_ms) {
    // Step 1: clean the queue and open receiving txns;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        incoming_queue_.clear();
    }
    accepting_.store(true);

    // Step 2: Wait until the interval ends;
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

    // Step 3: Close the queue, and get all txns;
    accepting_.store(false);
    std::vector<txn> result;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        result = std::move(incoming_queue_);
    }

    // Step 4: Assign all timestamp ids;
    //
    // ── FIX (4/26/26): also propagate ts to each op + assign global_op_id ─
    // Per-op global_op_id is unique within this epoch.  Used by:
    //   - DedupPlan::per_op_decision (key)
    //   - DedupPlan::oram_bound_ops slot assignment
    //   - Step 5 result assembly
    // Sequential assignment; no thread safety needed here because we
    // already moved off incoming_queue_ under the lock above.
    uint32_t goid = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        result[i].timestamp_id = i;
        for (auto& op : result[i].operations) {
            op.txn_timestamp_id = i;
            op.global_op_id     = goid++;
        }
    }

    return result;
}

void QueriesReceiving::VCConstruct(const txn& current_txn) {
    for (uint32_t i = 0; i < current_txn.operations.size(); ++i) {
        // Note: collect_epoch already set op.txn_timestamp_id and global_op_id;
        // VCConstruct doesn't need to re-tag.  Copy is intentional — the VC
        // owns its own copies decoupled from the source txn vector.
        Operat op = current_txn.operations[i];

        // ── FIX (4/26/26): VC keyed by (server_id, primary_key) ──────────
        // Same primary_key on different partitions does NOT mean the same
        // record (different bins → different ORAMs).  Keying by PK alone
        // would generate spurious MVSTO conflicts across unrelated partitions.
        std::pair<int,uint32_t> vc_key = {op.server_id, op.data_primary_key};
        version_chain_vector[vc_key].push_back(op);
    }
}

void QueriesReceiving::VCAbort(std::set<uint32_t>& aborted_ids) {
    // MVSTO Rule A:
    //   For each key, if a WRITE has txn_timestamp < some READ's txn_timestamp,
    //   the writer came too late (a later txn already read the old version).
    //   -> abort the writer.

    for (auto& [vc_key, vc] : version_chain_vector) {
        (void)vc_key;  // (server_id, pk) — unused inside the body
        // Step 1: Find the max read timestamp on this key
        uint32_t max_read_ts = 0;
        bool has_read = false;
        for (auto& op : vc) {
            if (op.type == OpType::READ) {
                if (!has_read || op.txn_timestamp_id > max_read_ts) {
                    max_read_ts = op.txn_timestamp_id;
                    has_read = true;
                }
            }
        }

        if (!has_read) continue;  // no reads on this key, no conflict possible

        // Step 2: Any WRITE with timestamp < max_read_ts must abort
        for (auto& op : vc) {
            if (op.type == OpType::UPDATE && op.txn_timestamp_id < max_read_ts) {
                aborted_ids.insert(op.txn_timestamp_id);
            }
        }
    }
}

void QueriesReceiving::VCDedup(const std::set<uint32_t>& aborted_ids) {
    // Remove all ops belonging to aborted txns from every version chain
    for (auto& [vc_key, vc] : version_chain_vector) {
        (void)vc_key;
        vc.erase(
            std::remove_if(vc.begin(), vc.end(),
                [&aborted_ids](const Operat& op) {
                    return aborted_ids.count(op.txn_timestamp_id) > 0;
                }),
            vc.end()
        );
    }

    // Remove empty chains
    for (auto it = version_chain_vector.begin(); it != version_chain_vector.end(); ) {
        if (it->second.empty()) {
            it = version_chain_vector.erase(it);
        } else {
            ++it;
        }
    }
}

void QueriesReceiving::AssignAbort(std::vector<txn>& received_txns) {
    // Clear VCs from any previous epoch
    version_chain_vector.clear();

    // ── Diagnostics (v-debug, 4/20/26) ─────────────────────────────────
    // Count WHY each txn gets aborted so we can tell at a glance whether
    // the issue is txn completeness (Case 1) or MVSTO Rule A (Case 2).
    //
    // Case 1 sub-breakdown:
    //   - "empty":        txn arrived at queue with 0 operations
    //   - "no_last":      ops present but none has last_one=true
    //                     (incomplete — builder didn't tag the terminator)
    //
    // Case 2 sub-breakdown (logged per-key, up to a small cap):
    //   - aborted_ids:    set of txn_timestamp_ids that failed Rule A
    //   - hot_keys:       which PKs triggered the rule (and how often)
    //
    // These prints fire once per epoch (i.e. once per Phase-3 config);
    // for a 6-config sweep you'll see 6 lines per run.
    int case1_empty  = 0;
    int case1_nolast = 0;
    int case1_total  = 0;
    for (auto& t : received_txns) {
        if (t.operations.empty()) {
            t.is_committed = false;
            case1_empty++;
            case1_total++;
        } else if (t.operations.back().last_one == false) {
            t.is_committed = false;
            case1_nolast++;
            case1_total++;
        } else {
            // Only build VC for complete txns
            VCConstruct(t);
        }
    }

    // Case 2: MVSTO Rule A — detect write-too-late conflicts
    std::set<uint32_t> aborted_ids;
    VCAbort(aborted_ids);

    // Find the top few keys where Rule A actually fired, to see if there's
    // a hot spot. This is cheap (iterates over VC once more) and harmless.
    // ── FIX (4/26/26): VC is now keyed by (server_id, pk); use pk for
    // hot-key reporting.
    std::map<uint32_t, int> abort_events_per_key;
    for (auto& [vc_key, vc] : version_chain_vector) {
        const uint32_t pk = vc_key.second;  // pk part of (server_id, pk)
        uint32_t max_read_ts = 0;
        bool     has_read    = false;
        for (const auto& op : vc) {
            if (op.type == OpType::READ) {
                if (!has_read || op.txn_timestamp_id > max_read_ts) {
                    max_read_ts = op.txn_timestamp_id;
                    has_read    = true;
                }
            }
        }
        if (!has_read) continue;
        for (const auto& op : vc) {
            if (op.type == OpType::UPDATE && op.txn_timestamp_id < max_read_ts) {
                abort_events_per_key[pk]++;
            }
        }
    }

    for (auto& t : received_txns) {
        if (aborted_ids.count(t.timestamp_id) > 0) {
            t.is_committed = false;
        }
    }

    VCDedup(aborted_ids);

    // ── Print the diagnostic summary ───────────────────────────────────
    int committed = 0;
    for (const auto& t : received_txns) if (t.is_committed) committed++;

    std::cout << "[AssignAbort]"
              << " total=" << received_txns.size()
              << "  committed=" << committed
              << "  Case1_total=" << case1_total
              <<   " (empty=" << case1_empty
              <<    ", no_last_one=" << case1_nolast << ")"
              << "  Case2_MVSTO=" << aborted_ids.size()
              << std::endl;

    // If Case 1 fired, dump one example so we can see what a bad txn looks like
    if (case1_total > 0) {
        for (const auto& t : received_txns) {
            if (t.is_committed) continue;
            std::cout << "  [Case1 example] timestamp_id=" << t.timestamp_id
                      << " ops=" << t.operations.size();
            if (!t.operations.empty()) {
                std::cout << " last.type="
                          << (t.operations.back().type == OpType::READ   ? "READ"   :
                              t.operations.back().type == OpType::UPDATE ? "UPDATE" :
                              t.operations.back().type == OpType::INSERT ? "INSERT" : "?")
                          << " last.last_one="
                          << (t.operations.back().last_one ? "true" : "false")
                          << " last.is_dummy="
                          << (t.operations.back().is_dummy ? "true" : "false");
            }
            std::cout << std::endl;
            break;  // just one example is enough
        }
    }

    // If Case 2 fired, dump the hottest keys
    if (!abort_events_per_key.empty()) {
        // Sort by event count, take top 3
        std::vector<std::pair<uint32_t,int>> sorted_keys(
            abort_events_per_key.begin(), abort_events_per_key.end());
        std::sort(sorted_keys.begin(), sorted_keys.end(),
                  [](auto& a, auto& b){ return a.second > b.second; });
        std::cout << "  [Case2 hot keys] ";
        for (size_t i = 0; i < std::min((size_t)3, sorted_keys.size()); ++i) {
            std::cout << "pk=" << sorted_keys[i].first
                      << "(x" << sorted_keys[i].second << ") ";
        }
        std::cout << std::endl;
    }
}

// ============================================================================
// Producer-side push (4/26/26)
// ============================================================================
// Clients (or the upstream txn ingestor) call this to enqueue a txn into the
// current epoch's queue.  Returns false if not currently accepting (between
// epochs) — caller can buffer locally and retry next epoch.
//
// Locking discipline: this function is the ONLY way for non-internal code
// to touch incoming_queue_ / queue_mu_ / accepting_ (they remain file-scope
// statics in this .cpp), so the mutex correctly serializes all producers
// against the consumer's clear / move-out steps.
// ============================================================================
bool QueriesReceiving::push_txn(txn t) {
    // Quick fence: if not accepting, drop without taking the mutex.
    // There's a TOCTOU race against the consumer flipping accepting_ to
    // false, but it's benign — at worst the txn ends up in the queue
    // just before the consumer's move-out, and gets processed THIS epoch
    // anyway (no data loss).
    if (!accepting_.load(std::memory_order_acquire)) return false;

    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        // Re-check under lock: consumer may have flipped accepting_ to
        // false between our fence-check and the lock acquisition, AND
        // already drained the queue.  In that case our push would land
        // in an EMPTY queue that the consumer just moved out of, and
        // it would be picked up NEXT epoch.  Acceptable.
        incoming_queue_.push_back(std::move(t));
    }
    return true;
}


// ============================================================================
// Step 3: Dedup-to-last + intra-epoch buffer (4/26/26)
// ============================================================================
//
// Walks every version chain (built by AssignAbort + VCConstruct, with
// aborted txns already filtered out by VCDedup).  Per VC:
//
//   1. Find the LAST UPDATE/INSERT (by txn_timestamp_id, then by VC push
//      order which already corresponds to op order within a single txn).
//   2. Walk the VC in push order:
//        - UPDATE/INSERT:
//            - update the "most recent prior write value" buffer for this key
//            - if this op IS the last update    → mark TO_ORAM, assign slot_id
//            - if this op is NOT the last update → mark DROPPED
//        - READ:
//            - if has_buffer (some earlier UPDATE in this VC has run):
//                → mark BUFFERED_FROM_WRITE with buffered_value
//            - else if no preceding ORAM read in THIS VC:
//                → mark TO_ORAM (this is the leader read), assign slot_id
//            - else:
//                → mark COALESCED_READ with leader_slot_id
//
// The slot_id assignment is monotonic = sequential push to oram_bound_ops.
// Step 5 (executeOramOps) returns per_slot_results in the same order.
// ============================================================================
DedupPlan QueriesReceiving::Dedup() {
    DedupPlan plan;

    for (auto& [vc_key, vc] : version_chain_vector) {
        (void)vc_key;

        // ── First pass: find the LAST update on this key ────────────────
        // The "last" op among ones with the highest txn_timestamp_id is
        // determined by VC push order, which (since VCConstruct iterates
        // current_txn.operations in order, and we processed txns in ts
        // order) equals: txn ts first, then op-position-in-txn.
        bool     has_update         = false;
        uint32_t last_update_op_id  = 0;   // global_op_id sentinel
        uint32_t last_update_ts     = 0;
        for (const auto& op : vc) {
            if (op.type == OpType::UPDATE || op.type == OpType::INSERT) {
                if (!has_update ||
                    op.txn_timestamp_id >  last_update_ts ||
                    op.global_op_id     >  last_update_op_id /* tie-break */) {
                    last_update_ts    = op.txn_timestamp_id;
                    last_update_op_id = op.global_op_id;
                    has_update        = true;
                }
            }
        }

        // ── Second pass: assign each op a decision ──────────────────────
        std::string most_recent_write_value;
        bool        has_buffer                 = false;
        bool        leader_read_assigned       = false;
        uint32_t    leader_read_slot_id        = 0;

        for (const auto& op : vc) {
            OpDecision d;

            switch (op.type) {
                case OpType::READ: {
                    if (has_buffer) {
                        // Buffered: result is the most recent prior write's value.
                        d.kind           = OpDecision::Kind::BUFFERED_FROM_WRITE;
                        d.buffered_value = most_recent_write_value;
                    } else if (!leader_read_assigned) {
                        // First read with no preceding write in this VC →
                        // must hit ORAM to materialize the value.
                        d.kind     = OpDecision::Kind::TO_ORAM;
                        d.slot_id  = static_cast<uint32_t>(plan.oram_bound_ops.size());
                        plan.oram_bound_ops.push_back(op);
                        leader_read_slot_id  = d.slot_id;
                        leader_read_assigned = true;
                    } else {
                        // Coalesced: same key, no intervening write → share
                        // the leader read's eventual ORAM result.
                        d.kind            = OpDecision::Kind::COALESCED_READ;
                        d.leader_slot_id  = leader_read_slot_id;
                    }
                    break;
                }

                case OpType::UPDATE:
                case OpType::INSERT: {
                    // Always update the buffered "current epoch value" on
                    // this key — subsequent reads in this VC see this.
                    most_recent_write_value = op.data_value;
                    has_buffer              = true;

                    if (op.global_op_id == last_update_op_id) {
                        d.kind     = OpDecision::Kind::TO_ORAM;
                        d.slot_id  = static_cast<uint32_t>(plan.oram_bound_ops.size());
                        plan.oram_bound_ops.push_back(op);
                    } else {
                        d.kind = OpDecision::Kind::DROPPED;
                    }
                    break;
                }
            }

            plan.per_op_decision[op.global_op_id] = std::move(d);
        }
    }

    // ── Diagnostics ──────────────────────────────────────────────────────
    size_t to_oram = 0, buffered = 0, coalesced = 0, dropped = 0;
    for (const auto& [_id, d] : plan.per_op_decision) {
        switch (d.kind) {
            case OpDecision::Kind::TO_ORAM:             to_oram++;   break;
            case OpDecision::Kind::BUFFERED_FROM_WRITE: buffered++;  break;
            case OpDecision::Kind::COALESCED_READ:      coalesced++; break;
            case OpDecision::Kind::DROPPED:             dropped++;   break;
        }
    }
    std::cout << "[Dedup] total_ops=" << plan.per_op_decision.size()
              << "  to_oram="    << to_oram
              << "  buffered="   << buffered
              << "  coalesced="  << coalesced
              << "  dropped="    << dropped
              << "  (oram_bound_ops.size=" << plan.oram_bound_ops.size() << ")"
              << std::endl;

    return plan;
}


// ============================================================================
// Step 5b: result assembly per txn (4/26/26)
// ============================================================================
std::vector<AssembledTxnResult> assembleResults(
    const std::vector<txn>& txns,
    const DedupPlan&        plan,
    const ExecutionResult&  er)
{
    std::vector<AssembledTxnResult> out;
    out.reserve(txns.size());

    // Hard fail-stop: dispatch failed mid-epoch.  Every committed txn
    // is now in an unknown state, so we must abort them all.  The
    // benchmark / client layer then tells each affected client "abort,
    // please retry".
    const bool global_failure = !er.success;
    if (global_failure) {
        std::cerr << "[assembleResults] WARNING: dispatch failed ("
                  << er.error_message
                  << "); forcing all txns to ABORT.\n";
    }

    size_t commits     = 0;
    size_t aborts      = 0;
    size_t total_reads = 0;
    size_t buffered    = 0;
    size_t coalesced   = 0;

    for (const auto& t : txns) {
        AssembledTxnResult r;
        r.txn_timestamp_id = t.timestamp_id;

        if (!t.is_committed || global_failure) {
            r.committed = false;
            // per_op_result intentionally left empty.
            ++aborts;
            out.push_back(std::move(r));
            continue;
        }

        r.committed = true;
        r.per_op_result.reserve(t.operations.size());

        for (const auto& op : t.operations) {
            auto it = plan.per_op_decision.find(op.global_op_id);
            if (it == plan.per_op_decision.end()) {
                // Op was filtered out by VCDedup (txn was actually aborted)
                // OR something is internally wrong.  Either way, push empty.
                std::cerr << "[assembleResults] WARNING: op global_op_id="
                          << op.global_op_id << " (txn ts="
                          << t.timestamp_id << ", pk=" << op.data_primary_key
                          << ") not in plan.per_op_decision; treating as empty.\n";
                r.per_op_result.emplace_back();
                continue;
            }

            const OpDecision& d = it->second;
            std::string value;

            switch (d.kind) {
                case OpDecision::Kind::TO_ORAM:
                    if (d.slot_id < er.per_slot_results.size())
                        value = er.per_slot_results[d.slot_id];
                    if (op.type == OpType::READ) ++total_reads;
                    break;

                case OpDecision::Kind::BUFFERED_FROM_WRITE:
                    value = d.buffered_value;
                    ++buffered;
                    if (op.type == OpType::READ) ++total_reads;
                    break;

                case OpDecision::Kind::COALESCED_READ:
                    if (d.leader_slot_id < er.per_slot_results.size())
                        value = er.per_slot_results[d.leader_slot_id];
                    ++coalesced;
                    if (op.type == OpType::READ) ++total_reads;
                    break;

                case OpDecision::Kind::DROPPED:
                    // UPDATE that was superseded.  Empty string is fine —
                    // client doesn't expect a return value for UPDATEs.
                    break;
            }

            r.per_op_result.push_back(std::move(value));
        }

        ++commits;
        out.push_back(std::move(r));
    }

    std::cout << "[assembleResults]"
              << " txns="          << txns.size()
              << "  committed="    << commits
              << "  aborted="      << aborts
              << "  reads_total="  << total_reads
              << "  buffered="     << buffered
              << "  coalesced="    << coalesced
              << std::endl;

    return out;
}


// ════════════════════════════════════════════════════════════════════════════
// PAPER-ALIGNED EPOCH ARCHITECTURE — IMPLEMENTATION (4/27/26)
// ════════════════════════════════════════════════════════════════════════════

// VersionCache static empty-string sentinel
const std::string VersionCache::empty_;

// ────────────────────────────────────────────────────────────────────────────
// VersionCache::absorb — pull TO_ORAM read results into cache
// ────────────────────────────────────────────────────────────────────────────
//
// After a sub-epoch's ORAM read batch returns, every TO_ORAM read in
// `plan` corresponds to one slot in `er.per_slot_results`.  We index by
// (server_id, primary_key) so subsequent sub-epochs / phase-B writes can
// see prior values.
//
// Note: plan.oram_bound_ops still has the ORIGINAL primary_key (proxy did
// PK→block_id translation in executeOramOps without mutating the plan
// vector — see executeOramOps comment).  So we can use op.data_primary_key
// directly here.
// ────────────────────────────────────────────────────────────────────────────
void VersionCache::absorb(const DedupPlan& plan, const ExecutionResult& er) {
    if (!er.success) {
        // Don't pollute cache with garbage from a failed dispatch.
        return;
    }

    // Walk plan.oram_bound_ops; each is a TO_ORAM op with a slot_id ==
    // its index in this vector.  Match against er.per_slot_results.
    for (uint32_t slot = 0;
         slot < plan.oram_bound_ops.size() && slot < er.per_slot_results.size();
         ++slot) {
        const Operat& op = plan.oram_bound_ops[slot];
        // We absorb both READ results and TO_ORAM-update results.  The
        // latter is normally an empty string (RingORAM's UPDATE returns
        // ""), but we still mark the key as "touched this epoch" so a
        // later read in this same epoch hits the cache (and gets "" —
        // which is correct: a same-epoch UPDATE's intermediate state
        // should be served from buffer).
        Key k = {op.server_id, op.data_primary_key};
        cache_[k] = er.per_slot_results[slot];
    }
}

// ────────────────────────────────────────────────────────────────────────────
// QueriesReceiving::start_epoch
// ────────────────────────────────────────────────────────────────────────────
void QueriesReceiving::start_epoch() {
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        incoming_queue_.clear();
    }
    accepting_.store(true, std::memory_order_release);

    // Reset epoch-wide id allocators.
    epoch_next_ts_   = 0;
    epoch_next_goid_ = 0;

    // Clear VCs from any previous epoch (Phase B's AssignAbort will rebuild).
    version_chain_vector.clear();

    std::cout << "[start_epoch] intake opened.\n";
}

// ────────────────────────────────────────────────────────────────────────────
// QueriesReceiving::collect_sub_epoch
// ────────────────────────────────────────────────────────────────────────────
//
// Same intake mechanism as collect_epoch but does NOT clear the queue at
// start (txns may have arrived since the last sub-epoch's drain) and does
// NOT toggle `accepting_` to false at end (we want to keep collecting in
// subsequent sub-epochs).
//
// id allocation (txn_timestamp_id, global_op_id) continues monotonically
// across all sub-epochs in this epoch via epoch_next_ts_/epoch_next_goid_.
// ────────────────────────────────────────────────────────────────────────────
std::vector<txn> QueriesReceiving::collect_sub_epoch(uint64_t sub_epoch_ms) {
    // Wait for the sub-epoch's intake window to elapse.
    std::this_thread::sleep_for(std::chrono::milliseconds(sub_epoch_ms));

    std::vector<txn> drained;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        drained = std::move(incoming_queue_);
        incoming_queue_.clear();   // empty the moved-from vector defensively
    }

    // Assign monotonic ids continuing from the previous sub-epoch.
    for (auto& t : drained) {
        t.timestamp_id = epoch_next_ts_++;
        for (auto& op : t.operations) {
            op.txn_timestamp_id = t.timestamp_id;
            op.global_op_id     = epoch_next_goid_++;
        }
    }

    std::cout << "[collect_sub_epoch] window=" << sub_epoch_ms << "ms"
              << "  txns=" << drained.size()
              << "  next_ts="  << epoch_next_ts_
              << "  next_goid=" << epoch_next_goid_
              << std::endl;
    return drained;
}

// ────────────────────────────────────────────────────────────────────────────
// QueriesReceiving::end_intake
// ────────────────────────────────────────────────────────────────────────────
void QueriesReceiving::end_intake() {
    accepting_.store(false, std::memory_order_release);
    std::cout << "[end_intake] intake closed.\n";
}

// ────────────────────────────────────────────────────────────────────────────
// QueriesReceiving::dedupReadsForBatch
// ────────────────────────────────────────────────────────────────────────────
//
// For each READ in `incoming`:
//   - cache hit                                          → BUFFERED_FROM_WRITE
//   - cache miss + first sighting in this sub-epoch       → TO_ORAM (leader)
//   - cache miss + already a leader for same key here     → COALESCED_READ
//
// For each UPDATE/INSERT in `incoming`:
//   - put new value in cache (so later reads same/next sub-epoch see it)
//   - NOT included in plan (writes deferred to Phase B)
//   - NOT recorded in per_op_decision either; its decision will be filled
//     in by Phase B's dedupWritesAtEnd().
// ────────────────────────────────────────────────────────────────────────────
DedupPlan QueriesReceiving::dedupReadsForBatch(
    const std::vector<txn>& incoming,
    VersionCache&           vcache)
{
    DedupPlan plan;

    // Per-key leader tracking *within this sub-epoch*.  Reset each call.
    // (Cross-sub-epoch leadership doesn't make sense — different ORAM batches.)
    std::map<VersionCache::Key, uint32_t> leader_slot_by_key;

    for (const auto& t : incoming) {
        for (const auto& op : t.operations) {
            VersionCache::Key k = {op.server_id, op.data_primary_key};

            switch (op.type) {
                case OpType::READ: {
                    OpDecision d;
                    if (vcache.contains(k)) {
                        d.kind           = OpDecision::Kind::BUFFERED_FROM_WRITE;
                        d.buffered_value = vcache.lookup(k);
                    } else {
                        auto lit = leader_slot_by_key.find(k);
                        if (lit == leader_slot_by_key.end()) {
                            d.kind     = OpDecision::Kind::TO_ORAM;
                            d.slot_id  = static_cast<uint32_t>(plan.oram_bound_ops.size());
                            plan.oram_bound_ops.push_back(op);
                            leader_slot_by_key[k] = d.slot_id;
                        } else {
                            d.kind            = OpDecision::Kind::COALESCED_READ;
                            d.leader_slot_id  = lit->second;
                        }
                    }
                    plan.per_op_decision[op.global_op_id] = std::move(d);
                    break;
                }

                case OpType::UPDATE:
                case OpType::INSERT: {
                    // Phase A: buffer the write so subsequent reads see it.
                    // Don't record a decision here — Phase B will do it.
                    vcache.put(k, op.data_value);
                    break;
                }
            }
        }
    }

    // Diagnostics
    size_t to_oram = 0, buffered = 0, coalesced = 0;
    for (const auto& [_id, d] : plan.per_op_decision) {
        switch (d.kind) {
            case OpDecision::Kind::TO_ORAM:             to_oram++;   break;
            case OpDecision::Kind::BUFFERED_FROM_WRITE: buffered++;  break;
            case OpDecision::Kind::COALESCED_READ:      coalesced++; break;
            case OpDecision::Kind::DROPPED:             /* impossible here */ break;
        }
    }
    std::cout << "[dedupReadsForBatch]"
              << " incoming_txns=" << incoming.size()
              << "  to_oram="      << to_oram
              << "  buffered="     << buffered
              << "  coalesced="    << coalesced
              << "  oram_bound="   << plan.oram_bound_ops.size()
              << "  vcache_size="  << vcache.size()
              << std::endl;

    return plan;
}

// ────────────────────────────────────────────────────────────────────────────
// QueriesReceiving::dedupWritesAtEnd
// ────────────────────────────────────────────────────────────────────────────
//
// Walk all_txns in order; for each UPDATE/INSERT op of a COMMITTED txn,
// remember it as "the latest write on this key so far".  At the end, the
// remembered op for each key is TO_ORAM; all earlier same-key ops are DROPPED.
//
// Aborted txns' writes are skipped entirely (no decision recorded — they
// were never going to materialize).  Their READs may still need decisions,
// but READs were handled by Phase A (per_op_decision in those plans).
//
// The vcache parameter is unused here (kept for symmetry / future use); we
// could in principle compare cache state vs. last-write to sanity-check.
// ────────────────────────────────────────────────────────────────────────────
DedupPlan QueriesReceiving::dedupWritesAtEnd(
    const std::vector<txn>& all_txns,
    VersionCache&           vcache)
{
    (void)vcache;  // reserved for future sanity checks

    DedupPlan plan;

    // For each key, track (global_op_id of last write, the op itself).
    struct LastWrite {
        uint32_t      global_op_id;
        const Operat* op;
    };
    std::map<VersionCache::Key, LastWrite> last_write_by_key;

    // Pass 1: scan committed txns, find last write per key.
    // Walk in txn order then op-position-in-txn; the comparison key for
    // "last" is global_op_id (which IS monotonic in walk order, so the
    // last assignment naturally wins).
    for (const auto& t : all_txns) {
        if (!t.is_committed) continue;
        for (const auto& op : t.operations) {
            if (op.type == OpType::UPDATE || op.type == OpType::INSERT) {
                VersionCache::Key k = {op.server_id, op.data_primary_key};
                last_write_by_key[k] = {op.global_op_id, &op};
            }
        }
    }

    // Pass 2: assign decisions to ALL writes (committed txns only).
    // Latest one per key → TO_ORAM (and gets a slot).  Earlier → DROPPED.
    for (const auto& t : all_txns) {
        if (!t.is_committed) continue;
        for (const auto& op : t.operations) {
            if (op.type != OpType::UPDATE && op.type != OpType::INSERT) continue;

            VersionCache::Key k = {op.server_id, op.data_primary_key};
            auto it = last_write_by_key.find(k);
            // it must exist because Pass 1 just inserted it.

            OpDecision d;
            if (it->second.global_op_id == op.global_op_id) {
                d.kind     = OpDecision::Kind::TO_ORAM;
                d.slot_id  = static_cast<uint32_t>(plan.oram_bound_ops.size());
                plan.oram_bound_ops.push_back(op);
            } else {
                d.kind = OpDecision::Kind::DROPPED;
            }
            plan.per_op_decision[op.global_op_id] = std::move(d);
        }
    }

    // Diagnostics
    size_t to_oram = 0, dropped = 0;
    for (const auto& [_id, d] : plan.per_op_decision) {
        if (d.kind == OpDecision::Kind::TO_ORAM)  to_oram++;
        if (d.kind == OpDecision::Kind::DROPPED)  dropped++;
    }
    std::cout << "[dedupWritesAtEnd]"
              << " total_committed_writes=" << plan.per_op_decision.size()
              << "  to_oram="                << to_oram
              << "  dropped="                << dropped
              << std::endl;

    return plan;
}

// ────────────────────────────────────────────────────────────────────────────
// assembleResultsRBRW — combined assembler for the new path
// ────────────────────────────────────────────────────────────────────────────
//
// For each txn's ops, look up the decision in:
//   - read_results[*].plan.per_op_decision  for READ ops
//   - write_result.plan.per_op_decision     for UPDATE/INSERT ops
// then resolve to plaintext from the matching ExecutionResult.
//
// Builds a global lookup map first (op.global_op_id → (which er, slot_id))
// to keep the inner loop O(1) per op.
// ────────────────────────────────────────────────────────────────────────────
std::vector<AssembledTxnResult> assembleResultsRBRW(
    const std::vector<txn>&              txns,
    const std::vector<PhaseAReadResult>& read_results,
    const PhaseBWriteResult&             write_result)
{
    std::vector<AssembledTxnResult> out;
    out.reserve(txns.size());

    // Detect global failure: any phase failure forces all-abort
    bool global_failure = !write_result.er.success;
    for (const auto& rr : read_results) {
        if (!rr.er.success) { global_failure = true; break; }
    }
    if (global_failure) {
        std::cerr << "[assembleResultsRBRW] WARNING: at least one phase "
                     "failed; forcing all txns to ABORT.\n";
    }

    // Build a per_op_decision index: global_op_id → (er pointer, decision pointer)
    // Read decisions might appear in any of read_results[r] (one per sub-epoch);
    // we walk them all.
    struct DecisionRef {
        const ExecutionResult* er;       // which ExecutionResult to query
        const OpDecision*      d;        // decision (TO_ORAM/BUFFERED/COALESCED/DROPPED)
    };
    std::map<uint32_t, DecisionRef> goid_to_decision;

    for (const auto& rr : read_results) {
        for (const auto& [goid, dec] : rr.plan.per_op_decision) {
            goid_to_decision[goid] = {&rr.er, &dec};
        }
    }
    for (const auto& [goid, dec] : write_result.plan.per_op_decision) {
        goid_to_decision[goid] = {&write_result.er, &dec};
    }

    size_t commits = 0, aborts = 0, missing = 0;

    for (const auto& t : txns) {
        AssembledTxnResult r;
        r.txn_timestamp_id = t.timestamp_id;

        if (!t.is_committed || global_failure) {
            r.committed = false;
            ++aborts;
            out.push_back(std::move(r));
            continue;
        }

        r.committed = true;
        r.per_op_result.reserve(t.operations.size());

        for (const auto& op : t.operations) {
            auto it = goid_to_decision.find(op.global_op_id);
            if (it == goid_to_decision.end()) {
                // Op had no decision recorded.  This SHOULDN'T happen for
                // committed txns — every op should have been classified
                // by either Phase A (READ) or Phase B (WRITE).  Likely a
                // bug; emit empty.
                std::cerr << "[assembleResultsRBRW] WARNING: op global_op_id="
                          << op.global_op_id << " (txn ts=" << t.timestamp_id
                          << ") has no decision; emitting empty.\n";
                r.per_op_result.emplace_back();
                ++missing;
                continue;
            }

            const ExecutionResult* er = it->second.er;
            const OpDecision&      d  = *it->second.d;
            std::string value;

            switch (d.kind) {
                case OpDecision::Kind::TO_ORAM:
                    if (d.slot_id < er->per_slot_results.size())
                        value = er->per_slot_results[d.slot_id];
                    break;
                case OpDecision::Kind::BUFFERED_FROM_WRITE:
                    value = d.buffered_value;
                    break;
                case OpDecision::Kind::COALESCED_READ:
                    if (d.leader_slot_id < er->per_slot_results.size())
                        value = er->per_slot_results[d.leader_slot_id];
                    break;
                case OpDecision::Kind::DROPPED:
                    // UPDATE that was superseded.  Empty result is correct.
                    break;
            }

            r.per_op_result.push_back(std::move(value));
        }

        ++commits;
        out.push_back(std::move(r));
    }

    std::cout << "[assembleResultsRBRW]"
              << " txns="          << txns.size()
              << "  committed="    << commits
              << "  aborted="      << aborts
              << "  missing_dec="  << missing
              << std::endl;

    return out;
}

// ────────────────────────────────────────────────────────────────────────────
// runEpoch_RBR_W — paper-aligned epoch driver
// ────────────────────────────────────────────────────────────────────────────
std::vector<AssembledTxnResult> runEpoch_RBR_W(
    QueriesReceiving&        qr,
    MultiRingORAM_Servers&   mrs,
    const EpochConfig&       cfg)
{
    auto t_start = std::chrono::high_resolution_clock::now();

    qr.start_epoch();
    VersionCache vcache;

    std::vector<txn>              all_txns;
    std::vector<PhaseAReadResult> read_results;
    read_results.reserve(cfg.R);

    // ── Phase A: R sub-epochs of reads ──────────────────────────────────
    for (uint32_t r = 0; r < cfg.R; ++r) {
        std::cout << "─── sub-epoch " << (r+1) << "/" << cfg.R << " ───\n";

        auto incoming = qr.collect_sub_epoch(cfg.sub_epoch_ms);

        // Run dedupReadsForBatch with the JUST-COLLECTED txns.  This
        // produces a plan whose oram_bound_ops are the reads to dispatch.
        // Note we pass `incoming` (not a class member) because the
        // intra-epoch UPDATE buffering happens in vcache, not in VCs.
        DedupPlan plan = qr.dedupReadsForBatch(incoming, vcache);

        // Dispatch (size = bread, padded if cfg.pad_batches).
        ExecutionResult er = mrs.executeOramOps(
            plan.oram_bound_ops, cfg.bread, cfg.pad_batches);

        // Absorb into cache so subsequent sub-epochs see read results.
        vcache.absorb(plan, er);

        // Stash for assembly.
        PhaseAReadResult pa;
        pa.plan = std::move(plan);
        pa.er   = std::move(er);
        read_results.push_back(std::move(pa));

        // Append incoming to the epoch-wide list for AssignAbort + write phase.
        for (auto& t : incoming) all_txns.push_back(std::move(t));
    }

    // ── Phase B: close intake, MVTSO, write batch ───────────────────────
    qr.end_intake();

    // AssignAbort sets is_committed on every txn.  This MUST happen
    // before dedupWritesAtEnd (which skips aborted txns' writes).
    qr.AssignAbort(all_txns);

    DedupPlan       wp = qr.dedupWritesAtEnd(all_txns, vcache);
    ExecutionResult ew = mrs.executeOramOps(
        wp.oram_bound_ops, cfg.bwrite, cfg.pad_batches);

    PhaseBWriteResult phase_b;
    phase_b.plan = std::move(wp);
    phase_b.er   = std::move(ew);

    // ── Phase 5b: assemble per-txn results ──────────────────────────────
    auto results = assembleResultsRBRW(all_txns, read_results, phase_b);

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(t_end - t_start).count();

    std::cout << "[runEpoch_RBR_W] DONE"
              << "  R="          << cfg.R
              << "  bread="      << cfg.bread
              << "  bwrite="     << cfg.bwrite
              << "  total_txns=" << all_txns.size()
              << "  elapsed_ms=" << elapsed_ms
              << std::endl;

    return results;
}