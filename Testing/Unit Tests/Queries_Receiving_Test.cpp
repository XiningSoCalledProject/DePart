//
// Created by Xining Yuan on 2/24/26.
//
//
// test_queries_receiving.cpp
//
// Standalone test for QueriesReceiving functions.
// Compiles without RingORAM/Repartition dependencies.
//
// Compile:
//   g++ -std=c++17 -pthread -O2 test_queries_receiving.cpp Queries_Receiving.cpp -o test_qr
//
// Run:
//   ./test_qr
//

#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include "Queries_Receiving.h"

// ============================================================================
// Helper: print all ops in the version chain
// ============================================================================

std::string op_type_str(OpType t) {
    switch (t) {
        case OpType::READ:   return "READ";
        case OpType::UPDATE:  return "WRITE";
        case OpType::INSERT: return "INSERT";
        default:             return "UNKNOWN";
    }
}

void print_version_chains(
    const std::map<std::pair<int, uint32_t>, std::vector<Operat>>& vc) {
    std::cout << "\n===== Final Version Chains =====\n";
    if (vc.empty()) {
        std::cout << "  (empty)\n";
        return;
    }
    // ── FIX (4/27/26): VC key is now (server_id, primary_key) ────────────
    // Was: keyed by primary_key alone (uint32_t).
    // The new key prevents cross-partition same-PK collisions.
    for (auto& [vc_key, ops] : vc) {
        int      server_id = vc_key.first;
        uint32_t pk        = vc_key.second;
        std::cout << "\n  Key = (server=" << server_id << ", pk=" << pk
                  << ") (" << ops.size() << " ops):\n";
        for (size_t i = 0; i < ops.size(); i++) {
            auto& op = ops[i];
            std::cout << "    [" << i << "] "
                      << op_type_str(op.type)
                      << "  txn_ts=" << op.txn_timestamp_id
                      << "  server=" << op.server_id
                      << "  value=\"" << op.data_value << "\""
                      << "  last_one=" << op.last_one
                      << "\n";
        }
    }
    std::cout << "================================\n";
}

void print_txns(const std::vector<txn>& txns, const std::string& label) {
    std::cout << "\n--- " << label << " ---\n";
    for (auto& t : txns) {
        std::cout << "  txn ts=" << t.timestamp_id
                  << "  committed=" << t.is_committed
                  << "  ops=" << t.operations.size() << " [";
        for (size_t i = 0; i < t.operations.size(); i++) {
            if (i > 0) std::cout << ", ";
            std::cout << op_type_str(t.operations[i].type)
                      << "(k=" << t.operations[i].data_primary_key << ")";
        }
        std::cout << "]\n";
    }
}

// ============================================================================
// Helper: build an Operat
// ============================================================================

Operat make_op(OpType type, uint32_t key, const std::string& val,
               int server_id = 0, bool last = false) {
    Operat op;
    op.type = type;
    op.data_primary_key = key;
    op.data_value = val;
    op.server_id = server_id;
    op.last_one = last;
    op.read_marker = false;
    op.txn_timestamp_id = 0;  // will be set by VCConstruct
    return op;
}

// ============================================================================
// Test 1: Basic — no conflicts
// ============================================================================

void test_no_conflict() {
    std::cout << "\n########## Test 1: No Conflict ##########\n";

    // txn0 (ts=0): READ key=10
    // txn1 (ts=1): WRITE key=20
    // txn2 (ts=2): READ key=30
    // No shared keys -> no conflict

    std::vector<txn> txns(3);

    txns[0].timestamp_id = 0;
    txns[0].operations = {
        make_op(OpType::READ, 10, "", 0, true)
    };

    txns[1].timestamp_id = 1;
    txns[1].operations = {
        make_op(OpType::UPDATE, 20, "write_20", 0, true)
    };

    txns[2].timestamp_id = 2;
    txns[2].operations = {
        make_op(OpType::READ, 30, "", 0, true)
    };

    print_txns(txns, "Before AssignAbort");

    QueriesReceiving qr;
    qr.AssignAbort(txns);

    print_txns(txns, "After AssignAbort");
    print_version_chains(qr.getVersionChains());

    // All should be committed
    for (auto& t : txns) assert(t.is_committed);
    std::cout << "✅ Test 1 passed\n";
}

// ============================================================================
// Test 2: Rule A — write too late, abort writer
// ============================================================================

void test_write_too_late() {
    std::cout << "\n########## Test 2: Write Too Late (Rule A) ##########\n";

    // On key=100:
    //   txn0 (ts=0): WRITE key=100   <- writer
    //   txn1 (ts=1): READ  key=100   <- reader with later ts
    //
    // Rule A: txn0's WRITE has ts=0 < max_read_ts=1 -> abort txn0

    std::vector<txn> txns(2);

    txns[0].timestamp_id = 0;
    txns[0].operations = {
        make_op(OpType::UPDATE, 100, "v0", 0, true)
    };

    txns[1].timestamp_id = 1;
    txns[1].operations = {
        make_op(OpType::READ, 100, "", 0, true)
    };

    print_txns(txns, "Before AssignAbort");

    QueriesReceiving qr;
    qr.AssignAbort(txns);

    print_txns(txns, "After AssignAbort");
    print_version_chains(qr.getVersionChains());

    assert(txns[0].is_committed == false);  // aborted
    assert(txns[1].is_committed == true);   // OK
    std::cout << "✅ Test 2 passed\n";
}

// ============================================================================
// Test 3: Write after read — no conflict (writer has larger ts)
// ============================================================================

void test_write_after_read_ok() {
    std::cout << "\n########## Test 3: Write After Read (OK) ##########\n";

    // On key=200:
    //   txn0 (ts=0): READ  key=200
    //   txn1 (ts=1): WRITE key=200   <- writer has ts=1 > max_read_ts=0
    //
    // No conflict: writer's ts is NOT less than reader's ts

    std::vector<txn> txns(2);

    txns[0].timestamp_id = 0;
    txns[0].operations = {
        make_op(OpType::READ, 200, "", 0, true)
    };

    txns[1].timestamp_id = 1;
    txns[1].operations = {
        make_op(OpType::UPDATE, 200, "v1", 0, true)
    };

    print_txns(txns, "Before AssignAbort");

    QueriesReceiving qr;
    qr.AssignAbort(txns);

    print_txns(txns, "After AssignAbort");
    print_version_chains(qr.getVersionChains());

    assert(txns[0].is_committed == true);
    assert(txns[1].is_committed == true);
    std::cout << "✅ Test 3 passed\n";
}

// ============================================================================
// Test 4: Incomplete txn (last_one == false) -> abort
// ============================================================================

void test_incomplete_txn() {
    std::cout << "\n########## Test 4: Incomplete Txn ##########\n";

    // txn0: two ops but last_one is never set -> abort
    // txn1: complete (last_one = true) -> committed

    std::vector<txn> txns(2);

    txns[0].timestamp_id = 0;
    txns[0].operations = {
        make_op(OpType::READ,  50, "", 0, false),
        make_op(OpType::UPDATE, 50, "v", 0, false)   // last_one = false!
    };

    txns[1].timestamp_id = 1;
    txns[1].operations = {
        make_op(OpType::READ, 60, "", 0, true)
    };

    print_txns(txns, "Before AssignAbort");

    QueriesReceiving qr;
    qr.AssignAbort(txns);

    print_txns(txns, "After AssignAbort");
    print_version_chains(qr.getVersionChains());

    assert(txns[0].is_committed == false);  // incomplete
    assert(txns[1].is_committed == true);
    std::cout << "✅ Test 4 passed\n";
}

// ============================================================================
// Test 5: Multi-key txn with conflict on one key
// ============================================================================

void test_multi_key_conflict() {
    std::cout << "\n########## Test 5: Multi-Key, Partial Conflict ##########\n";

    // txn0 (ts=0): WRITE key=300, READ key=400
    // txn1 (ts=1): READ  key=300, WRITE key=500
    // txn2 (ts=2): READ  key=400
    //
    // On key=300: txn0 WRITE(ts=0), txn1 READ(ts=1) -> max_read=1 > 0 -> abort txn0
    // On key=400: txn0 READ(ts=0), txn2 READ(ts=2)  -> no writes, no conflict
    // On key=500: txn1 WRITE(ts=1) only             -> no conflict
    //
    // Result: txn0 aborted, txn1 and txn2 committed
    // After dedup: txn0's ops removed from VCs

    std::vector<txn> txns(3);

    txns[0].timestamp_id = 0;
    txns[0].operations = {
        make_op(OpType::UPDATE, 300, "w300", 0, false),
        make_op(OpType::READ,  400, "",     0, true)
    };

    txns[1].timestamp_id = 1;
    txns[1].operations = {
        make_op(OpType::READ,  300, "",     1, false),
        make_op(OpType::UPDATE, 500, "w500", 1, true)
    };

    txns[2].timestamp_id = 2;
    txns[2].operations = {
        make_op(OpType::READ, 400, "", 2, true)
    };

    print_txns(txns, "Before AssignAbort");

    QueriesReceiving qr;
    qr.AssignAbort(txns);

    print_txns(txns, "After AssignAbort");
    print_version_chains(qr.getVersionChains());

    assert(txns[0].is_committed == false);
    assert(txns[1].is_committed == true);
    assert(txns[2].is_committed == true);
    std::cout << "✅ Test 5 passed\n";
}

// ============================================================================
// Test 6: Multiple writers aborted on same key
// ============================================================================

void test_multiple_writers_aborted() {
    std::cout << "\n########## Test 6: Multiple Writers Aborted ##########\n";

    // On key=777:
    //   txn0 (ts=0): WRITE  <- abort (0 < 3)
    //   txn1 (ts=1): WRITE  <- abort (1 < 3)
    //   txn2 (ts=2): WRITE  <- abort (2 < 3)
    //   txn3 (ts=3): READ   <- max_read_ts = 3
    //
    // All three writers have ts < 3 -> all aborted

    std::vector<txn> txns(4);

    txns[0].timestamp_id = 0;
    txns[0].operations = { make_op(OpType::UPDATE, 777, "w0", 0, true) };

    txns[1].timestamp_id = 1;
    txns[1].operations = { make_op(OpType::UPDATE, 777, "w1", 0, true) };

    txns[2].timestamp_id = 2;
    txns[2].operations = { make_op(OpType::UPDATE, 777, "w2", 0, true) };

    txns[3].timestamp_id = 3;
    txns[3].operations = { make_op(OpType::READ, 777, "", 0, true) };

    print_txns(txns, "Before AssignAbort");

    QueriesReceiving qr;
    qr.AssignAbort(txns);

    print_txns(txns, "After AssignAbort");
    print_version_chains(qr.getVersionChains());

    assert(txns[0].is_committed == false);
    assert(txns[1].is_committed == false);
    assert(txns[2].is_committed == false);
    assert(txns[3].is_committed == true);
    std::cout << "✅ Test 6 passed\n";
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "========================================\n";
    std::cout << " QueriesReceiving Test Suite\n";
    std::cout << "========================================\n";

    test_no_conflict();
    test_write_too_late();
    test_write_after_read_ok();
    test_incomplete_txn();
    test_multi_key_conflict();
    test_multiple_writers_aborted();

    std::cout << "\n========================================\n";
    std::cout << " All tests passed! ✅\n";
    std::cout << "========================================\n";

    return 0;
}