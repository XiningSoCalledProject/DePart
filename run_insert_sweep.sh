#!/bin/bash
# =============================================================================
# Insert-only RingORAM benchmark sweep
# =============================================================================
# For each N in the sweep:
#   1. Kill any leftover storage server + Insert_Benchmark processes
#   2. Restart ONE storage server on port 8700 (single insert-only ORAM)
#   3. Run Insert_Benchmark <N> e2e_insert_only.csv 8700
#   4. Move to next N
#
# Usage:
#   ./run_insert_sweep.sh           # all N values, 1 run each
#   ./run_insert_sweep.sh resume    # skip N values already in CSV
#   ./run_insert_sweep.sh 3         # 3 runs per N for averaging
#
# Time estimate (×10 logarithmic sweep):
#   N=50      → <1 sec
#   N=500     → ~1 sec
#   N=5K      → ~30 sec
#   N=50K    → ~5 min
#   N=500K    → ~6-12 hours (drain dominates)
#   Total per run-pass: ~6-12 hours (mostly N=500K)
# =============================================================================

set -u

# ── Paths ──
BUILD_DIR=~/seal-oram-netio-master/build
BIN_DIR="$BUILD_DIR/bin"
CSV_FILE="$BUILD_DIR/e2e_insert_only_log.csv"   # new ×10 logarithmic sweep
SERVER_BIN="$BIN_DIR/Servers_MultiRingORAM"
INSERT_BIN="$BIN_DIR/Insert_Benchmark"
LOG_DIR=~/insert_logs
SERVER_PORT=8700

# ── Sweep values ──
# Logarithmic ×10 spacing (per advisor request)
N_VALUES=(50 500 5000 50000 500000)

# ── Args ──
MODE="${1:-}"
RUNS_PER_N=1
if [ "$MODE" = "resume" ]; then
    RUNS_PER_N=1
elif [[ "$MODE" =~ ^[0-9]+$ ]]; then
    RUNS_PER_N="$MODE"
fi

mkdir -p "$LOG_DIR"

# ── Helper: check if N already in CSV ──
already_have_n() {
    local n="$1"
    [ -f "$CSV_FILE" ] || return 1
    awk -F, -v target="$n" 'NR>1 && $1==target { found=1; exit } END { exit !found }' "$CSV_FILE"
}

# ── Helper: run one N value ──
run_one_n() {
    local n="$1"
    local run_idx="$2"
    local label="N${n}_run${run_idx}"
    local logfile="$LOG_DIR/${label}_$(date +%Y%m%d_%H%M%S).log"

    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  RUNNING: N=${n}  (run ${run_idx}/${RUNS_PER_N})"
    echo "  Started: $(date)"
    echo "  Log:     $logfile"
    echo "═══════════════════════════════════════════════════════════════════"

    # Kill leftovers
    echo "[$(date +%H:%M:%S)] Killing existing processes..."
    killall -9 Insert_Benchmark 2>/dev/null || true
    pkill -9 -f "Servers_MultiRingORAM.*$SERVER_PORT" 2>/dev/null || true
    sleep 10

    # Start single storage server on SERVER_PORT
    echo "[$(date +%H:%M:%S)] Starting storage server on port $SERVER_PORT..."
    local server_log="$LOG_DIR/server_${SERVER_PORT}_${label}.log"
    nohup "$SERVER_BIN" "$SERVER_PORT" > "$server_log" 2>&1 &
    local server_pid=$!
    sleep 3
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "  ❌ Storage server failed to start. Last lines of log:"
        tail -10 "$server_log"
        return 1
    fi
    echo "  ✓ Storage server up (pid $server_pid)"

    # Run benchmark
    echo "[$(date +%H:%M:%S)] Running Insert_Benchmark..."
    cd "$BUILD_DIR"
    if "$INSERT_BIN" "$n" "$CSV_FILE" "$SERVER_PORT" 2>&1 | tee "$logfile"; then
        echo "  ✓ Run completed"
    else
        echo "  ⚠️  Run failed — check $logfile"
        kill -9 "$server_pid" 2>/dev/null || true
        return 1
    fi

    # Cleanup storage server
    kill -9 "$server_pid" 2>/dev/null || true
    sleep 2
    echo "  storage server killed"
    return 0
}

# ── Main sweep ──
echo "═══════════════════════════════════════════════════════════════════"
echo "  Insert-only RingORAM Sweep"
echo "  Started:  $(date)"
echo "  N values: ${N_VALUES[*]}"
echo "  Runs/N:   $RUNS_PER_N"
echo "  Mode:     ${MODE:-(default)}"
echo "  CSV:      $CSV_FILE"
echo "═══════════════════════════════════════════════════════════════════"

# Pre-flight: verify binaries exist
for bin in "$SERVER_BIN" "$INSERT_BIN"; do
    if [ ! -x "$bin" ]; then
        echo "❌ FATAL: binary not found or not executable: $bin"
        echo "    Build first:  cd $BUILD_DIR && make"
        exit 1
    fi
done

TOTAL_RUNS=$(( ${#N_VALUES[@]} * RUNS_PER_N ))
RUN_NUM=0
DONE=0
SKIPPED=0
FAILED=0

for n in "${N_VALUES[@]}"; do
    if [ "$MODE" = "resume" ] && already_have_n "$n"; then
        echo ""
        echo "[$n] SKIPPED (already in CSV)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    for run in $(seq 1 "$RUNS_PER_N"); do
        RUN_NUM=$((RUN_NUM + 1))
        echo ""
        echo "[$RUN_NUM/$TOTAL_RUNS] N=$n run=$run"
        if run_one_n "$n" "$run"; then
            DONE=$((DONE + 1))
        else
            FAILED=$((FAILED + 1))
        fi
    done
done

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Sweep Complete"
echo "  Finished:  $(date)"
echo "  Done:      $DONE"
echo "  Skipped:   $SKIPPED"
echo "  Failed:    $FAILED"
echo "  CSV:       $CSV_FILE"
if [ -f "$CSV_FILE" ]; then
    echo "  CSV rows:  $(tail -n +2 "$CSV_FILE" | wc -l)"
fi
echo "═══════════════════════════════════════════════════════════════════"

# Final cleanup
killall -9 Insert_Benchmark 2>/dev/null || true
pkill -9 -f "Servers_MultiRingORAM.*$SERVER_PORT" 2>/dev/null || true

[ "$FAILED" -eq 0 ] && exit 0 || exit 1
