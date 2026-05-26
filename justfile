# =============================================================================
# Configuration
# =============================================================================

# Directories
test_root    := "tests"
bench_root   := "benchmarks"
out_root     := "nimcache"
profile_dir  := "profiles"
coverage_dir := "coverage"

# Test defaults
parallel := "false"
cores    := "4"
mm       := "orc"      # orc | arc
mode     := "debug"    # debug | release
leaks    := "true"     # true | false

# Compiler
cc := "clang"

# =============================================================================
# Flag sets (shell arrays, paste into recipes)
# =============================================================================

_base_flags := "--verbosity:0 --hints:off --lineDir:on"

_debug_flags := "-d:debug -d:nimDebugDlOpen --opt:none --stacktrace:on --debuginfo:on --debugger:native -d:useMalloc --passC:-O0 --passC:-g3"

_sanitizer_flags := "--passC:-fsanitize=address --passL:-fsanitize=address"

_release_flags := "-d:release --opt:speed"

# Debug symbols + frame pointers for profiling builds
_profile_flags := "--passC:-O2 --passC:-g --passC:-fno-omit-frame-pointer --passL:-g -g --debuginfo --stacktrace:on --linetrace:on"

# =============================================================================
# Public targets
# =============================================================================

# List all available commands
default:
    @just --choose --justfile {{justfile()}}

# Run tests with current settings
test: (_run-tests parallel cores mm mode leaks)

# Convenience presets
test-debug: (_run-tests "false" "4" "orc" "debug" "true")
test-debug-par: (_run-tests "true" "8" "orc" "debug" "true")
test-release: (_run-tests "false" "4" "orc" "release" "false")
test-arc: (_run-tests "false" "4" "arc" "debug" "true")

# Build and debug a single file with lldb
dbg file:
    #!/usr/bin/env bash
    set -euo pipefail

    name="$(basename "{{file}}" .nim)"
    outdir="{{out_root}}/dbg"
    mkdir -p "$outdir"

    nim c \
        {{_base_flags}} \
        --cc:{{cc}} \
        --mm:{{mm}} \
        {{_debug_flags}} \
        --excessiveStackTrace:on \
        -o:"$outdir/$name" \
        "{{file}}"

    lldb "$outdir/$name"

# Run benchmarks. Examples:
#   just benchmark
#   just benchmark benchmarks/bench_foo.nim
#   just benchmark benchmarks/bench_foo.nim -o results/
#   just benchmark /any/path/to/file.nim
#   just benchmark /any/path/to/dir
benchmark *args="":
    #!/usr/bin/env bash
    set -euo pipefail

    OUT_ROOT="{{out_root}}/benchmarks"
    DEFAULT_DIR="benchmarks/results/$(date +%Y%m%d_%H%M%S)"

    set -- {{args}}

    # Parse arguments
    TARGET=""
    OUTPUT_ARG=""
    while [[ $# -gt 0 ]]; do
        case $1 in
            -o|--output) OUTPUT_ARG="$2"; shift 2 ;;
            *)           TARGET="$1"; shift ;;
        esac
    done

    # Resolve files and output location
    SINGLE_OUTPUT=""
    SAVE_DIR=""

    if [[ -z "$TARGET" ]]; then
        # Default: run all bench_*.nim in benchmarks/ (backward compat)
        mapfile -t BENCH_FILES < <(find "{{bench_root}}" -name 'bench_*.nim' | sort)
        SAVE_DIR="${OUTPUT_ARG:-$DEFAULT_DIR}"
    elif [[ -f "$TARGET" && "$TARGET" == *.nim ]]; then
        # Any .nim file
        BENCH_FILES=("$TARGET")
        if [[ -n "$OUTPUT_ARG" && "$OUTPUT_ARG" == *.json ]]; then
            SAVE_DIR="$(dirname "$OUTPUT_ARG")"
            SINGLE_OUTPUT="$(basename "$OUTPUT_ARG")"
        else
            SAVE_DIR="${OUTPUT_ARG:-$DEFAULT_DIR}"
        fi
    elif [[ -d "$TARGET" ]]; then
        # Any .nim files in the directory
        mapfile -t BENCH_FILES < <(find "$TARGET" -maxdepth 1 -name '*.nim' | sort)
        if [[ ${#BENCH_FILES[@]} -eq 0 ]]; then
            echo "ERROR: No *.nim files found in directory: $TARGET"
            exit 1
        fi
        SAVE_DIR="${OUTPUT_ARG:-$DEFAULT_DIR}"
    else
        echo "ERROR: Target not found: $TARGET"
        echo "Usage: just benchmark [path/to/file.nim|path/to/dir] [-o output]"
        exit 1
    fi

    rm -rf "$OUT_ROOT"
    mkdir -p "$OUT_ROOT" "$SAVE_DIR"

    for file in "${BENCH_FILES[@]}"; do
        name="$(basename "$file" .nim)"
        outdir="$OUT_ROOT/$name"
        mkdir -p "$outdir"

        echo "==> Benchmark: $file"
        nim c \
            {{_base_flags}} \
            --cc:clang \
            --mm:orc \
            {{_release_flags}} \
            -d:useMalloc \
            {{_profile_flags}} \
            -o:"$outdir/$name" \
            "$file"

        if [[ -n "$SINGLE_OUTPUT" ]]; then
            NARROW_BENCH_OUTPUT="$SAVE_DIR/$SINGLE_OUTPUT" "$outdir/$name"
        else
            NARROW_BENCH_OUTPUT="$SAVE_DIR/$name.json" "$outdir/$name"
        fi
        echo ""
    done

    echo "Results saved to: $SAVE_DIR"

# Compare two benchmark result sets
benchmark-compare baseline new:
    python3 {{bench_root}}/compare.py "{{baseline}}" "{{new}}" --sort delta

# Profile a benchmark with heaptrack
benchmark-heaptrack bench gui="false":
    #!/usr/bin/env bash
    set -euo pipefail

    raw="{{bench}}"
    raw="${raw%.nim}"

    if [[ "$raw" == */* ]]; then
        BENCH_FILE="${raw}.nim"
        name="$(basename "$raw")"
    else
        BENCH_FILE="{{bench_root}}/${raw}.nim"
        name="$raw"
    fi

    OUT_ROOT="{{out_root}}/benchmarks"
    PROF_DIR="{{profile_dir}}"

    if [[ ! -f "$BENCH_FILE" ]]; then
        echo "ERROR: Benchmark file not found: $BENCH_FILE"; exit 1
    fi

    outdir="$OUT_ROOT/$name"
    mkdir -p "$outdir" "$PROF_DIR"

    echo "==> Compiling: $BENCH_FILE"
    nim c \
        {{_base_flags}} \
        --cc:clang \
        --mm:orc \
        {{_release_flags}} \
        -d:useMalloc \
        -d:heaptrack \
        {{_profile_flags}} \
        -o:"$outdir/$name" \
        "$BENCH_FILE"

    echo "==> Recording heap profile: $name"
    rm -f "$PROF_DIR/${name}.heaptrack"*.zst

    set +e
    LD_LIBRARY_PATH="/usr/lib/heaptrack${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        heaptrack --record-only -o "$PROF_DIR/${name}.heaptrack" "$outdir/$name"
    bench_exit=$?
    set -e

    [[ $bench_exit -ne 0 ]] && echo "WARNING: benchmark exited with code $bench_exit"

    trace_file=$(ls -t "$PROF_DIR/${name}.heaptrack"*.zst 2>/dev/null | head -1)
    if [[ -z "$trace_file" ]]; then
        echo "ERROR: No heaptrack trace file found in $PROF_DIR"; exit 1
    fi

    echo "==> Analyzing: $trace_file"
    if [[ "{{gui}}" == "true" || "{{gui}}" == "gui=true" ]] && command -v heaptrack_gui >/dev/null 2>&1; then
        heaptrack_gui "$trace_file"
    else
        heaptrack_print \
            --shorten-templates \
            --print-peaks \
            --print-allocators \
            --print-leaks \
            --peak-limit 10 \
            -f "$trace_file"
    fi

    echo ""
    echo "Profile trace: $trace_file"
    echo "Full analysis: heaptrack_print -f $trace_file"

# Profile all benchmarks with heaptrack
benchmark-heaptrack-all gui="false":
    #!/usr/bin/env bash
    set -euo pipefail
    for file in $(find "{{bench_root}}" -name 'bench_*.nim' | sort); do
        name="$(basename "$file" .nim)"
        just benchmark-heaptrack "$name" "{{gui}}"
    done

# Profile a benchmark with perf
perf bench:
    #!/usr/bin/env bash
    set -euo pipefail

    raw="{{bench}}"
    raw="${raw%.nim}"

    if [[ "$raw" == */* ]]; then
        BENCH_FILE="${raw}.nim"
        name="$(basename "$raw")"
    else
        BENCH_FILE="{{bench_root}}/${raw}.nim"
        name="$raw"
    fi

    OUT_ROOT="{{out_root}}/benchmarks"
    PROF_DIR="{{profile_dir}}"

    if [[ ! -f "$BENCH_FILE" ]]; then
        echo "ERROR: Benchmark file not found: $BENCH_FILE"; exit 1
    fi

    outdir="$OUT_ROOT/$name"
    mkdir -p "$outdir" "$PROF_DIR"

    echo "==> Compiling: $BENCH_FILE"
    nim c \
        {{_base_flags}} \
        --cc:clang \
        --mm:orc \
        {{_release_flags}} \
        -d:useMalloc \
        {{_profile_flags}} \
        -o:"$outdir/$name" \
        "$BENCH_FILE"

    echo "==> Recording perf profile: $name"
    rm -f "$PROF_DIR/${name}.perf.data"

    DEBUGINFO_URLS="https://debuginfo.archlinux.org" \
        perf record --call-graph dwarf \
            -o "$PROF_DIR/${name}.perf.data" \
            "$outdir/$name"

    echo ""
    echo "Profile data: $PROF_DIR/${name}.perf.data"
    echo "==> Opening perf report"
    perf report -i "$PROF_DIR/${name}.perf.data"

# Generate lcov coverage report
coverage-report: _run-tests-gcov
    #!/usr/bin/env bash
    set -euo pipefail

    OUT_ROOT="{{out_root}}/tests"
    COV_DIR="{{coverage_dir}}"
    LCOV_FILE="coverage.info"

    GCDA_COUNT=$(find "$OUT_ROOT" -name "*.gcda" | wc -l)
    if [[ "$GCDA_COUNT" -eq 0 ]]; then
        echo "No coverage data (.gcda files) found."; exit 1
    fi
    echo "Found $GCDA_COUNT .gcda files"

    rm -rf "$COV_DIR"
    mkdir -p "$COV_DIR"

    lcov --capture \
        --directory "$OUT_ROOT" \
        --output-file "$LCOV_FILE" \
        --rc lcov_branch_coverage=1 \
        --ignore-errors inconsistent,unused,gcov

    lcov --extract "$LCOV_FILE" "*/src/*.nim" \
        --output-file "$LCOV_FILE" \
        --rc lcov_branch_coverage=1 \
        --ignore-errors inconsistent

    lcov --remove "$LCOV_FILE" "*/generated.nim" \
        --output-file "$LCOV_FILE" \
        --rc lcov_branch_coverage=1 \
        --ignore-errors inconsistent

    genhtml "$LCOV_FILE" \
        --output-directory "$COV_DIR" \
        --branch-coverage --legend \
        --ignore-errors inconsistent,missing,corrupt,range

    echo ""
    echo "Coverage Summary:"
    lcov --summary "$LCOV_FILE" --rc lcov_branch_coverage=1 \
        --ignore-errors corrupt,inconsistent 2>&1 \
        | grep -E "(lines|functions|branches)" || true
    echo ""
    echo "Report: $COV_DIR/index.html"

# Remove all build artifacts
clean:
    rm -rf {{out_root}} {{coverage_dir}} coverage.info {{profile_dir}}

# =============================================================================
# Internal targets
# =============================================================================

_run-tests parallel cores mm mode leaks:
    #!/usr/bin/env bash
    set -euo pipefail

    TEST_ROOT="{{test_root}}"
    OUT_ROOT="{{out_root}}/tests"
    PARALLEL="{{parallel}}"
    CORES="{{cores}}"
    MM="{{mm}}"
    MODE="{{mode}}"
    CC="{{cc}}"

    BASE_FLAGS="{{_base_flags}}"
    DEBUG_FLAGS="{{_debug_flags}}"
    SANITIZER_FLAGS="{{_sanitizer_flags}}"
    RELEASE_FLAGS="{{_release_flags}}"

    mkdir -p "$OUT_ROOT"
    mapfile -t TEST_FILES < <(find "$TEST_ROOT" -name 'test_*.nim' | sort)

    if [[ "{{leaks}}" == "true" ]]; then
        ASAN_OPTIONS="detect_leaks=1"
        LSAN_OPTIONS="suppressions=lsan.supp:print_suppressions=0"
    else
        ASAN_OPTIONS="detect_leaks=0"
        LSAN_OPTIONS=""
    fi

    run_test() {
        local file="$1"
        local name outdir
        name="$(basename "$file" .nim)"
        outdir="$OUT_ROOT/$name"
        mkdir -p "$outdir"

        local flags="$BASE_FLAGS --cc:$CC --mm:$MM --excessiveStackTrace:on"

        if [[ "$MODE" == "debug" ]]; then
            flags="$flags $DEBUG_FLAGS $SANITIZER_FLAGS"
        else
            flags="$flags $RELEASE_FLAGS"
        fi

        echo "==> $file"
        eval nim c $flags -o:"$outdir/$name" "$file"

        ASAN_OPTIONS="$ASAN_OPTIONS" \
        LSAN_OPTIONS="$LSAN_OPTIONS" \
        LLVM_PROFILE_FILE="$outdir/$name.profraw" \
            "$outdir/$name"
    }

    export -f run_test
    export OUT_ROOT MM MODE CC ASAN_OPTIONS LSAN_OPTIONS
    export BASE_FLAGS DEBUG_FLAGS SANITIZER_FLAGS RELEASE_FLAGS

    if [[ "$PARALLEL" == "true" ]]; then
        printf '%s\n' "${TEST_FILES[@]}" \
            | xargs -P "$CORES" -I {} bash -c 'run_test "$1"' _ {}
    else
        for file in "${TEST_FILES[@]}"; do
            run_test "$file"
        done
    fi

_run-tests-gcov:
    #!/usr/bin/env bash
    set -euo pipefail

    TEST_ROOT="{{test_root}}"
    OUT_ROOT="{{out_root}}/tests"

    # Clean old gcov data before running
    find "$OUT_ROOT" -name "*.gcda" -delete 2>/dev/null || true
    rm -f coverage.info

    mkdir -p "$OUT_ROOT"
    mapfile -t TEST_FILES < <(find "$TEST_ROOT" -name 'test_*.nim' | sort)

    for file in "${TEST_FILES[@]}"; do
        name="$(basename "$file" .nim)"
        outdir="$OUT_ROOT/$name"
        cache_dir="$outdir/cache"
        mkdir -p "$outdir"

        echo "==> Compiling: $file"
        nim c \
            --cc:gcc \
            {{_base_flags}} \
            --mm:orc \
            --debugger:native \
            -d:debug --opt:none \
            --passC:--coverage --passL:--coverage \
            --nimcache:"$cache_dir" \
            -o:"$outdir/$name" \
            "$file"

        echo "==> Running: $name"
        "$outdir/$name" || true
    done
