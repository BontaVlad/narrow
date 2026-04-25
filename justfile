# =========================
# Defaults
# =========================

parallel := "false"
cores := "4"
mm := "orc"        # orc | arc
mode := "debug"    # debug | release
leaks := "true"    # true | false


# =========================
# Public entry
# =========================


# List all available commands by default
default:
    @just --choose --justfile {{justfile()}}


test:
    just _test {{parallel}} {{cores}} {{mm}} {{mode}} {{leaks}}


# =========================
# Implementation
# =========================

_test parallel cores mm mode leaks:
    #!/usr/bin/env bash
    set -euo pipefail

    PARALLEL="{{parallel}}"
    CORES="{{cores}}"
    MM="{{mm}}"
    MODE="{{mode}}"
    LEAKS="{{leaks}}"

    CC=clang
    TEST_ROOT="tests"
    OUT_ROOT="nimcache/tests"

    mkdir -p "$OUT_ROOT"

    mapfile -t TEST_FILES < <(find "$TEST_ROOT" -name 'test_*.nim' | sort)

    if [ "$LEAKS" = "true" ]; then
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

        flags=(
            --cc:$CC
            --verbosity:0
            --hints:off
            --mm:$MM
            --lineDir:on
            --excessiveStackTrace:on
        )

        if [ "$MODE" = "debug" ]; then
            flags+=(
                -d:debug
                -d:nimDebugDlOpen
                --opt:none
                --stacktrace:on
                --debuginfo:on
                --debugger:native
                -d:useMalloc
                --passc:-O0
                --passc:-g3
                --passc:-fsanitize=address
                --passl:-fsanitize=address
            )
        else
            flags+=(
                -d:release
                --opt:speed
            )
        fi

        echo "==> $file"
        nim c \
            "${flags[@]}" \
            -o:"$outdir/$name" \
            "$file"

        ASAN_OPTIONS="$ASAN_OPTIONS" \
        LSAN_OPTIONS="$LSAN_OPTIONS" \
        LLVM_PROFILE_FILE="$outdir/$name.profraw" \
            "$outdir/$name"

    }

    export -f run_test
    export OUT_ROOT MM MODE CC ASAN_OPTIONS LSAN_OPTIONS

    if [ "$PARALLEL" = "true" ]; then
        printf '%s\n' "${TEST_FILES[@]}" \
            | xargs -P "$CORES" -I {} bash -c 'run_test "$1"' _ {}
    else
        for file in "${TEST_FILES[@]}"; do
            run_test "$file"
        done
    fi


# =========================
# Convenience targets
# =========================

test-debug:
    just _test false 4 orc debug true

test-debug-par:
    just _test true 8 orc debug true

test-release:
    just _test false 4 orc release false

test-arc:
    just _test false 4 arc debug true

coverage-report:
    #!/usr/bin/env bash
    set -euo pipefail

    OUT_ROOT="nimcache/tests"
    COVERAGE_DIR="coverage"
    LCOV_FILE="coverage.info"

    echo "Cleaning previous coverage data..."
    rm -rf "$COVERAGE_DIR"
    find "$OUT_ROOT" -name "*.gcda" -delete 2>/dev/null || true
    rm -f "$LCOV_FILE"

    # Run tests with gcov-based coverage
    echo "Running tests with coverage instrumentation..."
    just _test-gcov

    # Check if we have coverage data
    GCDA_COUNT=$(find "$OUT_ROOT" -name "*.gcda" | wc -l)
    if [ "$GCDA_COUNT" -eq 0 ]; then
        echo "No coverage data (.gcda files) found."
        exit 1
    fi
    echo "Found $GCDA_COUNT .gcda files"

    mkdir -p "$COVERAGE_DIR"

    # Capture coverage data from all cache directories
    echo "Capturing coverage data..."
    lcov --capture \
        --directory "$OUT_ROOT" \
        --output-file "$LCOV_FILE" \
        --rc lcov_branch_coverage=1 \
        --ignore-errors inconsistent,unused,gcov

    # Extract only src/ directory (Nim files)
    echo "Filtering to src/ directory only..."
    lcov --extract "$LCOV_FILE" \
        "*/src/*.nim" \
        --output-file "$LCOV_FILE" \
        --rc lcov_branch_coverage=1 \
        --ignore-errors inconsistent

    # Remove generated code files
    echo "Removing generated code from coverage..."
    lcov --remove "$LCOV_FILE" \
        "*/generated.nim" \
        --output-file "$LCOV_FILE" \
        --rc lcov_branch_coverage=1 \
        --ignore-errors inconsistent

    # Generate HTML report
    echo "Generating HTML report..."
    genhtml "$LCOV_FILE" \
        --output-directory "$COVERAGE_DIR" \
        --branch-coverage \
        --legend \
        --ignore-errors inconsistent,missing,corrupt,range

    # Generate summary
    echo ""
    echo "Coverage Summary:"
    lcov --summary "$LCOV_FILE" --rc lcov_branch_coverage=1 --ignore-errors corrupt,inconsistent 2>&1 | grep -E "(lines|functions|branches)" || true

    echo ""
    echo "Coverage report generated in: $COVERAGE_DIR/index.html"
    echo "Open with: xdg-open $COVERAGE_DIR/index.html  # Linux"
    echo "          open $COVERAGE_DIR/index.html      # macOS"

# Internal target for gcov-based coverage testing
_test-gcov:
    #!/usr/bin/env bash
    set -euo pipefail

    OUT_ROOT="nimcache/tests"
    TEST_ROOT="tests"

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
            --verbosity:0 \
            --hints:off \
            --mm:orc \
            --lineDir:on \
            --debugger:native \
            -d:debug \
            --opt:none \
            --passC:--coverage \
            --passL:--coverage \
            --nimcache:"$cache_dir" \
            -o:"$outdir/$name" \
            "$file"

        echo "==> Running: $name"
        "$outdir/$name" || true
    done

dbg file:
    out="{{file}}"; \
    out=$${out##*/}; \
    out=$${out%.nim}; \
    nim c --cc:clang \
          -g --debuginfo --linedir:on \
          --opt:none \
          --stacktrace:on --linetrace:on \
          --checks:on --assertions:on \
          -d:debug \
          --passC:"-O0 -g3 -fno-omit-frame-pointer" \
          --passL:"-g" \
          "{{file}}" && \
    lldb "./$$out"

benchmark output_dir="":
    #!/usr/bin/env bash
    set -euo pipefail

    BENCH_ROOT="benchmarks"
    OUT_ROOT="nimcache/benchmarks"

    rm -rf "$OUT_ROOT"
    mkdir -p "$OUT_ROOT"

    mapfile -t BENCH_FILES < <(find "$BENCH_ROOT" -name 'bench_*.nim' | sort)

    for file in "${BENCH_FILES[@]}"; do
        name="$(basename "$file" .nim)"
        outdir="$OUT_ROOT/$name"
        mkdir -p "$outdir"

        echo "==> Benchmark: $file"
        nim c \
            --cc:clang \
            --verbosity:0 \
            --hints:off \
            --mm:orc \
            --opt:speed \
            -d:release \
            --passC:-O3 \
            -o:"$outdir/$name" \
            "$file"

        if [ -n "{{output_dir}}" ]; then
            mkdir -p "{{output_dir}}"
            NARROW_BENCH_OUTPUT="{{output_dir}}/$name.json" "$outdir/$name"
        else
            "$outdir/$name"
        fi
        echo ""
    done

benchmark-compare baseline new:
    #!/usr/bin/env python3
    import json, sys
    from pathlib import Path

    baseline_dir = Path("{{baseline}}")
    new_dir = Path("{{new}}")

    if not baseline_dir.is_dir() or not new_dir.is_dir():
        print("Both baseline and new must be directories containing saved benchmark JSON files.")
        sys.exit(1)

    print("{:<40} {:>12} {:>12} {:>10} {:>8}".format(
        "Benchmark", "Baseline", "New", "Delta", "%"
    ))
    print("-" * 86)

    any_regression = False

    for new_file in sorted(new_dir.glob("*.json")):
        base_file = baseline_dir / new_file.name
        if not base_file.exists():
            print("{:<40} {:>12}".format(new_file.stem, "NO BASELINE"))
            continue

        with open(base_file) as f:
            base_data = json.load(f)
        with open(new_file) as f:
            new_data = json.load(f)

        base_map = {r["label"]: r for r in base_data}
        new_map = {r["label"]: r for r in new_data}

        for label in sorted(new_map.keys()):
            if label not in base_map:
                print("{:<40} {:>12}".format(new_file.stem + "/" + label, "NO BASELINE"))
                continue

            base_time = base_map[label]["estimates"]["time"]["mean"]["value"]
            new_time = new_map[label]["estimates"]["time"]["mean"]["value"]

            delta = new_time - base_time
            pct = (delta / base_time) * 100 if base_time != 0 else 0

            if abs(delta) < 0.0001:
                delta_str = "{:+.6f}".format(delta)
            elif abs(delta) < 0.1:
                delta_str = "{:+.6f}".format(delta)
            else:
                delta_str = "{:+.4f}".format(delta)

            pct_str = "{:+.2f}%".format(pct)

            marker = ""
            if pct > 5.0:
                marker = "  REGRESSION"
                any_regression = True
            elif pct < -5.0:
                marker = "  IMPROVEMENT"

            print("{:<40} {:>12.6f} {:>12.6f} {:>10} {:>8}{}".format(
                new_file.stem + "/" + label,
                base_time,
                new_time,
                delta_str,
                pct_str,
                marker
            ))

    if any_regression:
        print("\nWARNING: Regressions detected.")
        sys.exit(1)

# Profile a single benchmark under heaptrack (e.g. just benchmark-heaptrack bench_primitive)
benchmark-heaptrack BENCH:
    #!/usr/bin/env bash
    set -euo pipefail

    BENCH_FILE="benchmarks/{{BENCH}}.nim"
    OUT_ROOT="nimcache/benchmarks"
    PROFILE_DIR="profiles"
    mkdir -p "$PROFILE_DIR"

    if [ ! -f "$BENCH_FILE" ]; then
        echo "Benchmark file not found: $BENCH_FILE"
        exit 1
    fi

    name="{{BENCH}}"
    outdir="$OUT_ROOT/$name"
    mkdir -p "$outdir"

    echo "==> Compiling (with debug info for heaptrack): $BENCH_FILE"
    nim c \
        --cc:clang \
        --verbosity:0 \
        --hints:off \
        --mm:orc \
        --opt:speed \
        -d:release \
        --passC:-O3 \
        --passC:-fno-omit-frame-pointer \
        -g --debuginfo --linedir:on --stacktrace:on --linetrace:on \
        -o:"$outdir/$name" \
        "$BENCH_FILE"

    echo "==> Recording heap profile: $name"
    rm -f "$PROFILE_DIR/${name}.heaptrack"*.zst
    heaptrack -o "$PROFILE_DIR/${name}.heaptrack" "$outdir/$name"

    # Find the generated trace file
    trace_file=$(ls -t "$PROFILE_DIR/${name}.heaptrack"*.zst 2>/dev/null | head -1)

    if [ -n "$trace_file" ]; then
        echo ""
        echo "==> Analyzing heap profile: $trace_file"
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

# Profile all benchmarks under heaptrack
benchmark-heaptrack-all:
    #!/usr/bin/env bash
    set -euo pipefail

    BENCH_ROOT="benchmarks"
    mapfile -t BENCH_FILES < <(find "$BENCH_ROOT" -name 'bench_*.nim' | sort)

    for file in "${BENCH_FILES[@]}"; do
        name="$(basename "$file" .nim)"
        just benchmark-heaptrack "$name"
    done

clean:
    rm -rf nimcache
