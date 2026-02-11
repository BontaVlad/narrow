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
            | xargs -n 1 -P "$CORES" -I {} bash -c 'run_test "$1"' _ {}
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

clean:
    rm -rf nimcache
