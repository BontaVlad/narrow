# =========================
# Defaults
# =========================

parallel := "false"
cores := "4"
mm := "orc"        # orc | arc
mode := "debug"    # debug | release
coverage := "false"
leaks := "true"    # true | false


# =========================
# Public entry
# =========================

test:
    just _test {{parallel}} {{cores}} {{mm}} {{mode}} {{coverage}} {{leaks}}


# =========================
# Implementation
# =========================

_test parallel cores mm mode coverage leaks:
    #!/usr/bin/env bash
    set -euo pipefail

    PARALLEL="{{parallel}}"
    CORES="{{cores}}"
    MM="{{mm}}"
    MODE="{{mode}}"
    COVERAGE="{{coverage}}"
    LEAKS="{{leaks}}"

    CC=clang
    TEST_ROOT="tests"
    OUT_ROOT="nimcache/tests"

    mkdir -p "$OUT_ROOT"

    mapfile -t TEST_FILES < <(find "$TEST_ROOT" -name 'test_*.nim' | sort)

    if [ "$LEAKS" = "true" ]; then
        ASAN_OPTIONS="detect_leaks=1"
    else
        ASAN_OPTIONS="detect_leaks=0"
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

        if [ "$COVERAGE" = "true" ]; then
            flags+=(
                --passc:-fprofile-instr-generate
                --passc:-fcoverage-mapping
                --passl:-fprofile-instr-generate
            )
        fi

        echo "==> $file"
        nim c \
            "${flags[@]}" \
            -o:"$outdir/$name" \
            "$file"

        ASAN_OPTIONS="$ASAN_OPTIONS" \
        LLVM_PROFILE_FILE="$outdir/$name.profraw" \
            "$outdir/$name"

        if [ "$COVERAGE" = "true" ]; then
            llvm-profdata merge -sparse \
                "$outdir/$name.profraw" \
                -o "$outdir/$name.profdata"

            llvm-cov report \
                "$outdir/$name" \
                -instr-profile="$outdir/$name.profdata" \
                >/dev/null
        fi
    }

    export -f run_test
    export OUT_ROOT MM MODE COVERAGE CC ASAN_OPTIONS

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
    just _test false 4 orc debug false true

test-debug-no-leaks:
    just _test false 4 orc debug false false

test-debug-par:
    just _test true 8 orc debug false true

test-release:
    just _test false 4 orc release false false

test-coverage:
    just _test false 4 orc debug true false

test-arc:
    just _test false 4 arc debug false true

clean:
    rm -rf nimcache
