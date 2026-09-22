#!/usr/bin/env bash
# Copyright 2026 Mert Akin
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

source "$ROOT/scripts/env.sh"

readonly RNASEQ_REVISION="e7ca46272c8f9d5ceee3f71759f4ba551d3217a4"
readonly OUT_ROOT="$ROOT/.ci/rnaseq-acceptance"

need_command() {
    command -v "$1" >/dev/null 2>&1 || {
        printf 'Required command not found: %s\n' "$1" >&2
        exit 1
    }
}

need_command docker
need_command python3

docker info >/dev/null

plugin_version="$(sed -n "s/^version = '\([^']*\)'$/\1/p" build.gradle)"
if [[ -z "$plugin_version" ]]; then
    echo "Could not read plugin version from build.gradle" >&2
    exit 1
fi

rm -rf "$OUT_ROOT"
mkdir -p "$OUT_ROOT"

write_config() {
    local mode="$1"
    local dest="$2"
    cat > "$dest" <<CONFIG
plugins {
    id 'nf-gc@${plugin_version}'
}

nfGc {
    gc_mode = '${mode}'
}

// Test-only observability is read from Nextflow's config env scope.
env.NF_GC_TEST_TRACE = 'true'
CONFIG
}

run_case() {
    local case_name="$1"
    local gc_mode="$2"
    local publish_mode="$3"
    local run_dir="$OUT_ROOT/$case_name"

    mkdir -p "$run_dir"
    write_config "$gc_mode" "$run_dir/nf-gc.config"

    printf '\n=== nf-core/rnaseq acceptance: %s (gc=%s, publish=%s) ===\n' \
        "$case_name" "$gc_mode" "$publish_mode"

    (
        cd "$run_dir"
        export NXF_ANSI_LOG=false

        nextflow \
            -log "$run_dir/nextflow.log" \
            run nf-core/rnaseq \
            -r "$RNASEQ_REVISION" \
            -profile test,docker \
            -c "$run_dir/nf-gc.config" \
            --publish_dir_mode "$publish_mode" \
            --outdir "$run_dir/results" \
            -work-dir "$run_dir/work" \
            -with-trace "$run_dir/trace.tsv" \
            2>&1 | tee "$run_dir/console.log"
    )

    [[ -s "$run_dir/nf-gc-events.tsv" ]] || {
        echo "Missing nf-gc event trace for $case_name" >&2
        exit 1
    }
    [[ -s "$run_dir/trace.tsv" ]] || {
        echo "Missing Nextflow trace for $case_name" >&2
        exit 1
    }
}

# Install the exact working-tree plugin into the pinned local NXF_HOME used by env.sh.
./gradlew assemble installPlugin

# Keep the three third-party acceptance runs in one job so the nf-core checkout and
# Docker image cache are shared instead of paying their cold-start cost three times.
run_case process-link process link
run_case artifact-link artifact link
run_case artifact-copy artifact copy

python3 "$ROOT/scripts/ci/verify-rnaseq-acceptance.py" "$OUT_ROOT"
