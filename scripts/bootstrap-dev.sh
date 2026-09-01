#!/usr/bin/env bash
# Copyright 2026 Mert Akin
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

readonly JAVA_VERSION="21.0.12+8"
readonly JAVA_FILE_VERSION="21.0.12_8"
readonly NEXTFLOW_VERSION="26.04.6"
readonly NF_TEST_VERSION="0.9.5"

readonly NEXTFLOW_SHA256="182a63c74074e2dc7956ffa3c8cd59de952ed2c44394e21faf5e1736b945444c"
readonly NF_TEST_SHA256="b7679eb90cdc9642bfa89e9634db02ce6e699de53ad35ef0e2f4847634fc1641"

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$ROOT/.tools"
JAVA_HOME_LOCAL="$TOOLS_DIR/java"
NEXTFLOW_BIN="$TOOLS_DIR/nextflow"
NF_TEST_BIN="$TOOLS_DIR/nf-test"
NF_TEST_JAR="$TOOLS_DIR/nf-test.jar"

CLEANUP_PATHS=()

cleanup() {
    local path
    for path in "${CLEANUP_PATHS[@]}"; do
        [[ -n "$path" ]] && rm -rf "$path"
    done
}
trap cleanup EXIT

register_cleanup() {
    CLEANUP_PATHS+=("$1")
}

log() {
    printf '[nf-gc bootstrap] %s\n' "$*"
}

die() {
    printf '[nf-gc bootstrap] ERROR: %s\n' "$*" >&2
    exit 1
}

need_command() {
    command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

download() {
    local url="$1"
    local dest="$2"

    if command -v curl >/dev/null 2>&1; then
        curl --fail --location --silent --show-error --retry 3 --output "$dest" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget --quiet --tries=3 --output-document="$dest" "$url"
    else
        die "curl or wget is required to download the development toolchain"
    fi
}

sha256_of() {
    local file="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$file" | awk '{print $1}'
    else
        die "sha256sum or shasum is required for checksum verification"
    fi
}

verify_sha256() {
    local file="$1"
    local expected="$2"
    local actual
    actual="$(sha256_of "$file")"
    [[ "$actual" == "$expected" ]] || die "Checksum mismatch for ${file##*/}: expected $expected, got $actual"
}

platform_tuple() {
    local os arch java_os java_arch
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux)  java_os="linux" ;;
        Darwin) java_os="mac" ;;
        *) die "Unsupported operating system: $os (supported: Linux, macOS)" ;;
    esac

    case "$arch" in
        x86_64|amd64) java_arch="x64" ;;
        arm64|aarch64) java_arch="aarch64" ;;
        *) die "Unsupported architecture: $arch (supported: x86_64, arm64/aarch64)" ;;
    esac

    printf '%s %s\n' "$java_os" "$java_arch"
}

install_java() {
    local java_os="$1"
    local java_arch="$2"
    local marker="$JAVA_HOME_LOCAL/.nf-gc-version"

    if [[ -x "$JAVA_HOME_LOCAL/bin/java" && -f "$marker" && "$(cat "$marker")" == "$JAVA_VERSION" ]]; then
        log "Temurin JDK $JAVA_VERSION already installed"
        return
    fi

    local filename url checksum_url tmp archive checksum_file extracted java_bin extracted_home expected
    filename="OpenJDK21U-jdk_${java_arch}_${java_os}_hotspot_${JAVA_FILE_VERSION}.tar.gz"
    url="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12%2B8/$filename"
    checksum_url="$url.sha256.txt"
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/nf-gc-java.XXXXXX")"
    archive="$tmp/$filename"
    checksum_file="$archive.sha256.txt"
    extracted="$tmp/extracted"
    mkdir -p "$extracted"

    register_cleanup "$tmp"

    log "Downloading Temurin JDK $JAVA_VERSION ($java_os/$java_arch)"
    download "$url" "$archive"
    download "$checksum_url" "$checksum_file"
    expected="$(awk 'NF {print $1; exit}' "$checksum_file" | tr '[:upper:]' '[:lower:]')"
    [[ "$expected" =~ ^[0-9a-f]{64}$ ]] || die "Invalid Temurin checksum metadata"
    verify_sha256 "$archive" "$expected"

    tar -xzf "$archive" -C "$extracted"
    java_bin="$(find "$extracted" -type f -path '*/bin/java' -print -quit)"
    [[ -n "$java_bin" ]] || die "Could not locate bin/java in the Temurin archive"
    extracted_home="$(cd -- "$(dirname -- "$java_bin")/.." && pwd)"

    rm -rf "$JAVA_HOME_LOCAL"
    mkdir -p "$JAVA_HOME_LOCAL"
    cp -R "$extracted_home"/. "$JAVA_HOME_LOCAL"/
    printf '%s\n' "$JAVA_VERSION" > "$marker"

    rm -rf "$tmp"
}

install_nextflow() {
    local marker="$TOOLS_DIR/.nextflow-version"
    if [[ -x "$NEXTFLOW_BIN" && -f "$marker" && "$(cat "$marker")" == "$NEXTFLOW_VERSION" ]]; then
        log "Nextflow $NEXTFLOW_VERSION already installed"
        return
    fi

    local tmp url
    tmp="$(mktemp "${TMPDIR:-/tmp}/nf-gc-nextflow.XXXXXX")"
    register_cleanup "$tmp"
    url="https://github.com/nextflow-io/nextflow/releases/download/v${NEXTFLOW_VERSION}/nextflow-${NEXTFLOW_VERSION}-dist"

    log "Downloading Nextflow $NEXTFLOW_VERSION"
    download "$url" "$tmp"
    verify_sha256 "$tmp" "$NEXTFLOW_SHA256"
    install -m 0755 "$tmp" "$NEXTFLOW_BIN"
    printf '%s\n' "$NEXTFLOW_VERSION" > "$marker"

    rm -f "$tmp"
}

install_nf_test() {
    local marker="$TOOLS_DIR/.nf-test-version"
    if [[ -x "$NF_TEST_BIN" && -f "$NF_TEST_JAR" && -f "$marker" && "$(cat "$marker")" == "$NF_TEST_VERSION" ]]; then
        log "nf-test $NF_TEST_VERSION already installed"
        return
    fi

    local tmp archive url
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/nf-gc-nf-test.XXXXXX")"
    archive="$tmp/nf-test-${NF_TEST_VERSION}.tar.gz"
    url="https://github.com/askimed/nf-test/releases/download/v${NF_TEST_VERSION}/nf-test-${NF_TEST_VERSION}.tar.gz"
    register_cleanup "$tmp"

    log "Downloading nf-test $NF_TEST_VERSION"
    download "$url" "$archive"
    verify_sha256 "$archive" "$NF_TEST_SHA256"
    tar -xzf "$archive" -C "$tmp" nf-test.jar
    install -m 0644 "$tmp/nf-test.jar" "$NF_TEST_JAR"

    cat > "$NF_TEST_BIN" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
TOOLS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec "$TOOLS_DIR/java/bin/java" -jar "$TOOLS_DIR/nf-test.jar" "$@"
WRAPPER
    chmod 0755 "$NF_TEST_BIN"
    printf '%s\n' "$NF_TEST_VERSION" > "$marker"

    rm -rf "$tmp"
}

verify_installation() {
    local java_version nextflow_version nf_test_version

    java_version="$($JAVA_HOME_LOCAL/bin/java -version 2>&1 | head -n 1)"
    [[ "$java_version" == *"21.0.12"* ]] || die "Unexpected Java version: $java_version"

    nextflow_version="$(JAVA_HOME="$JAVA_HOME_LOCAL" PATH="$JAVA_HOME_LOCAL/bin:$TOOLS_DIR:$PATH" NXF_HOME="$ROOT/.nxf" NXF_VER="$NEXTFLOW_VERSION" "$NEXTFLOW_BIN" -version 2>&1)"
    [[ "$nextflow_version" == *"version $NEXTFLOW_VERSION"* ]] || die "Unexpected Nextflow version output"

    nf_test_version="$($NF_TEST_BIN version 2>&1)"
    [[ "$nf_test_version" == *"$NF_TEST_VERSION"* ]] || die "Unexpected nf-test version output: $nf_test_version"

    log "$java_version"
    log "Nextflow $NEXTFLOW_VERSION verified"
    log "nf-test $NF_TEST_VERSION verified"
}

main() {
    need_command uname
    need_command tar
    need_command awk
    need_command find
    need_command install
    need_command tr

    mkdir -p "$TOOLS_DIR" "$ROOT/.nxf"

    local tuple java_os java_arch
    tuple="$(platform_tuple)"
    read -r java_os java_arch <<< "$tuple"

    install_java "$java_os" "$java_arch"
    install_nextflow
    install_nf_test
    verify_installation

    if command -v docker >/dev/null 2>&1; then
        log "Docker detected: $(docker --version 2>/dev/null || printf 'version unavailable')"
    else
        log "Docker not found (optional; not required for core nf-gc development)"
    fi

    log "Development environment is ready. Run: source env.sh"
}

main "$@"
