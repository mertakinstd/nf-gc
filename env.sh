#!/usr/bin/env bash
# Copyright 2026 Mert Akin
# SPDX-License-Identifier: Apache-2.0

# Source this file after running ./scripts/bootstrap-dev.sh.

_NF_GC_ENV_SOURCE="${BASH_SOURCE[0]:-$0}"
NF_GC_ROOT="$(cd -- "$(dirname -- "$_NF_GC_ENV_SOURCE")" && pwd)"
unset _NF_GC_ENV_SOURCE
export NF_GC_ROOT

NF_GC_TOOLS="$NF_GC_ROOT/.tools"
NF_GC_JAVA_HOME="$NF_GC_TOOLS/java"

if [[ ! -x "$NF_GC_JAVA_HOME/bin/java" || ! -x "$NF_GC_TOOLS/nextflow" || ! -x "$NF_GC_TOOLS/nf-test" ]]; then
    printf '%s\n' \
        "nf-gc development tools are not installed." \
        "Run ./scripts/bootstrap-dev.sh before sourcing env.sh." >&2
    unset NF_GC_TOOLS NF_GC_JAVA_HOME
    return 1 2>/dev/null || exit 1
fi

export JAVA_HOME="$NF_GC_JAVA_HOME"
export NXF_VER="26.04.6"
export NXF_HOME="$NF_GC_ROOT/.nxf"

case ":$PATH:" in
    *":$NF_GC_TOOLS:"*) ;;
    *) PATH="$NF_GC_TOOLS:$PATH" ;;
esac
case ":$PATH:" in
    *":$JAVA_HOME/bin:"*) ;;
    *) PATH="$JAVA_HOME/bin:$PATH" ;;
esac
export PATH

unset NF_GC_TOOLS NF_GC_JAVA_HOME
