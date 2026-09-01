#!/usr/bin/env bash

NF_GC_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export NXF_VER=26.04.6
export NXF_HOME="$NF_GC_ROOT/.nxf"
export PATH="$NF_GC_ROOT/.tools:$PATH"

unset NF_GC_ROOT
