#!/usr/bin/env bash
# Copyright 2026 Mert Akin
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

source "$ROOT/env.sh"

./gradlew clean test assemble installPlugin
nf-test test tests --verbose
