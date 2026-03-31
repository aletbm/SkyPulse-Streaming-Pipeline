#!/usr/bin/env bash
set -euo pipefail

set -a && source .env && set +a

bruin run pipeline \
  --full-refresh \
  --workers 1
