#!/usr/bin/env bash
set -euo pipefail

set -a && source .env && set +a

bruin connections test --name "supabase"
