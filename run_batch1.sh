#!/bin/bash
# Batch 1: posidex, poswaiver, baz_vendor, chola_hrms
cd "$(dirname "$0")"
if [[ -x .venv/bin/python ]]; then
  exec .venv/bin/python run_all.py --only posidex,poswaiver,baz_vendor,chola_hrms "$@"
fi
if command -v uv >/dev/null 2>&1; then
  export UV_SYSTEM_CERTS=1
  exec uv run python run_all.py --only posidex,poswaiver,baz_vendor,chola_hrms "$@"
fi
echo "No project Python. From this folder run: UV_SYSTEM_CERTS=1 uv sync" >&2
exit 1
