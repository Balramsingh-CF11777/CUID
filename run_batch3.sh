#!/bin/bash
# Batch 3: remaining jobs. Vendor Master runs after CoApplicant and Guarantor.
cd "$(dirname "$0")"
if [[ -x .venv/bin/python ]]; then
  exec .venv/bin/python run_all.py --only cd_samsung_los,csel_d2c,csel_zoho,gold_loan,hl_falcon,lap_los,sbpl_los,sme_los,coapplicant_guarantor,vendor_master "$@"
fi
if command -v uv >/dev/null 2>&1; then
  export UV_SYSTEM_CERTS=1
  exec uv run python run_all.py --only cd_samsung_los,csel_d2c,csel_zoho,gold_loan,hl_falcon,lap_los,sbpl_los,sme_los,coapplicant_guarantor,vendor_master "$@"
fi
echo "No project Python. From this folder run: UV_SYSTEM_CERTS=1 uv sync" >&2
exit 1
