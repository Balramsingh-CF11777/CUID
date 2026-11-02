#!/bin/bash
# Batch 3: remaining jobs. Vendor Master runs after CoApplicant and Guarantor.
cd "$(dirname "$0")"
exec python run_all.py --only cd_samsung_los,csel_d2c,csel_zoho,gold_loan,hl_falcon,lap_los,sbpl_los,sme_los,coapplicant_guarantor,vendor_master "$@"
