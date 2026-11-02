#!/bin/bash
# Batch 2: dealer_vf, hl_agent, lap_agent, vf_leap
cd "$(dirname "$0")"
exec python run_all.py --only dealer_vf,hl_agent,lap_agent,vf_leap "$@"
