#!/bin/bash
# Batch 1: posidex, poswaiver, baz_vendor, chola_hrms
cd "$(dirname "$0")"
exec python run_all.py --only posidex,poswaiver,baz_vendor,chola_hrms "$@"
