# CUID sync runner

`run_all.py` runs the completed sync scripts one after another. Each script is started inside its own folder, so it loads that folder's `.env`.

Vendor Master runs after CoApplicant and Guarantor. Dealer Funding and Channel Partner are not in the run.

Turn a job off by setting `"enabled": false` in `jobs.json`, or with `--disable`. Disabled jobs are left out of a full run and out of `--resume`.

## Run in three batches

Run these from the repo root, in this order. Each script runs only its jobs, in the order listed, and waits 20 seconds between them. Pass `--interval` to change the gap for that run.

```bash
./run_batch1.sh
./run_batch2.sh
./run_batch3.sh
```

Two minutes between jobs:

```bash
./run_batch1.sh --interval 120
```

| Batch | Order |
|---|---|
| 1 | `posidex`, `poswaiver`, `baz_vendor`, `chola_hrms` |
| 2 | `dealer_vf`, `hl_agent`, `lap_agent`, `vf_leap` |
| 3 | `cd_samsung_los`, `csel_d2c`, `csel_zoho`, `gold_loan`, `hl_falcon`, `lap_los`, `sbpl_los`, `sme_los`, `coapplicant_guarantor`, `vendor_master` |

Vendor Master runs after CoApplicant and Guarantor. A failure inside a batch is recorded and the later jobs in that batch still run.

The same commands without the shell scripts:

```bash
python run_all.py --only posidex,poswaiver,baz_vendor,chola_hrms
python run_all.py --only dealer_vf,hl_agent,lap_agent,vf_leap
python run_all.py --only cd_samsung_los,csel_d2c,csel_zoho,gold_loan,hl_falcon,lap_los,sbpl_los,sme_los,coapplicant_guarantor,vendor_master
```

`--only` runs the ids in the order you type them.

## Run everything that is enabled

From the repo root:

```bash
python run_all.py
```

`python run_all.py` runs every enabled job in `jobs.json`, including Vendor Master after CoApplicant and Guarantor. The default gap between jobs is 20 seconds (`interval_seconds` in `jobs.json`). Override it for one run:

```bash
python run_all.py --interval 120
```

Use `--interval 0` to run the next job immediately.

A failure is written to `run_state.json` and the remaining jobs still run. Output for each job is printed and saved to `logs/<job_id>.log`.

## Enable and disable jobs

Edit `jobs.json` and set `"enabled"` to `true` or `false`, or use the flags:

```bash
python run_all.py --list
python run_all.py --disable posidex
python run_all.py --disable posidex,sme_los
python run_all.py --enable posidex
python run_all.py --enable hl_agent --disable vf_leap
```

`--enable` and `--disable` only update `jobs.json`. They do not start the sync.

`--list` shows the order, whether the job is enabled, and the last status.

## If one job fails

Rerun that job on its own:

```bash
python run_all.py --only posidex
```

`--only` runs the named jobs in the order given, even when a job is disabled, and even when it already succeeded. Several ids are allowed:

```bash
python run_all.py --only posidex,sme_los
```

Then continue with every enabled job that is not yet successful:

```bash
python run_all.py --resume
```

`--resume` skips jobs whose last status is `success`. Failed jobs and jobs that were interrupted (`running` or `failed`) run again. Disabled jobs stay skipped.

Clear the saved results and start clean on the next run:

```bash
python run_all.py --reset
python run_all.py
```

## Job order

| # | id | Script |
|---|---|---|
| 1 | `baz_vendor` | `BAZ Vendor - MSSQL to CH - Prod - Completed/BAZ_Vendor - Complete.py` |
| 2 | `cd_samsung_los` | `CD_SAMSUNG_LOS - Prod - Completed/CD_SAMSUNG_LOS_complete.py` |
| 3 | `csel_d2c` | `CSEL_D2C_LOS - Prod - Completed/CSEL_D2C_Complete.py` |
| 4 | `csel_zoho` | `CSEL_ZOHO_LOS - Prod - Completed/CSEL_ZOHO_LOS_complete.py` |
| 5 | `chola_hrms` | `Chola_HRMS_Employee - Prod - Completed/chola_hrms_employee.py` |
| 6 | `dealer_vf` | `Dealer_VF- PROD- Completed/Dealer VF_complete.py` |
| 7 | `gold_loan` | `GL_LOS - Created Date - PROD - Completed/Gold_loan_complete.py` |
| 8 | `hl_agent` | `HL_AGENT - PROD - Completed/HL_Agent_complete.py` |
| 9 | `hl_falcon` | `HL_FALCON_THEMEPRO_LOS - Completed/HL_FALCON_THEMEPRO_LOS_complete.py` |
| 10 | `lap_agent` | `LAP_AGENT - PROD - Completed/LAP_Agent_complete.py` |
| 11 | `lap_los` | `LAP_LOS - PROD - Completed/LAP_LOS_complete.py` |
| 12 | `posidex` | `Posidex - Postgres to CH Python - PROD - Completed/posidex_complete_script.py` |
| 13 | `poswaiver` | `Poswaiver - CH to CH Python - PROD - Completed/poswaiver_complete_script.py` |
| 14 | `sbpl_los` | `SBPL_LOS - PROD - Completed/SBPL_LOS_complete.py` |
| 15 | `sme_los` | `SME_LOS - PROD - Completed/SME_LOS_complete.py` |
| 16 | `vf_leap` | `VF_LEAP_LOS - Latest - PROD - Completed/VF_LEAP_LOS_Latest_complete.py` |
| 17 | `coapplicant_guarantor` | `CoApplicant and Guarantor - Prod - Completed/coapplicant and guarantor_cuid.py` |
| 18 | `vendor_master` | `Vendor Master Table - PROD - Completed/Vendor Master Table.py` |

These files are not part of the batch: `valkey/`, Poswaiver `cuid_service.py`, and the VF LEAP `valkey_set_value.py` / `valkey_update.py` helpers.

## Files

| File | Purpose |
|---|---|
| `run_batch1.sh` | posidex, poswaiver, baz_vendor, chola_hrms |
| `run_batch2.sh` | dealer_vf, hl_agent, lap_agent, vf_leap |
| `run_batch3.sh` | Remaining jobs. Vendor Master runs after CoApplicant and Guarantor |
| `run_all.py` | Runner |
| `jobs.json` | Order, paths, `enabled`, and the default interval |
| `run_state.json` | Last result for each job. Created on the first run. |
| `logs/<job_id>.log` | Stdout and stderr from the latest run of that job |
# CUID
