#!/usr/bin/env python3
"""Run the CUID sync scripts one after another.

Each job runs in its own folder so that script's .env is loaded.
Set "enabled" to false in jobs.json to leave a job out of the batch.
A failed job is recorded and the rest continue. Use --only to rerun
one job, then --resume to continue with the ones that are not successful yet.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
JOBS_FILE = ROOT / "jobs.json"
STATE_FILE = ROOT / "run_state.json"
LOG_DIR = ROOT / "logs"
VENV_PYTHON = ROOT / ".venv" / "bin" / "python"


def use_project_python() -> None:
    """Run jobs with the project virtualenv, which has pyodbc and the other dependencies."""
    if not VENV_PYTHON.is_file():
        return
    if Path(sys.executable).resolve() == VENV_PYTHON.resolve():
        return
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_config() -> dict:
    if not JOBS_FILE.is_file():
        raise SystemExit(f"Missing {JOBS_FILE.name}. It lists the jobs and which ones are enabled.")
    config = load_json(JOBS_FILE)
    jobs = config.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise SystemExit(f"{JOBS_FILE.name} has no jobs.")
    seen: set[str] = set()
    for job in jobs:
        job_id = job.get("id")
        if not job_id or job_id in seen:
            raise SystemExit(f"Each job needs a unique id. Problem in {JOBS_FILE.name}: {job_id!r}")
        seen.add(job_id)
        for key in ("name", "folder", "script"):
            if not job.get(key):
                raise SystemExit(f"Job {job_id} is missing {key}.")
        job["enabled"] = bool(job.get("enabled", True))
    return config


def load_state() -> dict:
    if not STATE_FILE.is_file():
        return {"jobs": {}}
    state = load_json(STATE_FILE)
    state.setdefault("jobs", {})
    return state


def parse_ids(values: list[str]) -> list[str]:
    ids: list[str] = []
    for value in values:
        for part in value.split(","):
            job_id = part.strip()
            if job_id:
                ids.append(job_id)
    return ids


def require_known(jobs: list[dict], job_ids: list[str]) -> None:
    known = {job["id"] for job in jobs}
    unknown = [job_id for job_id in job_ids if job_id not in known]
    if unknown:
        names = ", ".join(job["id"] for job in jobs)
        raise SystemExit(f"Unknown job id: {', '.join(unknown)}\nKnown ids: {names}")


def set_enabled(jobs: list[dict], job_ids: list[str], enabled: bool) -> None:
    wanted = set(job_ids)
    for job in jobs:
        if job["id"] in wanted:
            job["enabled"] = enabled


def print_jobs(jobs: list[dict], state: dict) -> None:
    print(f"{'#':<3} {'id':<24} {'enabled':<8} {'status':<10} name")
    for index, job in enumerate(jobs, start=1):
        record = state.get("jobs", {}).get(job["id"], {})
        status = record.get("status", "-")
        enabled = "yes" if job["enabled"] else "no"
        print(f"{index:<3} {job['id']:<24} {enabled:<8} {status:<10} {job['name']}")


def select_jobs(jobs: list[dict], state: dict, only_ids: list[str], resume: bool) -> list[dict]:
    if only_ids:
        by_id = {job["id"]: job for job in jobs}
        selected = []
        seen: set[str] = set()
        for job_id in only_ids:
            if job_id in seen:
                continue
            seen.add(job_id)
            selected.append(by_id[job_id])
        return selected
    selected = []
    for job in jobs:
        if not job["enabled"]:
            continue
        if resume and state.get("jobs", {}).get(job["id"], {}).get("status") == "success":
            continue
        selected.append(job)
    return selected


def run_job(job: dict, log_path: Path) -> tuple[int, subprocess.Popen | None]:
    folder = ROOT / job["folder"]
    script = folder / job["script"]
    env_file = folder / ".env"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("w", encoding="utf-8") as log:
        header = (
            f"=== {job['name']} ({job['id']}) started {now_iso()} ===\n"
            f"cwd: {folder}\n"
            f"script: {job['script']}\n"
        )
        sys.stdout.write(header)
        log.write(header)
        log.flush()

        if not script.is_file():
            message = f"Script not found: {script}\n"
            sys.stdout.write(message)
            log.write(message)
            return 1, None
        if not env_file.is_file():
            message = f".env not found: {env_file}\n"
            sys.stdout.write(message)
            log.write(message)
            return 1, None

        process = subprocess.Popen(
            [sys.executable, "-u", job["script"]],
            cwd=folder,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        try:
            for line in process.stdout:
                sys.stdout.write(line)
                log.write(line)
            exit_code = process.wait()
        except KeyboardInterrupt:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            log.write(f"\n=== interrupted {now_iso()} ===\n")
            raise
        footer = f"=== {job['id']} finished {now_iso()} exit {exit_code} ===\n"
        sys.stdout.write(footer)
        log.write(footer)
        return exit_code, process


def record(state: dict, job: dict, status: str, exit_code: int | None, started_at: str, finished_at: str | None) -> None:
    state["jobs"][job["id"]] = {
        "name": job["name"],
        "status": status,
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "log": str(Path("logs") / f"{job['id']}.log"),
    }
    save_json(STATE_FILE, state)


def execute(jobs: list[dict], state: dict, interval: int) -> int:
    failures: list[str] = []
    for index, job in enumerate(jobs):
        started_at = now_iso()
        log_path = LOG_DIR / f"{job['id']}.log"
        record(state, job, "running", None, started_at, None)
        print(f"\n[{index + 1}/{len(jobs)}] {job['name']} ({job['id']})")
        try:
            exit_code, _process = run_job(job, log_path)
        except KeyboardInterrupt:
            record(state, job, "failed", None, started_at, now_iso())
            print(f"\nStopped during {job['id']}. State saved. Resume with: python run_all.py --resume")
            return 130

        status = "success" if exit_code == 0 else "failed"
        record(state, job, status, exit_code, started_at, now_iso())
        if exit_code != 0:
            failures.append(job["id"])
            print(f"{job['id']} failed with exit {exit_code}. Continuing with the remaining jobs.")

        remaining = index < len(jobs) - 1
        if remaining and interval > 0:
            print(f"Waiting {interval} seconds before the next job...")
            try:
                time.sleep(interval)
            except KeyboardInterrupt:
                print("\nStopped during the interval. Completed jobs stay recorded. Resume with: python run_all.py --resume")
                return 130

    print("\nSummary")
    print(f"  ran: {len(jobs)}")
    print(f"  success: {len(jobs) - len(failures)}")
    print(f"  failed: {len(failures)}")
    if failures:
        print("  failed ids: " + ", ".join(failures))
        print("Rerun one job:  python run_all.py --only " + failures[0])
        print("Resume the rest: python run_all.py --resume")
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run enabled CUID sync jobs in order. Each job uses the .env in its own folder."
    )
    parser.add_argument("--interval", type=int, default=None, help="Seconds to wait between jobs. Overrides jobs.json for this run.")
    parser.add_argument("--only", action="append", default=[], help="Run these job ids only, in the order given, even if disabled. Repeat the flag or separate ids with commas.")
    parser.add_argument("--resume", action="store_true", help="Skip enabled jobs that already succeeded.")
    parser.add_argument("--list", action="store_true", help="Show job order, enabled flag, and last status.")
    parser.add_argument("--reset", action="store_true", help="Clear run_state.json. Does not start a run.")
    parser.add_argument("--enable", action="append", default=[], help="Turn jobs on in jobs.json. Repeat the flag or separate ids with commas.")
    parser.add_argument("--disable", action="append", default=[], help="Turn jobs off in jobs.json. Repeat the flag or separate ids with commas.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = load_config()
    jobs: list[dict] = config["jobs"]
    enable_ids = parse_ids(args.enable)
    disable_ids = parse_ids(args.disable)
    only_ids = parse_ids(args.only)
    require_known(jobs, enable_ids + disable_ids + only_ids)

    overlap = sorted(set(enable_ids) & set(disable_ids))
    if overlap:
        raise SystemExit(f"Same job in --enable and --disable: {', '.join(overlap)}")

    changed = False
    if enable_ids:
        set_enabled(jobs, enable_ids, True)
        changed = True
    if disable_ids:
        set_enabled(jobs, disable_ids, False)
        changed = True
    if changed:
        save_json(JOBS_FILE, config)
        print("Updated jobs.json")
        for job_id in enable_ids:
            print(f"  enabled {job_id}")
        for job_id in disable_ids:
            print(f"  disabled {job_id}")

    if args.reset:
        save_json(STATE_FILE, {"jobs": {}})
        print(f"Cleared {STATE_FILE.name}")

    state = load_state()
    if args.list:
        print_jobs(jobs, state)
        return 0

    # Enable, disable, and reset only change config or state.
    # A batch starts when those flags are absent, or when --only / --resume is set.
    config_only = (changed or args.reset) and not only_ids and not args.resume
    if config_only:
        return 0

    if args.interval is None:
        interval = int(config.get("interval_seconds", 20))
    else:
        interval = args.interval
    if interval < 0:
        raise SystemExit("--interval must be 0 or greater.")

    selected = select_jobs(jobs, state, only_ids, args.resume)
    if not selected:
        print("No jobs to run.")
        if args.resume:
            print("Every enabled job already succeeded. Use --reset to run them again, or --only <id> to rerun one.")
        else:
            print("Enable a job in jobs.json or with: python run_all.py --enable <id>")
        return 0

    disabled_in_only = [job["id"] for job in selected if only_ids and not job["enabled"]]
    if disabled_in_only:
        print("Running disabled jobs because --only was set: " + ", ".join(disabled_in_only))

    if args.resume:
        skipped = [
            job["id"]
            for job in jobs
            if job["enabled"] and state.get("jobs", {}).get(job["id"], {}).get("status") == "success"
        ]
        if skipped:
            print("Skipping already successful: " + ", ".join(skipped))

    print(f"Jobs this run: {', '.join(job['id'] for job in selected)}")
    print(f"Interval: {interval} seconds")
    return execute(selected, state, interval)


if __name__ == "__main__":
    use_project_python()
    raise SystemExit(main())
