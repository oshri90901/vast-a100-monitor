"""
Vast.ai A100 market monitor.

Polls the public Vast.ai search API (same data as cloud.vast.ai/create) every
N seconds, and records per-machine state into SQLite:
  - machines   : latest static specs per machine (VRAM, CPU, RAM, disk, location...) + raw JSON
  - snapshots  : one row per machine per poll (total / free / rented GPUs, price...)
  - events     : derived changes (rent, release, price_change, new_machine, gone, back)

Rented GPUs are inferred from gpu_ids: every machine publishes offers for GPU
subsets; GPUs that appear only in non-rentable offers are currently rented.

Usage:
  python monitor.py                 # poll forever, every 60s
  python monitor.py --interval 120
  python monitor.py --once          # single poll (for Task Scheduler / testing)
"""
import argparse
import json
import os
import sqlite3
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime

import vastdata

API = "https://console.vast.ai/api/v0/bundles/"
GPU_NAMES = ["A100 SXM4", "A100 PCIE", "A100X", "A100 SXM"]
PAGE_LIMIT = 64          # anonymous API returns at most 64 offers per call
SPLIT_AT = 30            # the server silently trims results well below 64, so split early
GONE_AFTER = 3           # consecutive polls a machine must be missing before a 'gone' event
MAX_MACHINE_ID = 10_000_000
DEFAULT_DB = "vast_a100.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS machines (
    machine_id      INTEGER PRIMARY KEY,
    host_id         INTEGER,
    gpu_name        TEXT,
    total_gpus      INTEGER,
    gpu_ram_gb      REAL,
    gpu_mem_bw      REAL,
    gpu_max_power   REAL,
    bw_nvlink       REAL,
    pci_gen         REAL,
    gpu_lanes       INTEGER,
    pcie_bw         REAL,
    compute_cap     INTEGER,
    cuda_max_good   REAL,
    driver_version  TEXT,
    cpu_name        TEXT,
    cpu_cores       INTEGER,
    cpu_ram_gb      REAL,
    mobo_name       TEXT,
    disk_name       TEXT,
    disk_bw         REAL,
    disk_space_gb   REAL,
    inet_up         REAL,
    inet_down       REAL,
    geolocation     TEXT,
    public_ipaddr   TEXT,
    static_ip       INTEGER,
    hosting_type    INTEGER,
    cluster_id      INTEGER,
    verification    TEXT,
    reliability     REAL,
    dlperf          REAL,
    total_flops     REAL,
    os_version      TEXT,
    vms_enabled     INTEGER,
    end_date        REAL,
    first_seen      INTEGER,
    last_seen       INTEGER,
    raw_json        TEXT
);
CREATE TABLE IF NOT EXISTS snapshots (
    ts              INTEGER,
    machine_id      INTEGER,
    gpu_name        TEXT,
    gpu_ram_gb      REAL,
    total_gpus      INTEGER,
    free_gpus       INTEGER,
    rented_gpus     INTEGER,
    price_gpu_hr    REAL,     -- GPU price per GPU per hour (dph_base / num_gpus)
    dph_total_full  REAL,     -- whole machine $/hr incl. default storage
    storage_cost    REAL,     -- $/GB/month
    min_bid_gpu     REAL,     -- interruptible min bid per GPU per hour
    reliability     REAL,
    verification    TEXT,
    num_offers      INTEGER,
    PRIMARY KEY (ts, machine_id)
);
CREATE INDEX IF NOT EXISTS ix_snap_machine ON snapshots(machine_id, ts);
CREATE TABLE IF NOT EXISTS events (
    ts              INTEGER,
    machine_id      INTEGER,
    event           TEXT,
    delta           REAL,
    old_value       REAL,
    new_value       REAL,
    price_gpu_hr    REAL
);
CREATE INDEX IF NOT EXISTS ix_events ON events(machine_id, ts);
CREATE TABLE IF NOT EXISTS polls (
    ts              INTEGER PRIMARY KEY,
    ok              INTEGER,
    api_calls       INTEGER,
    offers          INTEGER,
    machines        INTEGER,
    total_gpus      INTEGER,
    rented_gpus     INTEGER,
    error           TEXT
);
"""


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


API_KEY = os.environ.get("VAST_API_KEY")


def api_query(q, retries=3):
    url = API + "?q=" + urllib.parse.quote(json.dumps(q))
    headers = {"User-Agent": "a100-market-monitor/1.0"}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(5 * (attempt + 1))


def fetch_all_offers():
    """Fetch every A100 offer, splitting by machine_id range to get past the 64-result cap."""
    base = {"gpu_name": {"in": GPU_NAMES}, "type": "on-demand", "limit": PAGE_LIMIT}
    offers, calls = {}, 0

    def fetch(lo, hi):
        nonlocal calls
        calls += 1
        res = api_query({**base, "machine_id": {"gte": lo, "lt": hi}})["offers"]
        if len(res) >= SPLIT_AT and hi - lo > 1:
            mid = (lo + hi) // 2
            fetch(lo, mid)
            fetch(mid, hi)
        else:
            if len(res) >= PAGE_LIMIT:
                log(f"WARN machine {lo} has >= {PAGE_LIMIT} offers, may be incomplete")
            for o in res:
                offers[o["id"]] = o

    fetch(0, MAX_MACHINE_ID)
    return list(offers.values()), calls


def summarize_machines(offers):
    """Collapse offers into one record per machine."""
    by_machine = {}
    for o in offers:
        by_machine.setdefault(o["machine_id"], []).append(o)

    result = {}
    for mid, offs in by_machine.items():
        all_ids, free_ids = set(), set()
        frac_total = 0
        for o in offs:
            all_ids.update(o.get("gpu_ids") or [])
            if o.get("rentable"):
                free_ids.update(o.get("gpu_ids") or [])
            if o.get("gpu_frac"):
                frac_total = max(frac_total, round(o["num_gpus"] / o["gpu_frac"]))
        total = max(len(all_ids), frac_total)
        free = len(free_ids)
        # Most representative offer: the largest bundle
        big = max(offs, key=lambda o: o["num_gpus"])
        price_gpu = big["dph_base"] / big["num_gpus"]
        result[mid] = {
            "offer": big,
            "total_gpus": total,
            "free_gpus": free,
            "rented_gpus": total - free,
            "price_gpu_hr": price_gpu,
            "dph_total_full": big["dph_total"] * total / big["num_gpus"],
            "min_bid_gpu": (big.get("min_bid") or 0) / big["num_gpus"],
            "num_offers": len(offs),
        }
    return result


class Monitor:
    def __init__(self, db_path):
        self.db = sqlite3.connect(db_path)
        self.db.executescript(SCHEMA)
        self.prev = self._load_last_state()

    def _load_last_state(self):
        rows = self.db.execute(
            """SELECT s.machine_id, s.rented_gpus, s.price_gpu_hr FROM snapshots s
               JOIN (SELECT machine_id, MAX(ts) ts FROM snapshots GROUP BY machine_id) l
               ON s.machine_id = l.machine_id AND s.ts = l.ts"""
        ).fetchall()
        last_poll = self.db.execute("SELECT MAX(ts) FROM snapshots").fetchone()[0]
        present = {r[0] for r in self.db.execute(
            "SELECT machine_id FROM snapshots WHERE ts = ?", (last_poll,))} if last_poll else set()
        return {r[0]: {"rented": r[1], "price": r[2], "present": r[0] in present, "missing": 0}
                for r in rows}

    def poll(self):
        ts = int(time.time())
        try:
            offers, calls = fetch_all_offers()
        except Exception as e:
            self.db.execute("INSERT OR REPLACE INTO polls(ts, ok, error) VALUES (?,0,?)", (ts, str(e)))
            self.db.commit()
            log(f"ERROR poll failed: {e}")
            return
        machines = summarize_machines(offers)
        events = []

        for mid, m in machines.items():
            o = m["offer"]
            self._upsert_machine(ts, mid, m)
            self.db.execute(
                "INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ts, mid, o["gpu_name"], o["gpu_ram"] / 1024, m["total_gpus"], m["free_gpus"],
                 m["rented_gpus"], m["price_gpu_hr"], m["dph_total_full"], o.get("storage_cost"),
                 m["min_bid_gpu"], o.get("reliability"), o.get("verification"), m["num_offers"]))

            p = self.prev.get(mid)
            price = m["price_gpu_hr"]
            if p is None:
                events.append((ts, mid, "new_machine", m["rented_gpus"], None, m["rented_gpus"], price))
            else:
                if not p["present"]:
                    events.append((ts, mid, "back", 0, None, m["rented_gpus"], price))
                d = m["rented_gpus"] - p["rented"]
                if d > 0:
                    events.append((ts, mid, "rent", d, p["rented"], m["rented_gpus"], price))
                elif d < 0:
                    events.append((ts, mid, "release", d, p["rented"], m["rented_gpus"], price))
                if p["price"] and abs(price - p["price"]) > 1e-6:
                    events.append((ts, mid, "price_change", price - p["price"], p["price"], price, price))
            self.prev[mid] = {"rented": m["rented_gpus"], "price": price, "present": True, "missing": 0}

        for mid, p in self.prev.items():
            if p["present"] and mid not in machines:
                p["missing"] += 1
                if p["missing"] >= GONE_AFTER:
                    events.append((ts, mid, "gone", 0, p["rented"], None, p["price"]))
                    p["present"] = False

        self.db.executemany("INSERT INTO events VALUES (?,?,?,?,?,?,?)", events)
        tot = sum(m["total_gpus"] for m in machines.values())
        rented = sum(m["rented_gpus"] for m in machines.values())
        self.db.execute("INSERT OR REPLACE INTO polls VALUES (?,1,?,?,?,?,?,NULL)",
                        (ts, calls, len(offers), len(machines), tot, rented))
        self.db.commit()

        ev = {}
        for e in events:
            ev[e[2]] = ev.get(e[2], 0) + 1
        util = rented / tot * 100 if tot else 0
        log(f"{len(machines)} machines | {tot} GPUs | {rented} rented ({util:.1f}%) | "
            f"{len(offers)} offers, {calls} calls | events: {ev or '-'}")

    def _upsert_machine(self, ts, mid, m):
        o = m["offer"]
        cols = {
            "machine_id": mid, "host_id": o.get("host_id"), "gpu_name": o.get("gpu_name"),
            "total_gpus": m["total_gpus"], "gpu_ram_gb": (o.get("gpu_ram") or 0) / 1024,
            "gpu_mem_bw": o.get("gpu_mem_bw"), "gpu_max_power": o.get("gpu_max_power"),
            "bw_nvlink": o.get("bw_nvlink"), "pci_gen": o.get("pci_gen"),
            "gpu_lanes": o.get("gpu_lanes"), "pcie_bw": o.get("pcie_bw"),
            "compute_cap": o.get("compute_cap"), "cuda_max_good": o.get("cuda_max_good"),
            "driver_version": o.get("driver_version"), "cpu_name": (o.get("cpu_name") or "").strip(),
            "cpu_cores": o.get("cpu_cores"), "cpu_ram_gb": (o.get("cpu_ram") or 0) / 1024,
            "mobo_name": o.get("mobo_name"), "disk_name": o.get("disk_name"),
            "disk_bw": o.get("disk_bw"), "disk_space_gb": o.get("disk_space"),
            "inet_up": o.get("inet_up"), "inet_down": o.get("inet_down"),
            "geolocation": o.get("geolocation"), "public_ipaddr": o.get("public_ipaddr"),
            "static_ip": int(bool(o.get("static_ip"))), "hosting_type": o.get("hosting_type"),
            "cluster_id": o.get("cluster_id"), "verification": o.get("verification"),
            "reliability": o.get("reliability"), "dlperf": o.get("dlperf"),
            "total_flops": o.get("total_flops"), "os_version": o.get("os_version"),
            "vms_enabled": int(bool(o.get("vms_enabled"))), "end_date": o.get("end_date"),
            "last_seen": ts, "raw_json": json.dumps(o),
        }
        names = ", ".join(cols)
        marks = ", ".join("?" * len(cols))
        updates = ", ".join(f"{k}=excluded.{k}" for k in cols if k != "machine_id")
        self.db.execute(
            f"INSERT INTO machines ({names}, first_seen) VALUES ({marks}, ?) "
            f"ON CONFLICT(machine_id) DO UPDATE SET {updates}",
            (*cols.values(), ts))


def git_push(paths, retries=3):
    subprocess.run(["git", "add", *paths], check=True)
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
        return
    subprocess.run(["git", "commit", "-q", "-m", f"data: {os.path.basename(paths[0]).split('-')[0]}"],
                   check=True)
    for attempt in range(retries):
        subprocess.run(["git", "pull", "-q", "--rebase"], check=False)
        if subprocess.run(["git", "push", "-q"]).returncode == 0:
            return
        time.sleep(10 * (attempt + 1))
    log("ERROR git push failed")


def main():
    ap = argparse.ArgumentParser(description="Vast.ai A100 market monitor")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--interval", type=int, default=60, help="seconds between polls (default 60)")
    ap.add_argument("--once", action="store_true", help="poll once and exit")
    ap.add_argument("--duration", type=float, help="stop after N minutes")
    ap.add_argument("--export-dir", help="also write gzip data chunks into this directory")
    ap.add_argument("--chunk-minutes", type=float, default=30, help="minutes per exported chunk")
    ap.add_argument("--git-push", action="store_true", help="commit and push each exported chunk")
    ap.add_argument("--gpus", help="comma-separated Vast gpu_name values to track (default: A100 models)")
    args = ap.parse_args()
    if args.gpus:
        global GPU_NAMES
        GPU_NAMES = [g.strip() for g in args.gpus.split(",")]

    mon = Monitor(args.db)
    log(f"DB: {args.db} | known machines: {len(mon.prev)} | interval: {args.interval}s")
    if args.once:
        mon.poll()
        return

    t_start = time.time()
    exported_machines = set()   # specs are exported once per run; snapshots carry the changing fields
    chunk_start = int(t_start)

    def export(until):
        nonlocal chunk_start
        if not args.export_dir:
            return
        paths = vastdata.export_chunk(mon.db, args.export_dir, chunk_start, until, exported_machines)
        chunk_start = until
        if paths:
            log(f"exported {paths[0]}")
            if args.git_push:
                git_push(paths)

    try:
        while not args.duration or time.time() - t_start < args.duration * 60:
            start = time.time()
            mon.poll()
            if time.time() - chunk_start >= args.chunk_minutes * 60:
                export(int(time.time()))
            time.sleep(max(1, args.interval - (time.time() - start)))
    except KeyboardInterrupt:
        log("stopped")
    export(int(time.time()) + 1)


if __name__ == "__main__":
    main()
