"""
Shared data helpers:
  - export_chunk : dump rows from a monitor DB into small gzip files (for committing to git)
  - import_dir   : load all chunk files into a local SQLite DB (skips files already imported)
  - smooth       : median-of-3 filter that removes single-poll API glitches
  - rebuild_events: recompute rent/release/price events from smoothed snapshots
"""
import csv
import gzip
import io
import json
import os
import sqlite3
from datetime import datetime, timezone

SNAP_COLS = ["ts", "machine_id", "gpu_name", "gpu_ram_gb", "total_gpus", "free_gpus", "rented_gpus",
             "price_gpu_hr", "dph_total_full", "storage_cost", "min_bid_gpu", "reliability",
             "verification", "num_offers"]
POLL_COLS = ["ts", "ok", "api_calls", "offers", "machines", "total_gpus", "rented_gpus", "error"]
GAP_FACTOR = 3   # neighbours further apart than GAP_FACTOR x poll interval are not smoothed together


def _write_gz(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        fh.write(text)


def _csv_text(cols, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    w.writerows(rows)
    return buf.getvalue()


def export_chunk(db, out_dir, since, until, skip_machines=None):
    """Write rows with since <= ts < until into out_dir/YYYY-MM-DD/HHMMSS-*.gz. Returns written paths.

    Machines in skip_machines (a set, updated in place) were already exported and are left out."""
    snaps = db.execute(f"SELECT {', '.join(SNAP_COLS)} FROM snapshots WHERE ts >= ? AND ts < ? ORDER BY ts",
                       (since, until)).fetchall()
    polls = db.execute(f"SELECT {', '.join(POLL_COLS)} FROM polls WHERE ts >= ? AND ts < ? ORDER BY ts",
                       (since, until)).fetchall()
    if not polls:
        return []
    cur = db.execute("SELECT * FROM machines WHERE last_seen >= ? AND last_seen < ?", (since, until))
    names = [d[0] for d in cur.description]
    machines = [dict(zip(names, r)) for r in cur.fetchall()]
    if skip_machines is not None:
        machines = [m for m in machines if m["machine_id"] not in skip_machines]
        skip_machines.update(m["machine_id"] for m in machines)

    t0 = datetime.fromtimestamp(polls[0][0], timezone.utc)
    base = os.path.join(out_dir, t0.strftime("%Y-%m-%d"), t0.strftime("%H%M%S"))
    paths = [base + "-snapshots.csv.gz", base + "-polls.csv.gz", base + "-machines.jsonl.gz"]
    _write_gz(paths[0], _csv_text(SNAP_COLS, snaps))
    _write_gz(paths[1], _csv_text(POLL_COLS, polls))
    _write_gz(paths[2], "".join(json.dumps(m) + "\n" for m in machines))
    return paths


def _num(v):
    if v in ("", None):
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() and "." not in v else f
    except ValueError:
        return v


def import_dir(db, data_dir):
    """Import every chunk under data_dir that hasn't been imported yet. Returns number of new files."""
    db.execute("CREATE TABLE IF NOT EXISTS imported_files (path TEXT PRIMARY KEY)")
    done = {r[0] for r in db.execute("SELECT path FROM imported_files")}
    new = 0
    for root, _, files in os.walk(data_dir):
        for f in sorted(files):
            rel = os.path.relpath(os.path.join(root, f), data_dir).replace("\\", "/")
            if rel in done or not f.endswith(".gz"):
                continue
            with gzip.open(os.path.join(root, f), "rt", encoding="utf-8", newline="") as fh:
                if f.endswith("-snapshots.csv.gz"):
                    rows = [[_num(v) for v in r] for r in list(csv.reader(fh))[1:]]
                    db.executemany(f"INSERT OR REPLACE INTO snapshots ({', '.join(SNAP_COLS)}) "
                                   f"VALUES ({', '.join('?' * len(SNAP_COLS))})", rows)
                elif f.endswith("-polls.csv.gz"):
                    rows = [[_num(v) for v in r] for r in list(csv.reader(fh))[1:]]
                    db.executemany(f"INSERT OR REPLACE INTO polls ({', '.join(POLL_COLS)}) "
                                   f"VALUES ({', '.join('?' * len(POLL_COLS))})", rows)
                elif f.endswith("-machines.jsonl.gz"):
                    for line in fh:
                        _upsert_machine(db, json.loads(line))
            db.execute("INSERT INTO imported_files VALUES (?)", (rel,))
            new += 1
    db.commit()
    return new


def _upsert_machine(db, m):
    cols = list(m)
    updates = ", ".join(f"{k}=excluded.{k}" for k in cols if k not in ("machine_id", "first_seen", "last_seen"))
    db.execute(
        f"INSERT INTO machines ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
        f"ON CONFLICT(machine_id) DO UPDATE SET {updates}, "
        f"first_seen=MIN(first_seen, excluded.first_seen), "
        f"last_seen=MAX(last_seen, excluded.last_seen)",
        list(m.values()))


def smooth(values, times, max_gap):
    """Median of each value and its two neighbours (when they are within max_gap seconds)."""
    out = []
    for i, v in enumerate(values):
        if 0 < i < len(values) - 1 and times[i] - times[i - 1] <= max_gap and times[i + 1] - times[i] <= max_gap:
            out.append(sorted((values[i - 1], v, values[i + 1]))[1])
        else:
            out.append(v)
    return out


def rebuild_events(db, interval=60):
    """Recompute events from smoothed snapshots (replaces the noisy live events)."""
    max_gap = interval * GAP_FACTOR
    db.execute("DELETE FROM events")
    rows = db.execute("SELECT machine_id, ts, rented_gpus, price_gpu_hr FROM snapshots ORDER BY machine_id, ts")
    events, cur, series = [], None, []

    def flush():
        if not series:
            return
        ts = [s[1] for s in series]
        rented = smooth([s[2] for s in series], ts, max_gap)
        price = smooth([s[3] for s in series], ts, max_gap)
        mid = series[0][0]
        events.append((ts[0], mid, "new_machine", rented[0], None, rented[0], price[0]))
        for i in range(1, len(series)):
            if ts[i] - ts[i - 1] > max_gap:
                events.append((ts[i - 1], mid, "gone", 0, rented[i - 1], None, price[i - 1]))
                events.append((ts[i], mid, "back", 0, None, rented[i], price[i]))
            d = rented[i] - rented[i - 1]
            if d:
                events.append((ts[i], mid, "rent" if d > 0 else "release", d, rented[i - 1], rented[i], price[i]))
            if abs(price[i] - price[i - 1]) > 1e-6:
                events.append((ts[i], mid, "price_change", price[i] - price[i - 1], price[i - 1], price[i], price[i]))

    for r in rows:
        if r[0] != cur:
            flush()
            cur, series = r[0], []
        series.append(r)
    flush()
    db.executemany("INSERT INTO events VALUES (?,?,?,?,?,?,?)", events)
    db.commit()
    return len(events)
