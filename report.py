"""
Analyse the data collected by monitor.py.

  python report.py                  # all data
  python report.py --hours 24       # last 24h only
  python report.py --csv out        # also export CSVs into ./out/

Outputs:
  1. Market overview per model (GPU type + VRAM): machines, GPUs, utilization, prices
  2. Per-machine stats: utilization %, rent events, idle time, est. gross revenue
  3. Pricing curve: utilization and revenue per GPU-hour by price bucket
     -> the price bucket with the best (price x utilization) is the suggested price
"""
import argparse
import csv
import os
import sqlite3
import statistics
import time
from collections import defaultdict

import vastdata

GAP_FACTOR = vastdata.GAP_FACTOR   # a snapshot covers time until the next one, up to GAP_FACTOR x poll interval


def load(db, since):
    snaps = db.execute(
        """SELECT ts, machine_id, gpu_name, gpu_ram_gb, total_gpus, free_gpus, rented_gpus,
                  price_gpu_hr, reliability, verification
           FROM snapshots WHERE ts >= ? ORDER BY machine_id, ts""", (since,)).fetchall()
    polls = [r[0] for r in db.execute("SELECT ts FROM polls WHERE ok=1 AND ts >= ? ORDER BY ts", (since,))]
    machines = {r[0]: r for r in db.execute(
        "SELECT machine_id, geolocation, cpu_ram_gb, disk_space_gb, inet_down, dlperf FROM machines")}
    return snaps, polls, machines


def poll_interval(polls):
    if len(polls) < 2:
        return 60
    return statistics.median(b - a for a, b in zip(polls, polls[1:]))


def by_machine_smoothed(snaps, interval):
    """Group snapshots per machine; rented_gpus is median-of-3 smoothed to drop API glitches."""
    by_m = defaultdict(list)
    for s in snaps:
        by_m[s[1]].append(list(s))
    for rows in by_m.values():
        rented = vastdata.smooth([r[6] for r in rows], [r[0] for r in rows], interval * GAP_FACTOR)
        for r, v in zip(rows, rented):
            r[6] = min(v, r[4])
            r[5] = r[4] - r[6]
    return by_m


def per_machine(snaps, interval):
    """Time-weighted stats per machine. Each snapshot is weighted by the time until the next one."""
    cap = interval * GAP_FACTOR
    out = {}
    for mid, rows in by_machine_smoothed(snaps, interval).items():
        gpu_h = rented_h = revenue = idle_h = observed_h = 0.0
        rents = 0
        for i, s in enumerate(rows):
            nxt = rows[i + 1][0] if i + 1 < len(rows) else s[0] + interval
            dt = min(nxt - s[0], cap) / 3600
            total, free, rented, price = s[4], s[5], s[6], s[7]
            if i and rented > rows[i - 1][6]:
                rents += rented - rows[i - 1][6]
            observed_h += dt
            gpu_h += total * dt
            rented_h += rented * dt
            revenue += rented * price * dt
            idle_h += free * dt
        last = rows[-1]
        out[mid] = {
            "machine_id": mid, "gpu_name": last[2], "vram_gb": last[3], "gpus": last[4],
            "observed_h": observed_h, "utilization": rented_h / gpu_h if gpu_h else 0,
            "idle_gpu_h": idle_h, "rented_gpu_h": rented_h,
            "gpus_rented_events": rents, "price_now": last[7],
            "price_avg": statistics.mean(r[7] for r in rows),
            "est_revenue": revenue, "rev_per_gpu_day": revenue / gpu_h * 24 if gpu_h else 0,
            "reliability": last[8], "verification": last[9],
        }
    return out


def pricing_curve(snaps, interval, step=0.1):
    """Group GPU-hours by (model, price bucket) -> utilization and revenue per available GPU-hour."""
    cap = interval * GAP_FACTOR
    buckets = defaultdict(lambda: [0.0, 0.0])   # (model, bucket) -> [gpu_h, rented_h]
    for rows in by_machine_smoothed(snaps, interval).values():
        for i, s in enumerate(rows):
            nxt = rows[i + 1][0] if i + 1 < len(rows) else s[0] + interval
            dt = min(nxt - s[0], cap) / 3600
            b = round(int(s[7] / step) * step, 2)
            key = (f"{s[2]} {s[3]:.0f}GB", b)
            buckets[key][0] += s[4] * dt
            buckets[key][1] += s[6] * dt
    rows = []
    for (model, b), (gh, rh) in sorted(buckets.items()):
        util = rh / gh if gh else 0
        mid_price = b + step / 2
        rows.append({"model": model, "price_from": b, "price_to": round(b + step, 2), "gpu_h": gh,
                     "utilization": util, "rev_per_gpu_h": mid_price * util})
    return rows


def model_key(m):
    return f"{m['gpu_name']} {m['vram_gb']:.0f}GB"


def print_table(rows, cols, title):
    print(f"\n=== {title} ===")
    if not rows:
        print("(no data)")
        return
    widths = [max(len(h), *(len(fmt(r[k], f)) for r in rows)) for h, k, f in cols]
    print("  ".join(h.ljust(w) for (h, _, _), w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(fmt(r[k], f).ljust(w) for (_, k, f), w in zip(cols, widths)))


def fmt(v, f):
    if v is None:
        return "-"
    return format(v, f) if f else str(v)


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="vast_a100.db")
    ap.add_argument("--hours", type=float, help="only analyse the last N hours")
    ap.add_argument("--csv", help="directory to export CSV files to")
    ap.add_argument("--bucket", type=float, default=0.1, help="price bucket size in $/GPU/hr")
    args = ap.parse_args()

    db = sqlite3.connect(args.db)
    since = int(time.time() - args.hours * 3600) if args.hours else 0
    snaps, polls, _ = load(db, since)
    if not snaps:
        print("No data yet. Run monitor.py first.")
        return
    interval = poll_interval(polls)
    span_h = (polls[-1] - polls[0]) / 3600 if len(polls) > 1 else 0
    print(f"Polls: {len(polls)} over {span_h:.1f}h (median interval {interval:.0f}s)")

    machines = per_machine(snaps, interval)

    # 1. market overview per model
    agg = defaultdict(lambda: defaultdict(float))
    prices = defaultdict(list)
    for m in machines.values():
        a = agg[model_key(m)]
        a["machines"] += 1
        a["gpus"] += m["gpus"]
        a["gpu_h"] += m["rented_gpu_h"] + m["idle_gpu_h"]
        a["rented_h"] += m["rented_gpu_h"]
        a["revenue"] += m["est_revenue"]
        a["rents"] += m["gpus_rented_events"]
        prices[model_key(m)].append(m["price_now"])
    overview = []
    for k, a in sorted(agg.items()):
        p = sorted(prices[k])
        overview.append({
            "model": k, "machines": int(a["machines"]), "gpus": int(a["gpus"]),
            "utilization": a["rented_h"] / a["gpu_h"] if a["gpu_h"] else 0,
            "price_min": p[0], "price_median": statistics.median(p), "price_max": p[-1],
            "rev_per_gpu_day": a["revenue"] / a["gpu_h"] * 24 if a["gpu_h"] else 0,
            "gpu_rent_events": int(a["rents"]),
        })
    print_table(overview, [
        ("Model", "model", ""), ("Machines", "machines", ""), ("GPUs", "gpus", ""),
        ("Util", "utilization", ".1%"), ("$min", "price_min", ".3f"), ("$median", "price_median", ".3f"),
        ("$max", "price_max", ".3f"), ("$/GPU/day", "rev_per_gpu_day", ".2f"),
        ("Rentals", "gpu_rent_events", "")], "Market overview ($ = per GPU per hour)")

    # 2. per machine
    ms = sorted(machines.values(), key=lambda m: (model_key(m), -m["utilization"]))
    print_table(ms, [
        ("Machine", "machine_id", ""), ("GPU", "gpu_name", ""), ("VRAM", "vram_gb", ".0f"),
        ("#", "gpus", ""), ("Seen h", "observed_h", ".1f"), ("Util", "utilization", ".1%"),
        ("Idle GPU-h", "idle_gpu_h", ".1f"), ("Rentals", "gpus_rented_events", ""),
        ("$/GPU/h", "price_now", ".3f"), ("Est. rev $", "est_revenue", ".2f"),
        ("$/GPU/day", "rev_per_gpu_day", ".2f"), ("Rel", "reliability", ".3f"),
        ("Verif", "verification", "")], "Per machine")

    # 3. pricing curve
    curve = pricing_curve(snaps, interval, args.bucket)
    print_table(curve, [
        ("Model", "model", ""), ("From $", "price_from", ".2f"), ("To $", "price_to", ".2f"),
        ("GPU-h", "gpu_h", ".1f"), ("Util", "utilization", ".1%"),
        ("Rev/GPU-h", "rev_per_gpu_h", ".3f")], "Pricing curve (utilization by price bucket)")

    print("\n=== Suggested price (bucket with highest revenue per available GPU-hour) ===")
    best = {}
    for r in curve:
        if r["gpu_h"] < 1:   # ignore buckets with too little data
            continue
        if r["model"] not in best or r["rev_per_gpu_h"] > best[r["model"]]["rev_per_gpu_h"]:
            best[r["model"]] = r
    for model, r in sorted(best.items()):
        print(f"{model:18} ${r['price_from']:.2f}-{r['price_to']:.2f}/GPU/h  "
              f"util {r['utilization']:.0%}  ~${r['rev_per_gpu_h'] * 24:.2f}/GPU/day gross")

    if args.csv:
        os.makedirs(args.csv, exist_ok=True)
        write_csv(os.path.join(args.csv, "overview.csv"), overview)
        write_csv(os.path.join(args.csv, "machines.csv"), ms)
        write_csv(os.path.join(args.csv, "pricing_curve.csv"), curve)
        specs = db.execute("SELECT * FROM machines").fetchall()
        cols = [d[0] for d in db.execute("SELECT * FROM machines").description]
        write_csv(os.path.join(args.csv, "machine_specs.csv"),
                  [dict(zip(cols, r)) for r in specs])
        print(f"\nCSV files written to {args.csv}/")


if __name__ == "__main__":
    main()
