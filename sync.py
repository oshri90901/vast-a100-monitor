"""
Pull the latest data collected by GitHub Actions and load it into the local DB.

  python sync.py            # git pull + import new chunks + rebuild events
  python sync.py --no-pull  # import only
"""
import argparse
import sqlite3
import subprocess

import vastdata
from monitor import DEFAULT_DB, SCHEMA


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--no-pull", action="store_true")
    args = ap.parse_args()

    if not args.no_pull:
        subprocess.run(["git", "pull", "-q", "--ff-only"], check=True)
    db = sqlite3.connect(args.db)
    db.executescript(SCHEMA)
    new = vastdata.import_dir(db, args.data_dir)
    events = vastdata.rebuild_events(db)
    polls, first, last = db.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM polls WHERE ok=1").fetchone()
    hours = (last - first) / 3600 if polls else 0
    print(f"Imported {new} new files | {polls} polls over {hours:.1f}h | {events} events rebuilt")


if __name__ == "__main__":
    main()
