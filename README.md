# Vast.ai A100 market monitor

Tracks every A100 offer on Vast.ai (the same data as cloud.vast.ai/create filtered to A100)
once a minute: per-machine specs, VRAM, how many GPUs are rented vs free, and the hourly price.
The goal is to measure utilization and find the right hourly price.

## How it works
- `.github/workflows/collect.yml` runs `monitor.py` on GitHub Actions around the clock and commits
  a gzip data chunk to `data/YYYY-MM-DD/` every 30 minutes.
- Rented GPUs are inferred from `gpu_ids`: GPUs that appear only in non-rentable offers are rented.
- The anonymous API caps results, so each poll splits the query into machine_id ranges.
  Optional: add a Vast.ai API key as repo secret `VAST_API_KEY`.

## Local analysis
```
python sync.py                      # git pull + load data/ into vast_a100.db
python report.py --hours 168 --csv reports
```
`report.py` prints a market overview per model, per-machine utilization/revenue and a
price-vs-utilization curve, and exports CSVs.

Local collection without GitHub: `python monitor.py` (polls every 60s into vast_a100.db).

## RTX 4090
A second workflow, `.github/workflows/collect-4090.yml`, tracks RTX 4090 offers every 10 minutes
into `data-4090/` (the 4090 market is ~1,000 machines, ~180 API calls per scan).
```
python sync.py --data-dir data-4090 --db vast_4090.db
python report.py --db vast_4090.db --bucket 0.05 --hours 168 --csv reports-4090
```
