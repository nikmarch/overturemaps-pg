# Spatial Query Benchmark

Benchmarks for the "Optimizing Spatial Queries" blog article.

## Usage

From the repo root:

```bash
# Run with default divisions (Germany, Chile, Bretagne)
./benchmark.sh pages/optimizing-spatial-queries/run.py

# Restart PostgreSQL between queries (cold cache)
./benchmark.sh pages/optimizing-spatial-queries/run.py --restart

# Custom divisions
./benchmark.sh pages/optimizing-spatial-queries/run.py --divisions r51477,r60189
```

## Default Divisions

- `r51477` - Deutschland (159k vertices)
- `r167454` - Chile (207k vertices)
- `r60189` - Russia (209k vertices)

## Output

Results written to `pages/optimizing-spatial-queries/benchmark_results.md`:
- Summary table with timings and improvement ratio
- Full EXPLAIN ANALYZE output for each query

## Note

Slow queries can take 30+ minutes per division. Run in background:

```bash
nohup ./benchmark.sh pages/optimizing-spatial-queries/run.py --restart > benchmark.log 2>&1 &
tail -f benchmark.log
```
