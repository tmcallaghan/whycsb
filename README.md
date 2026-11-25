# whycsb

Python implementation of the YCSB (Yahoo! Cloud Serving Benchmark) for MongoDB-compatible databases. All of the features, none of the bloat.

## Features

- Single-file Python implementation (~800 lines)
- Standard YCSB workloads A-F with correct operation mixes
- Zipfian, uniform, and latest key distributions
- Multiprocessing for parallel load and execution
- Real-time throughput and latency reporting, including user-defined latency percentiles
- CSV output for metrics analysis

## Requirements

- Python 3.8+
- pymongo

## Installation

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install pymongo
```

Or install from requirements.txt:

```bash
pip install -r requirements.txt
```

## Usage

### Load Mode - load the data

```bash
python whycsb.py --uri mongodb://localhost:27017 --database ycsb --collection usertable --load --threads 16 --record-count 1000000 --batch-size 200
```

### Run Mode - execute a YCSB workload

```bash
# Run 100,000 operations
python whycsb.py --uri mongodb://localhost:27017 --database ycsb --collection usertable --run --threads 16 --workload A --operation-count 100000

# Run for 60 seconds
python whycsb.py --uri mongodb://localhost:27017 --database ycsb --collection usertable --run --threads 16 --workload A --run-seconds 60

# With rate limiting and CSV output
python whycsb.py --uri mongodb://localhost:27017 --database ycsb --collection usertable --run --threads 16 --workload A --run-seconds 60 --rate-limit 10000 --output-file results.csv
```

## YCSB Workloads

| Workload | Operations | Distribution | Use Case |
|----------|-----------|--------------|----------|
| A | 50% read, 50% update | Zipfian | Update heavy workload (session store) |
| B | 95% read, 5% update | Zipfian | Read mostly workload (photo tagging) |
| C | 100% read | Zipfian | Read only (user profile cache) |
| D | 95% read, 5% insert | Latest | Read latest workload (user status updates) |
| E | 95% scan, 5% insert | Zipfian | Short ranges (threaded conversations) |
| F | 50% read, 50% read-modify-write | Zipfian | Read-modify-write (user database) |

## Command-Line Options

### Connection
- `--uri` - MongoDB connection string (required)
- `--database` - Database name (required)
- `--collection` - Collection name (default: usertable)

### Mode
- `--load` - Load data mode
- `--run` - Run workload mode

### Common Parameters
- `--threads` - Number of worker threads (default: 1)
- `--record-count` - Number of records to load or dataset size (default: 1000)
- `--field-count` - Fields per document (default: 10)
- `--field-length` - Field length in bytes (default: 100)
- `--insert-order` - Key order: `ordered` (sequential, default) or `hashed` (FNV-hashed keys spread across the key space, like YCSB's `insertorder=hashed`). Use the same value for `--run` as was used for `--load`; a mismatch is detected and rejected

### Load-Specific
- `--batch-size` - Batch size for inserts (default: 100)

### Run-Specific
- `--workload` - YCSB workload A-F (required for run mode)
- `--operation-count` - Number of operations to execute
- `--run-seconds` - Duration in seconds to run
- `--max-scan-length` - Maximum scan length (default: 100)
- `--rate-limit` - Target ops/sec across all threads (0 = unlimited)

### Output
- `--output-file` - CSV file for performance metrics
- `--percentiles` - Comma separated latency percentiles to report (default: 50,90,95,99), e.g. `--percentiles 50,90,95,99`
- `--seed` - Random seed (default: 42)

## Document Schema

Documents follow YCSB's standard schema:

```json
{
  "_id": "user0000000000000123",
  "field0": "random100bytesofdata...",
  "field1": "random100bytesofdata...",
  ...
  "field9": "random100bytesofdata..."
}
```

Each document is approximately 1KB with 10 fields of 100 bytes each.

## Architecture

- **Multiprocessing**: Each worker is a separate process with its own MongoDB client. Workers are started with the `forkserver` method (pymongo is imported once in the fork server rather than in every worker), falling back to `spawn` where forkserver is unavailable (Windows)
- **Queue-based reporting**: Workers record every operation latency into a compact log-bucketed histogram (exact below 256 us, within 0.4% above) and send it once per second via multiprocessing.Queue
- **Reporter thread**: Continuously consumes the queue (blocking with a timeout, no long sleeps) and displays metrics every 10 seconds; a single reporter keeps up with 1000+ workers
- **Interval accuracy**: Workers tag stats with the 1-second time slot the operations completed in, so each 10 second interval (including the final partial one) reflects work actually done in it, independent of message arrival time
- **Inserts (workloads D and E)**: Key numbers come from a counter shared by all workers, continuing after any keys inserted by earlier runs, so repeated runs never collide; the `latest` distribution follows inserts from all workers, advancing only past keys whose inserts have completed (like YCSB's acknowledged counter)
- **Percentiles**: Nearest-rank percentiles are reported per interval and, at the end of the run, per operation type over the entire run
- **Latency measurement**: Only the database call is timed; keys, field values and new documents are generated before the timer starts (matching YCSB's `DBWrapper`)
- **Errors**: As in YCSB, an operation that raises a driver error, or a read/update/scan/read-modify-write that finds no document, is counted as `<op>-failed` (e.g. `read-failed`) and the worker carries on. Failed operations count toward throughput, have their own rows in the final latency table, and are excluded from the interval percentiles and the `all` row. The first error of each kind per worker is printed to stderr
- **Rate limiting**: Interval-based token bucket per worker
- **Key distributions**: Pure Python implementations (no numpy required)

## Example Workflow

```bash
# 1. Start MongoDB (Docker example)
docker run -d -p 27017:27017 mongo:latest

# 2. Load 1 million records
python whycsb.py --uri mongodb://localhost:27017 \
    --database ycsb --collection usertable \
    --load --threads 16 --record-count 1000000 --batch-size 1000

# 3. Run workload A for 60 seconds
python whycsb.py --uri mongodb://localhost:27017 \
    --database ycsb --collection usertable \
    --run --threads 16 --workload A --run-seconds 60 \
    --output-file workload_a.csv

# 4. Run other workloads
for wl in B C D E F; do
    python whycsb.py --uri mongodb://localhost:27017 \
        --database ycsb --collection usertable \
        --run --threads 16 --workload $wl --run-seconds 60 \
        --output-file workload_${wl}.csv
done
```

## Output Format

### Console Output

```
[2026-05-01T13:22:45.123Z] elapsed 00:00:10.000 | total ops 12,345 at 1,234.50 op/s | interval 1,250.00 op/s @ 8.00 ms | p50 4.85 p90 12.45 p95 18.37 p99 26.30 ms | last 10 1,242.25 op/s @ 8.05 ms
```

The final summary includes a latency table per operation type (plus `all` when the workload mixes operations):

```
Latency (ms):
  operation                 samples        avg        min        p50        p90        p95        p99        max
  read                       98,585       2.06       0.41       1.32       3.96       5.55      14.62     181.56
  update                     98,249       9.33       2.04       7.54      16.10      20.54      31.55     208.53
  all                       196,834       5.69       0.41       4.66      11.94      16.58      25.92     208.53
```

In load mode latencies (and percentiles) are per `insert_many` batch.

### CSV Output

Columns: timestamp, elapsed_time, elapsed_seconds, total_operations, overall_tps, interval_tps, interval_latency_ms, window_tps, window_latency_ms, followed by one `interval_pNN_latency_ms` column per requested percentile, and `interval_errors` (failed operations in the interval)

## References

- [YCSB GitHub](https://github.com/brianfrankcooper/YCSB)
- [YCSB Core Workloads](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads)
- [py-mongo-sysbench](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench)
