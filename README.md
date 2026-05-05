# whycsb

Python implementation of the YCSB (Yahoo! Cloud Serving Benchmark) for MongoDB-compatible databases. All of the features, none of the bloat.

## Features

- Single-file Python implementation (~800 lines)
- Standard YCSB workloads A-F with correct operation mixes
- Zipfian, uniform, and latest key distributions
- Multiprocessing for parallel load and execution
- Real-time throughput and latency reporting
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

- **Multiprocessing**: Each worker is a separate process with its own MongoDB client
- **Queue-based reporting**: Workers send performance metrics via multiprocessing.Queue
- **Reporter thread**: Collects and displays metrics every 10 seconds
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
[2026-05-01T13:22:45.123Z] elapsed 00:00:10.000 | total ops 12,345 at 1,234.50 op/s | interval 1,250.00 op/s @ 8.00 ms | last 10 1,242.25 op/s @ 8.05 ms
```

### CSV Output

Columns: timestamp, elapsed_time, elapsed_seconds, total_operations, overall_tps, interval_tps, interval_latency_ms, window_tps, window_latency_ms

## References

- [YCSB GitHub](https://github.com/brianfrankcooper/YCSB)
- [YCSB Core Workloads](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads)
- [py-mongo-sysbench](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench)
