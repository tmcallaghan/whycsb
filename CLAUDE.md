# CLAUDE.md — whycsb

## Project Overview

whycsb is a Python implementation of the YCSB (Yahoo! Cloud Serving Benchmark) for MongoDB API compatible databases. It is a single-script benchmark tool that can load data and execute standard YCSB workloads against any MongoDB-compatible endpoint (MongoDB, Amazon DocumentDB, Azure Cosmos DB, etc.).

## Goals

1. **Single script** — one Python file (`whycsb.py`) handles both data loading (`--load`) and benchmark execution (`--run`)
2. **Inspired by py-mongo-sysbench** — follow the architectural patterns from [py-mongo-sysbench](https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench): argparse CLI, multiprocessing workers, a shared performance queue, and a reporter thread
3. **Emulate YCSB** — implement the core YCSB workloads (A–F) with the same operation mix, field layout, and key distributions as [YCSB](https://github.com/brianfrankcooper/YCSB)
4. **Python multiprocessing for concurrency** — use `multiprocessing.Process` for workers and `multiprocessing.Manager().Queue()` for performance reporting, not threading
5. **Keep it simple** — minimal dependencies (pymongo only), no plugin architecture, no abstract classes

## Architecture

### Single-file layout

All code lives in `whycsb.py`. No packages, no modules, no config files beyond CLI args.

### Two modes

- `--load` — bulk-insert documents into the target collection using batched `insert_many` / `bulk_write`. Each worker owns a key range partition.
- `--run` — execute a YCSB workload against the loaded data. Each worker runs operations independently, choosing operation type by weighted random selection.

### Concurrency model (from py-mongo-sysbench)

- Workers are `multiprocessing.Process` instances, each with its own pymongo `MongoClient`
- A shared `multiprocessing.Manager().Queue()` carries performance messages from workers to a reporter
- A reporter thread in the main process drains the queue and prints periodic throughput/latency stats
- Rate limiting is per-worker using a token-bucket or interval-based approach

### YCSB workloads to implement

Each workload defines proportions of operations. The operations are:

| Operation | Description |
|-----------|-------------|
| read | Point read by key (`find_one`) |
| update | Read then update fields (`update_one`) |
| insert | Insert a new document (`insert_one`) |
| scan | Range scan from a start key (`find` with limit) |
| readmodifywrite | Read, modify in client, write back (read + update) |

Standard workloads:

| Workload | Mix | Distribution | Description |
|----------|-----|--------------|-------------|
| A | 50% read, 50% update | zipfian | Update heavy — session store |
| B | 95% read, 5% update | zipfian | Read mostly — photo tagging |
| C | 100% read | zipfian | Read only — user profile cache |
| D | 95% read, 5% insert | latest | Read latest — user status updates |
| E | 95% scan, 5% insert | zipfian | Short ranges — threaded conversations |
| F | 50% read, 50% readmodifywrite | zipfian | Read-modify-write — user database |

### Document schema

Match YCSB's default: 10 fields (`field0`–`field9`), each 100 random bytes, plus a string `_id` key like `"user6284781860667377211"`. Total record size ~1 KB.

### Key distributions

- **zipfian** — most accesses go to a small subset of keys (YCSB default for A/B/C/E/F)
- **uniform** — all keys equally likely
- **latest** — newest keys are most popular (workload D)

## CLI Interface

Follow the py-mongo-sysbench pattern for argument structure:

```
python whycsb.py --uri <connection-string> --database <db> --collection <col> \
    --load --threads <N> --record-count <R> --batch-size <B>

python whycsb.py --uri <connection-string> --database <db> --collection <col> \
    --run --threads <N> --workload <A|B|C|D|E|F> \
    --operation-count <O> | --run-seconds <S> \
    [--request-distribution zipfian|uniform|latest] \
    [--record-count <R>] [--field-count 10] [--field-length 100] \
    [--max-scan-length 100] [--rate-limit <ops/sec>]
```

## Key Design Decisions

- **pymongo is the only external dependency.** No numpy, no special stats libraries.
- **Zipfian distribution** must be implemented in pure Python (port the YCSB scrambled zipfian generator).
- **Each worker creates its own MongoClient** — pymongo clients are not fork-safe.
- **Performance reporting** uses a queue + reporter thread pattern, not shared counters, to avoid lock contention.
- **Output** goes to stdout and optionally to a CSV file, matching py-mongo-sysbench's dual-output approach.
- **No ORM or abstraction layer** — direct pymongo calls.

## Coding Conventions

- Python 3.8+ (f-strings, walrus operator OK)
- No type hints required but welcome
- Functions over classes where possible
- `argparse` for CLI
- `multiprocessing` for parallelism (not `threading`, not `concurrent.futures`)
- Keep functions short and focused
- Use `random.Random` instances per-worker (seeded) to avoid contention on the global random state

## Dependencies

- Python 3.8+
- pymongo

## Reference Materials

- YCSB: https://github.com/brianfrankcooper/YCSB
- YCSB Core Workloads: https://github.com/brianfrankcooper/YCSB/tree/master/workloads
- YCSB Wiki (Core Properties): https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties
- py-mongo-sysbench: https://github.com/aws-samples/amazon-documentdb-samples/tree/master/samples/py-mongo-sysbench
