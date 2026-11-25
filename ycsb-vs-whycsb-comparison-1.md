# YCSB vs whycsb comparison #1: workload A on Amazon DocumentDB

**Date:** 2026-11-25
**whycsb version:** commit `2f98a07` (main, after PR #1: percentile tracking and the 4 bug fixes)
**YCSB version:** 0.17.0 release, `ycsb-mongodb-binding-0.17.0` (mongo-java-driver 3.8.0)

## Summary

whycsb gives the same throughput as real YCSB within about ±2% wherever the cluster held steady. Latency percentiles agree within a few percent at 10, 100 and 500 threads on both a serverless and an instance-based DocumentDB cluster. The gaps that remain are small and come from the client side; whycsb doesn't misrepresent the database.

- **Throughput:** steady-state throughput is within ±2% in 4 of the 6 cells. The serverless/100 cell (+9.7%) comes from the cluster scaling up during the run sequence; the final back-to-back pair differs by +0.5%. The instance/500 cell (−4.6%) fell within run-to-run noise in follow-up runs (−2.6%, and whycsb runs ranged 3,338–3,703 against YCSB's 3,494).
- **Read latency:** in this matrix whycsb's read p50 was about 3–4% (20–25 µs) higher than YCSB's. A follow-up rerun with 3 rounds per tool **did not reproduce it**: whycsb read p50 was equal to or slightly below YCSB's (see the [addendum](#addendum-follow-up-rerun-and-whycsb-changes)). Treat it as run-to-run noise, not a consistent whycsb overhead. Update latency is the same.
- **Work sent to the database:** in all 24 measured runs, the primary's `serverStatus` op counters match each tool's reported reads and updates exactly (whycsb +1 query, from its startup insert-order check). No run had errors.
- **Client cost:** whycsb uses 5–10× the client CPU (up to 10.5% of 16 cores vs about 2%) and holds twice as many connections on the primary (1,005 vs 506 at 500 threads). Neither limited these tests.
- **Cluster behaviour:** both tools show the same contention collapse from 100 to 500 threads. Throughput falls, and update p99 reaches about 3 s (serverless) and about 9 s (instance), from zipfian hot-key update contention (the hottest key receives 3.8% of all operations).

## Test environment

### Client

- **Machine:** one Linux ARM64 host, 16 vCPU, 123 GiB RAM, running both tools (never at the same time).
- **whycsb:** Python 3.13.5, pymongo 4.10.1 (C extensions present).
- **YCSB:** OpenJDK 21.0.13, YCSB 0.17.0 run as `java -cp "lib/*" site.ycsb.Client ...` (no launcher script), logback set to WARN.

### Clusters

| Cluster | Type | Primary | Primary AZ | Other members | Storage | Parameter group |
|---|---|---|---|---|---|---|
| `ddb8-serverless` ("serverless") | DocumentDB 8.0.3 serverless, 4–256 DCU | `ddb8-serverless-p` | us-east-1c | `rr-1` (1c) | I/O-Optimized | `tls-disabled-80` |
| `ddb803` ("instance") | DocumentDB 8.0.3 instance-based | `ddb803-p`, db.r8g.xlarge (4 vCPU, 32 GiB) | us-east-1c | `rr-2` (1a), `rr-3` (1b), r8g.xlarge | I/O-Optimized | `tls-disabled-80` |

Both tools connect to the cluster endpoint, and all operations go to the primary (read preference primary).

## Workload and dataset

- **Workload:** YCSB workload A, 50% read and 50% update, `requestdistribution=zipfian`, `readallfields=true`, `writeallfields=false`.
- **Records:** 1,000,000 per tool: YCSB in `ycsb_cmp.usertable`, whycsb in `whycsb_cmp.usertable`. Each tool loaded its own data with its own load phase.
- **In-memory:** both datasets together (about 2.4 GB) fit in memory on both clusters, even with serverless at its 4 DCU (about 8 GiB) minimum. A 60 s warm-up of each tool at 100 threads ran on each cluster before measuring.

### Datasets as loaded (verified with `collStats` and key sampling)

| | YCSB (`ycsb_cmp`) | whycsb (`whycsb_cmp`) |
|---|---|---|
| documents | 1,000,000 | 1,000,000 |
| avgObjSize | 1,236 B | 1,226 B |
| size | 1.150 GB | 1.142 GB |
| index size | 0.061 GB | 0.066 GB |
| indexes | `_id_` only | `_id_` only |
| fields | `field0`–`field9` | `field0`–`field9` |
| value type / length | BSON Binary, 100 bytes | string, 100 characters (same 105-byte BSON size) |
| `_id` format | `"user" + fnv64(keynum)` (YCSB default `insertorder=hashed`, `zeropadding=1`) | `"user" + fnv64(keynum)` zero-filled to 16 (`--insert-order hashed`) |
| expected run-phase keys present (2,000-key sample) | 2000/2000 | 2000/2000 |

The two tools hash differently: YCSB uses FNV-1 over the 8 bytes of the key number, whycsb uses FNV-1a over its decimal string. Both spread keys uniformly, and the key-length difference is about 1 byte per operation.

### Load phase

| Cluster | Tool | Threads | Batch size | Load throughput |
|---|---|---|---|---|
| serverless | YCSB | 40 | 100 | 50,363 docs/s |
| serverless | whycsb | 40 | 100 | 53,251 docs/s |
| instance | YCSB | 40 | 100 | 54,401 docs/s |
| instance | whycsb | 40 | 100 | 53,794 docs/s |

YCSB's MongoDB binding never flushes a thread's final partial batch when `batchsize > 1`. 40 threads × 25,000 records divides evenly by 100, so no documents were lost, and the counts confirm 1,000,000.

## Making the comparison fair

### What each tool sends to the database (from YCSB 0.17.0 source and whycsb.py)

| | YCSB `MongoDbClient` | whycsb |
|---|---|---|
| read | `find({_id: key}).first()`, no projection → `limit 1, singleBatch` | `find_one({_id: key})`, no projection → `limit 1, singleBatch` |
| update | `updateOne({_id: key}, {$set: {fieldN: <100 random bytes>}})`, one random field, no upsert | `update_one({_id: key}, {$set: {fieldN: <100-char string>}})`, one random field, no upsert |
| write concern | default (acknowledged) | default (acknowledged) |
| retryable writes | `retryWrites=false` (also the Java 3.8 default) | `retryWrites=false` (pymongo's default would be true) |
| sessions | implicit `lsid` | implicit `lsid` |
| read concern / compression | none / none | none / none |
| concurrency | N threads in one JVM sharing one `MongoClient` | N processes, one `MongoClient` each |

An independent review (a subagent reading both code bases without seeing these conclusions) agreed on all of the above.

### Configuration changes for fairness

- **Connection pool:** YCSB's `mongodb.url` sets `maxPoolSize=<threads>`. The Java driver's default pool of 100 would cap the 500-thread run at 100 in-flight operations.
- **Retryable writes:** `retryWrites=false` on both URIs.
- **Logging:** YCSB's logback set to WARN. With no config the Java driver logs at DEBUG, which would slow YCSB down.
- **Key order:** whycsb run with `--insert-order hashed`, which is YCSB's default.

### Key distribution check

I ported YCSB's `ScrambledZipfianGenerator` and `CoreWorkload.nextKeynum()` to Python exactly, including Java 64-bit `fnvhash64` semantics and YCSB's `recordcount + 1` item count with rejection. Its popularity profile over 2,000,000 draws on 1,000,000 keys matches whycsb's:

| share of accesses going to | YCSB | whycsb |
|---|---|---|
| hottest key | 3.77% | 3.77% |
| top 10 keys | 11.79% | 11.79% |
| top 100 | 20.76% | 20.75% |
| top 1,000 | 30.01% | 29.99% |
| top 10,000 | 40.28% | 40.10% |
| top 100,000 | 59.15% | 59.10% |
| distinct keys touched | 65.95% | 65.84% |

So the two tools generate the same hot-key update contention.

### Server-side verification

Before and after every run, the harness read `serverStatus.opcounters` from the primary through a direct connection. The `query` and `update` deltas match each tool's reported read and update counts exactly in every measured run (see the per-run tables). whycsb's +1 query is its startup check of which insert order the data was loaded with. This also shows nothing else was running against the clusters during the tests.

## Method

- **Order:** per cluster (serverless first, then instance): warm-up (YCSB then whycsb, 100 threads, 60 s), then for each of 10, 100 and 500 threads: YCSB r1, whycsb r1, whycsb r2, YCSB r2. Each measured run lasted 120 s. The alternating order keeps drift from favouring either tool.
- **Steady-state throughput:** the mean of per-interval throughput over full 10 s intervals ending after 20 s. That's the YCSB `-s` status lines and the whycsb CSV, excluding ramp-up and the final partial interval. Use it rather than the tools' overall figures, because whycsb's overall time includes about 6 s of process spawning at 500 threads.
- **Latency percentiles:** whole run, from each tool's own final report: YCSB HdrHistogram (`measurementtype=hdrhistogram`, 3 significant digits) and whycsb's log-bucketed histogram (≤0.4% error). Both use nearest-rank-style percentiles.
- **Client CPU:** `vmstat 5` us+sy, averaged over the run.
- **Failures:** every run exited 0; there were no `-FAILED` operations in YCSB logs and no tracebacks in whycsb logs.

## Results

### Summary: steady-state throughput (op/s, mean of r1 and r2)

| Cluster | Threads | YCSB | whycsb | whycsb vs YCSB | Notes |
|---|---|---|---|---|---|
| serverless | 10 | 6,833 | 6,926 | +1.4% | |
| serverless | 100 | 14,974 | 16,431 | +9.7% | drift: the cluster scaled up across the sequence (runs in order: YCSB 13,150 → whycsb 15,985 → whycsb 16,878 → YCSB 16,798); final back-to-back pair +0.5% |
| serverless | 500 | 10,933 | 11,102 | +1.5% | |
| instance | 10 | 4,472 | 4,486 | +0.3% | |
| instance | 100 | 6,109 | 6,105 | −0.1% | |
| instance | 500 | 3,484 | 3,324 | −4.6% | consistent in both reps; follow-up runs below put it within noise |

### Summary: latency (ms, whole run, mean of r1 and r2), YCSB / whycsb

| Cluster | Threads | read p50 | read p95 | read p99 | update p50 | update p95 | update p99 |
|---|---|---|---|---|---|---|---|
| serverless | 10 | 0.556 / 0.575 | 0.792 / 0.810 | 0.995 / 1.010 | 2.19 / 2.16 | 3.21 / 3.13 | 5.20 / 5.16 |
| serverless | 100 | 0.689 / 0.710 | 1.56 / 1.40 | 2.59 / 2.15 | 3.56 / 3.17 | 8.93 / 7.66 | 318 / 282 |
| serverless | 500 | 0.726 / 0.790 | 2.58 / 2.67 | 5.10 / 5.03 | 3.12 / 3.19 | 12.3 / 10.5 | 2,956 / 2,875 |
| instance | 10 | 0.669 / 0.695 | 1.44 / 1.47 | 2.26 / 2.34 | 3.40 / 3.30 | 6.42 / 6.34 | 10.0 / 10.1 |
| instance | 100 | 1.337 / 1.385 | 4.57 / 4.68 | 7.06 / 7.38 | 9.51 / 9.51 | 22.4 / 22.5 | 765 / 770 |
| instance | 500 | 1.93 / 1.99 | 8.79 / 8.80 | 15.7 / 14.9 | 13.8 / 14.3 | 81.7 / 42.4 | 8,880 / 9,208 |

The serverless/100 latencies are affected by the same scaling drift as its throughput. Instance/500 update p95 is noisy for YCSB: its own two runs differ by 61%.

### Follow-up: instance cluster, 500 threads, connection mode

pymongo in every whycsb process discovers the replica set and monitors every member, while YCSB's Java 3.8 driver connects to the single cluster endpoint directly. To test whether that explained the −4.6% gap, I ran 4 more back-to-back 120 s runs:

| order | tool | connection mode | steady-state op/s | overall op/s | read p50 | read p99 | update p50 | update p99 | primary connections (mid-run) | client CPU % |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | whycsb | directConnection=true | 3,338 | 3,538 | 1.960 | 14.94 | 14.18 | 9,208 | 1,005 | 5.2 |
| 2 | YCSB | default | 3,494 | 3,715 | 1.778 | 16.21 | 13.51 | 8,667 | 506 | 0.4 |
| 3 | whycsb | default (replica-set discovery) | 3,404 | 3,643 | 1.910 | 14.69 | 13.73 | 8,880 | 1,005 | 5.8 |
| 4 | whycsb | directConnection=true | 3,703 | 3,899 | 1.710 | 13.02 | 12.96 | 8,421 | 1,005 | 5.3 |

- **Throughput:** whycsb ranged from 3,338 to 3,703 op/s against YCSB's 3,494; the default-mode whycsb run is −2.6%. The 500-thread gap on this cluster is within run-to-run variation, and `directConnection=true` doesn't change it.
- **Connections:** whycsb holds 1,005 connections on the primary in both modes (500 application plus 500 monitoring, one of each per process); YCSB holds 506 (a 500-connection pool plus monitoring). That's the one structural difference, and on a 4-vCPU primary it could plausibly cost a few percent.

## Findings

### Where whycsb and YCSB agree

1. Throughput matches within about ±2% whenever the cluster is steady, at low, medium and high concurrency.
2. Latency percentiles match within a few percent, including the tail.
3. Both tools generate identical work: same operations, same payload sizes, same key popularity, confirmed against the server's op counters.
4. Both expose the same database behaviour. Going from 100 to 500 threads cuts throughput (instance 6.1k → 3.5k op/s; serverless about 16k → 11k), and update p99 grows to seconds as writers queue on the hottest zipfian keys.

### Where they differ

| # | Difference | Effect seen | Notes |
|---|---|---|---|
| 1 | whycsb's timer also covered key and value generation, while YCSB times only the `db.read` / `db.update` call (`DBWrapper`) | read p50 +3–4% in this matrix, but not reproduced in the follow-up rerun (noise). A paired test measured the timer-scope part at 2.3 µs per read and 18.4 µs per update | **Fixed** after this test: whycsb now times only the database call (see addendum) |
| 2 | Client CPU | whycsb 1–10.5% of 16 cores vs 0.2–2.3% for YCSB (5–10×) | Not a bottleneck here; it would be at much higher op rates |
| 3 | Connections on the primary | whycsb 1,005 vs YCSB 506 at 500 threads | One application plus one monitoring connection per whycsb process |
| 4 | YCSB startup outliers | At 500 threads YCSB reads of 0.58–4.7 s, all in the first 10 s (later intervals max out at 22–50 ms), as the shared driver opens and authenticates 500 pooled connections inside timed ops | Inflates YCSB's average and max at 500 threads (e.g. instance read avg 4.32 vs 3.13 ms), not its percentiles. whycsb's worst read was 61–66 ms (serverless) and 213–269 ms (instance) |
| 5 | Ramp time | whycsb spawning 500 processes adds about 6 s (wall time 126 s vs 122 s) | Handled by comparing steady-state intervals |
| 6 | Error handling | An op error killed a whycsb worker for the rest of the run; YCSB counts it under `*-FAILED` and carries on | No errors happened in these tests. **Fixed** after this test: whycsb now counts `<op>-failed` and carries on (see addendum) |

### YCSB first-interval read outliers at 500 threads

| Run | 0–10 s read max | 10–20 s read max | 20–30 s read max |
|---|---|---|---|
| instance YCSB r1 | 4,722.7 ms | 46.9 ms | 49.6 ms |
| instance YCSB r2 | 579.6 ms | 43.3 ms | 31.6 ms |
| serverless YCSB r1 | 1,420.3 ms | 22.0 ms | 22.8 ms |

## Recommendations for whycsb

1. ~~**Narrow the latency timer** to cover only the database call, generating the key and update value before starting the timer, so the measurement matches YCSB's `DBWrapper`.~~ Done (see addendum).
2. ~~**Catch operation errors** in `run_worker`, count them per operation type (like YCSB's `READ-FAILED` / `UPDATE-FAILED`) and keep going, rather than letting one error end a worker.~~ Done (see addendum).
3. When comparing against YCSB, **use steady-state interval throughput**, not either tool's overall figure.

## Addendum: follow-up rerun and whycsb changes

After this comparison, whycsb was changed in three ways, and the instance cluster (`ddb803`, chosen because it doesn't auto-scale) was used to measure the effect.

### Changes

1. **Latency timer covers only the database call.** Keys, update values, read-modify-write values and insert documents are built before the timer starts, matching YCSB's `DBWrapper`.
2. **YCSB-style error counting.** A driver error (`PyMongoError`), or a read, update, scan or read-modify-write that finds no document, is recorded as `<op>-failed` and the worker carries on. Failed ops count toward throughput, have their own rows in the final latency table, and are excluded from the interval percentiles and the `all` row. The CSV gets an `interval_errors` column.
3. **Workload D "latest" fix**, found by the new error counting. The shared latest-key marker could move past a key that another worker had allocated but not yet inserted, so about 1 in 7,000 latest reads missed (12 and 3 `read-failed` in two 8-thread runs of about 90k reads). whycsb now uses a port of YCSB's `AcknowledgedCounterGenerator`: the marker advances only across a contiguous run of acknowledged insert keys. After the fix, D runs at 16 and 500 threads had 0 `read-failed` (315,636 and 421,445 reads).

Error-handling checks on the cluster: reads of missing keys (`--record-count 2000000` over 1M docs) gave 148,179 `read-failed` + 144,288 `read`, equal to the server's query delta. A deliberate `socketTimeoutMS=3` run counted 5,701 failures with every worker continuing and exit code 0.

### Paired timer-scope test (exact effect of change 1)

A single process ran real operations and timed each one both ways: old scope (preparation + database call) and new scope (database call only). Because each pair is the same request, cluster noise cancels out.

| Workload | Operation | Samples | Old avg (ms) | New avg (ms) | Removed from timer | p50 old → new (ms) | p99 old → new (ms) |
|---|---|---|---|---|---|---|---|
| A | read | 11,928 | 0.4758 | 0.4735 | 2.3 µs | 0.472 → 0.470 | 0.710 → 0.706 |
| A | update | 11,796 | 2.0581 | 2.0397 | 18.4 µs | 2.011 → 1.996 | 2.967 → 2.951 |
| F | read | 9,174 | 0.4924 | 0.4900 | 2.3 µs | 0.478 → 0.474 | 0.805 → 0.801 |
| F | readmodifywrite | 9,059 | 2.8090 | 2.6556 | 153.4 µs | 2.679 → 2.519 | 6.671 → 6.543 |

This matches the client-side cost of preparation measured on its own: 1.5 µs (read), 17.0 µs (update), 151 µs (read-modify-write and insert, which generate all 10 field values).

### Benchmark rerun: YCSB vs whycsb before (old) vs after (new), instance cluster

Each tool ran 3 times per thread count for 60 s, with the order rotated each round, after a warm-up. The server's op counters matched every run (whycsb +1 query from its startup check). Values are the mean of 3 runs, [min..max] in brackets. The new whycsb here has changes 1 and 2; change 3 doesn't affect workloads A or F.

**Workload A, 10 threads**

| Metric | YCSB | whycsb old | whycsb new | new vs old |
|---|---|---|---|---|
| steady-state op/s | 4,471 [4,379..4,535] | 4,437 [4,284..4,632] | 4,452 [4,269..4,609] | +0.3% |
| read p50 (ms) | 0.698 [0.692..0.704] | 0.687 [0.630..0.730] | 0.690 [0.660..0.720] | +0.5% |
| read p99 (ms) | 2.808 [2.707..2.913] | 2.387 [2.100..2.540] | 2.467 [2.250..2.680] | +3.4% |
| update p50 (ms) | 3.387 [3.331..3.483] | 3.320 [3.160..3.530] | 3.337 [3.240..3.510] | +0.5% |
| update p99 (ms) | 11.03 [10.83..11.38] | 10.34 [9.38..11.62] | 10.21 [10.02..10.46] | −1.3% |

**Workload A, 100 threads**

| Metric | YCSB | whycsb old | whycsb new | new vs old |
|---|---|---|---|---|
| steady-state op/s | 5,901 [5,797..6,032] | 6,097 [6,028..6,157] | 5,832 [5,274..6,246] | −4.3% (one low run) |
| read p50 (ms) | 1.328 [1.273..1.414] | 1.353 [1.300..1.430] | 1.377 [1.240..1.540] | +1.7% |
| read p99 (ms) | 7.708 [7.291..8.223] | 8.230 [8.180..8.290] | 8.220 [7.700..8.670] | −0.1% |
| update p50 (ms) | 9.474 [9.007..9.991] | 9.223 [8.860..9.820] | 9.633 [8.930..10.590] | +4.4% |
| update p99 (ms) | 776.2 [757.2..797.2] | 754.3 [735.2..788.5] | 783.0 [739.3..841.7] | +3.8% |

**Workload F, read-modify-write latency (whycsb only).** YCSB's read-modify-write `$set`s one field while whycsb's replaces the whole document, so the two aren't comparable.

| Threads | Metric | whycsb old | whycsb new | new − old |
|---|---|---|---|---|
| 10 | p50 (ms) | 4.100 [3.86..4.43] | 3.923 [3.83..4.01] | −177 µs (−4.3%) |
| 10 | p99 (ms) | 12.23 [11.49..13.15] | 11.66 [11.17..12.06] | −577 µs (−4.7%) |
| 100 | p50 (ms) | 13.17 [12.00..14.88] | 12.60 [12.13..13.22] | −573 µs (−4.4%) |
| 100 | p99 (ms) | 858.1 [792.6..944.1] | 863.6 [837.6..886.8] | +0.6% |

### What the rerun shows

- On workload A the timer change is too small to see on the cluster: about 17 µs per update and 2 µs per read, against run-to-run variation of several percent. Every old-vs-new difference in the A tables is within the overlapping run ranges. The paired test gives the exact effect.
- On workload F the change is visible: read-modify-write p50 drops by about 180 µs at 10 threads, consistent with the 153 µs measured in the paired test, though still about the size of the run-to-run spread.
- The earlier read-latency gap against YCSB didn't reproduce. Old and new whycsb read p50 were both slightly below YCSB's at 10 threads (0.687 / 0.690 vs 0.698 ms).

## Detailed results

### Per-run results (latencies in ms; server Δ = server opcounter delta minus client-reported count)

Runs are listed in the order they ran. Warm-up runs are not included in the summaries.

#### serverless

| order | threads | tool | tag | steady-state op/s | overall op/s | read p50 | read p99 | read max | update p50 | update p99 | update max | server query Δ vs client | server update Δ vs client | client CPU % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | ycsb | warmup | 9,631 | 9,974 | 0.906 | 5.899 | 811.0 | 5.155 | 459.8 | 4,968 | +0 | +0 | 1.7 |
| 2 | 100 | whycsb | warmup | 11,624 | 11,149 | 0.930 | 5.360 | 275.1 | 4.660 | 408.6 | 4,846 | +1 | +0 | 8.3 |
| 3 | 10 | ycsb | r1 | 6,809 | 6,780 | 0.558 | 1.012 | 58.9 | 2.213 | 5.2 | 64 | +0 | +0 | 1.1 |
| 4 | 10 | whycsb | r1 | 6,790 | 6,736 | 0.590 | 1.010 | 24.6 | 2.220 | 5.2 | 55 | +1 | +0 | 2.1 |
| 5 | 10 | whycsb | r2 | 7,063 | 7,024 | 0.560 | 1.010 | 16.4 | 2.100 | 5.1 | 46 | +1 | +0 | 2.1 |
| 6 | 10 | ycsb | r2 | 6,858 | 6,893 | 0.554 | 0.978 | 62.0 | 2.167 | 5.2 | 67 | +0 | +0 | 1.0 |
| 7 | 100 | ycsb | r1 | 13,150 | 13,076 | 0.756 | 3.483 | 607.2 | 3.929 | 358.9 | 3,766 | +0 | +0 | 2.1 |
| 8 | 100 | whycsb | r1 | 15,985 | 15,923 | 0.720 | 2.260 | 67.7 | 3.350 | 285.7 | 2,541 | +1 | +0 | 9.7 |
| 9 | 100 | whycsb | r2 | 16,877 | 16,898 | 0.700 | 2.040 | 64.0 | 2.980 | 277.5 | 2,328 | +1 | +0 | 10.1 |
| 10 | 100 | ycsb | r2 | 16,798 | 16,825 | 0.621 | 1.694 | 211.2 | 3.187 | 277.5 | 2,683 | +0 | +0 | 2.3 |
| 11 | 500 | ycsb | r1 | 11,026 | 11,043 | 0.740 | 5.443 | 1,420.3 | 3.091 | 2,969.6 | 22,397 | +0 | +0 | 1.6 |
| 12 | 500 | whycsb | r1 | 11,103 | 11,326 | 0.790 | 5.070 | 61.1 | 3.240 | 2,891.8 | 23,157 | +1 | +0 | 10.4 |
| 13 | 500 | whycsb | r2 | 11,101 | 11,431 | 0.790 | 4.980 | 65.8 | 3.140 | 2,859.0 | 23,076 | +1 | +0 | 10.5 |
| 14 | 500 | ycsb | r2 | 10,840 | 11,145 | 0.711 | 4.747 | 341.2 | 3.139 | 2,943.0 | 24,035 | +0 | +0 | 1.7 |

#### instance

| order | threads | tool | tag | steady-state op/s | overall op/s | read p50 | read p99 | read max | update p50 | update p99 | update max | server query Δ vs client | server update Δ vs client | client CPU % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | ycsb | warmup | 6,397 | 6,281 | 1.336 | 7.247 | 926.2 | 9.239 | 735.7 | 4,424 | +0 | +0 | 1.1 |
| 2 | 100 | whycsb | warmup | 5,760 | 5,748 | 1.490 | 7.890 | 120.9 | 9.950 | 788.5 | 4,875 | +1 | +0 | 4.5 |
| 3 | 10 | ycsb | r1 | 4,374 | 4,266 | 0.641 | 2.075 | 61.7 | 3.573 | 10.4 | 66 | +0 | +0 | 0.2 |
| 4 | 10 | whycsb | r1 | 4,323 | 4,284 | 0.680 | 2.250 | 20.9 | 3.460 | 10.5 | 71 | +1 | +0 | 1.2 |
| 5 | 10 | whycsb | r2 | 4,649 | 4,574 | 0.710 | 2.420 | 22.0 | 3.140 | 9.7 | 39 | +1 | +0 | 1.0 |
| 6 | 10 | ycsb | r2 | 4,569 | 4,475 | 0.698 | 2.453 | 59.3 | 3.223 | 9.7 | 68 | +0 | +0 | 0.2 |
| 7 | 100 | ycsb | r1 | 6,144 | 6,075 | 1.347 | 7.019 | 220.0 | 9.583 | 759.3 | 4,801 | +0 | +0 | 0.9 |
| 8 | 100 | whycsb | r1 | 5,810 | 5,758 | 1.440 | 7.570 | 68.2 | 10.080 | 809.0 | 4,771 | +1 | +0 | 3.9 |
| 9 | 100 | whycsb | r2 | 6,400 | 6,328 | 1.330 | 7.180 | 77.5 | 8.930 | 731.1 | 4,738 | +1 | +0 | 4.7 |
| 10 | 100 | ycsb | r2 | 6,074 | 6,041 | 1.327 | 7.099 | 130.9 | 9.431 | 770.0 | 6,943 | +0 | +0 | 0.8 |
| 11 | 500 | ycsb | r1 | 3,480 | 3,555 | 1.978 | 15.231 | 4,722.7 | 13.911 | 8,912.9 | 50,528 | +0 | +0 | 0.4 |
| 12 | 500 | whycsb | r1 | 3,307 | 3,466 | 2.070 | 15.390 | 269.5 | 14.560 | 9,338.9 | 61,430 | +1 | +0 | 5.5 |
| 13 | 500 | whycsb | r2 | 3,341 | 3,537 | 1.910 | 14.500 | 213.2 | 14.050 | 9,076.7 | 69,635 | +1 | +0 | 5.6 |
| 14 | 500 | ycsb | r2 | 3,488 | 3,686 | 1.880 | 16.207 | 579.6 | 13.687 | 8,847.4 | 71,238 | +0 | +0 | 0.5 |

### Per-cell summaries (mean of r1 and r2; "run range" = (max − min) / min across the two runs)

#### serverless, 10 threads

| metric | YCSB (mean of r1, r2) | YCSB run range | whycsb (mean of r1, r2) | whycsb run range | whycsb vs YCSB |
|---|---|---|---|---|---|
| steady-state op/s | 6,833 | 0.7% | 6,926 | 3.9% | +1.4% |
| overall op/s | 6,837 | 1.6% | 6,880 | 4.2% | +0.6% |
| steady-state avg latency (all ops, ms) | 1.463 | 0.7% | 1.443 | 4.1% | -1.4% |
| read avg (ms) | 0.591 | 1.4% | 0.605 | 1.7% | +2.4% |
| read p50 (ms) | 0.556 | 0.7% | 0.575 | 5.2% | +3.4% |
| read p95 (ms) | 0.792 | 1.4% | 0.810 | 2.5% | +2.3% |
| read p99 (ms) | 0.995 | 3.4% | 1.010 | 0.0% | +1.5% |
| update avg (ms) | 2.33 | 1.8% | 2.29 | 4.8% | -1.5% |
| update p50 (ms) | 2.190 | 2.1% | 2.160 | 5.6% | -1.4% |
| update p95 (ms) | 3.21 | 1.0% | 3.13 | 1.9% | -2.6% |
| update p99 (ms) | 5.2 | 0.7% | 5.2 | 0.6% | -0.8% |

#### serverless, 100 threads

| metric | YCSB (mean of r1, r2) | YCSB run range | whycsb (mean of r1, r2) | whycsb run range | whycsb vs YCSB |
|---|---|---|---|---|---|
| steady-state op/s | 14,974 | 24.4% | 16,431 | 5.4% | +9.7% |
| overall op/s | 14,950 | 25.1% | 16,411 | 5.9% | +9.8% |
| steady-state avg latency (all ops, ms) | 6.778 | 24.4% | 6.088 | 5.4% | -10.2% |
| read avg (ms) | 0.840 | 32.4% | 0.815 | 3.7% | -2.9% |
| read p50 (ms) | 0.689 | 19.6% | 0.710 | 2.8% | +3.1% |
| read p95 (ms) | 1.558 | 54.3% | 1.400 | 5.7% | -10.1% |
| read p99 (ms) | 2.588 | 69.1% | 2.150 | 10.2% | -16.9% |
| update avg (ms) | 12.74 | 24.5% | 11.30 | 6.1% | -11.3% |
| update p50 (ms) | 3.558 | 20.9% | 3.165 | 11.7% | -11.0% |
| update p95 (ms) | 8.93 | 23.9% | 7.65 | 16.6% | -14.3% |
| update p99 (ms) | 318.2 | 25.6% | 281.6 | 2.9% | -11.5% |

#### serverless, 500 threads

| metric | YCSB (mean of r1, r2) | YCSB run range | whycsb (mean of r1, r2) | whycsb run range | whycsb vs YCSB |
|---|---|---|---|---|---|
| steady-state op/s | 10,933 | 1.7% | 11,102 | 0.0% | +1.5% |
| overall op/s | 11,094 | 0.9% | 11,379 | 0.9% | +2.6% |
| steady-state avg latency (all ops, ms) | 45.608 | 2.3% | 44.969 | 0.7% | -1.4% |
| read avg (ms) | 1.280 | 22.6% | 1.105 | 0.9% | -13.6% |
| read p50 (ms) | 0.726 | 4.0% | 0.790 | 0.0% | +8.9% |
| read p95 (ms) | 2.578 | 11.6% | 2.670 | 0.7% | +3.6% |
| read p99 (ms) | 5.095 | 13.7% | 5.025 | 1.8% | -1.4% |
| update avg (ms) | 88.00 | 0.3% | 84.19 | 0.9% | -4.3% |
| update p50 (ms) | 3.115 | 1.5% | 3.190 | 3.1% | +2.4% |
| update p95 (ms) | 12.28 | 10.1% | 10.53 | 6.1% | -14.3% |
| update p99 (ms) | 2,956.3 | 0.9% | 2,875.4 | 1.1% | -2.7% |

#### instance, 10 threads

| metric | YCSB (mean of r1, r2) | YCSB run range | whycsb (mean of r1, r2) | whycsb run range | whycsb vs YCSB |
|---|---|---|---|---|---|
| steady-state op/s | 4,472 | 4.4% | 4,486 | 7.3% | +0.3% |
| overall op/s | 4,371 | 4.8% | 4,429 | 6.6% | +1.3% |
| steady-state avg latency (all ops, ms) | 2.237 | 4.4% | 2.231 | 7.2% | -0.3% |
| read avg (ms) | 0.789 | 8.3% | 0.820 | 4.9% | +3.9% |
| read p50 (ms) | 0.669 | 8.5% | 0.695 | 4.3% | +3.8% |
| read p95 (ms) | 1.437 | 12.7% | 1.470 | 8.2% | +2.3% |
| read p99 (ms) | 2.264 | 16.7% | 2.335 | 7.3% | +3.1% |
| update avg (ms) | 3.78 | 7.7% | 3.69 | 9.2% | -2.3% |
| update p50 (ms) | 3.398 | 10.3% | 3.300 | 9.7% | -2.9% |
| update p95 (ms) | 6.42 | 3.3% | 6.33 | 8.7% | -1.3% |
| update p99 (ms) | 10.0 | 6.1% | 10.1 | 7.5% | +0.4% |

#### instance, 100 threads

| metric | YCSB (mean of r1, r2) | YCSB run range | whycsb (mean of r1, r2) | whycsb run range | whycsb vs YCSB |
|---|---|---|---|---|---|
| steady-state op/s | 6,109 | 1.2% | 6,105 | 9.7% | -0.1% |
| overall op/s | 6,058 | 0.6% | 6,043 | 9.4% | -0.3% |
| steady-state avg latency (all ops, ms) | 16.359 | 1.1% | 16.422 | 9.6% | +0.4% |
| read avg (ms) | 1.815 | 1.1% | 1.870 | 6.4% | +3.0% |
| read p50 (ms) | 1.337 | 1.5% | 1.385 | 7.9% | +3.6% |
| read p95 (ms) | 4.567 | 0.2% | 4.675 | 6.2% | +2.4% |
| read p99 (ms) | 7.059 | 1.1% | 7.375 | 5.3% | +4.5% |
| update avg (ms) | 31.10 | 0.5% | 31.16 | 9.6% | +0.2% |
| update p50 (ms) | 9.507 | 1.6% | 9.505 | 12.1% | -0.0% |
| update p95 (ms) | 22.41 | 1.4% | 22.46 | 9.1% | +0.2% |
| update p99 (ms) | 764.7 | 1.4% | 770.0 | 10.1% | +0.7% |

#### instance, 500 threads

| metric | YCSB (mean of r1, r2) | YCSB run range | whycsb (mean of r1, r2) | whycsb run range | whycsb vs YCSB |
|---|---|---|---|---|---|
| steady-state op/s | 3,484 | 0.3% | 3,324 | 1.0% | -4.6% |
| overall op/s | 3,620 | 3.6% | 3,501 | 2.0% | -3.3% |
| steady-state avg latency (all ops, ms) | 140.947 | 0.9% | 144.800 | 0.6% | +2.7% |
| read avg (ms) | 4.322 | 42.3% | 3.125 | 6.1% | -27.7% |
| read p50 (ms) | 1.929 | 5.1% | 1.990 | 8.0% | +3.2% |
| read p95 (ms) | 8.787 | 1.0% | 8.800 | 5.9% | +0.1% |
| read p99 (ms) | 15.719 | 6.2% | 14.945 | 6.0% | -4.9% |
| update avg (ms) | 269.12 | 2.6% | 274.62 | 2.1% | +2.0% |
| update p50 (ms) | 13.799 | 1.6% | 14.305 | 3.6% | +3.7% |
| update p95 (ms) | 81.66 | 61.0% | 42.36 | 13.3% | -48.1% |
| update p99 (ms) | 8,880.1 | 0.7% | 9,207.8 | 2.8% | +3.7% |

## Reproducing

Scripts lived in `/tmp/cmp` (`env.sh`, `run_one.sh`, `run_matrix.sh`, `opcounters.py`, `analyze.py`, `zipf_compare.py`, `verify_data.py`). Key commands, with credentials redacted:

```bash
# YCSB load (40 threads x 25,000 records, divisible by batchsize 100)
java -Dlogback.configurationFile=logback.xml -cp "ycsb-mongodb-binding-0.17.0/lib/*" site.ycsb.Client -load \
  -db site.ycsb.db.MongoDbClient -P workloads/workloada -p recordcount=1000000 -p batchsize=100 -threads 40 \
  -p "mongodb.url=mongodb://<USER>:<PASSWORD>@<cluster-endpoint>:27017/ycsb_cmp?retryWrites=false&maxPoolSize=40"

# whycsb load
python3 whycsb.py --uri "mongodb://<USER>:<PASSWORD>@<cluster-endpoint>:27017/?retryWrites=false" --database whycsb_cmp \
  --load --insert-order hashed --threads 40 --record-count 1000000 --batch-size 100 --drop-collection

# YCSB run (N = 10, 100, 500)
java -Dlogback.configurationFile=logback.xml -cp "ycsb-mongodb-binding-0.17.0/lib/*" site.ycsb.Client -t \
  -db site.ycsb.db.MongoDbClient -P workloads/workloada -p recordcount=1000000 -p operationcount=1000000000 \
  -p maxexecutiontime=120 -p measurementtype=hdrhistogram -p hdrhistogram.percentiles=50,90,95,99 \
  -p status.interval=10 -s -threads N \
  -p "mongodb.url=mongodb://<USER>:<PASSWORD>@<cluster-endpoint>:27017/ycsb_cmp?retryWrites=false&maxPoolSize=N"

# whycsb run (N = 10, 100, 500)
python3 whycsb.py --uri "mongodb://<USER>:<PASSWORD>@<cluster-endpoint>:27017/?retryWrites=false" --database whycsb_cmp \
  --run --workload A --insert-order hashed --record-count 1000000 --threads N --run-seconds 120 --output-file run.csv
```

`logback.xml` sets the root logger to `WARN`. Server-side verification read `serverStatus.opcounters` from the primary instance endpoint with `directConnection=true` before and after each run.
