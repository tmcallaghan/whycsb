#!/usr/bin/env python3
"""
whycsb - Python implementation of YCSB for MongoDB-compatible databases
"""

import argparse
import csv
import multiprocessing as mp
import queue
import random
import string
import sys
import threading
import time
import warnings
import datetime as dt
from fractions import Fraction

import pymongo


# YCSB workload definitions
WORKLOADS = {
    'A': {'read': 0.50, 'update': 0.50, 'distribution': 'zipfian'},
    'B': {'read': 0.95, 'update': 0.05, 'distribution': 'zipfian'},
    'C': {'read': 1.00, 'distribution': 'zipfian'},
    'D': {'read': 0.95, 'insert': 0.05, 'distribution': 'latest'},
    'E': {'scan': 0.95, 'insert': 0.05, 'distribution': 'zipfian'},
    'F': {'read': 0.50, 'readmodifywrite': 0.50, 'distribution': 'zipfian'}
}


# Distribution generators

class UniformGenerator:
    """Uniform distribution generator"""
    def __init__(self, min_key, max_key, seed=None):
        self.min_key = min_key
        self.max_key = max_key
        self.rng = random.Random(seed)

    def next(self):
        return self.rng.randint(self.min_key, self.max_key)


class ZipfianGenerator:
    """Scrambled Zipfian distribution generator (YCSB-compatible)"""
    ZIPFIAN_CONSTANT = 0.99
    ITEM_COUNT = 10000000000
    ZETAN = 26.46902820178302

    def __init__(self, min_key, max_key, seed=None, scrambled=True):
        self.min_key = min_key
        self.max_key = max_key
        self.num_items = max_key - min_key + 1
        self.rng = random.Random(seed)
        self.scrambled = scrambled

        # Generate over 10B-item space when scrambled, actual keyspace otherwise
        self.gen_items = self.ITEM_COUNT if self.scrambled else self.num_items

        # Calculate zeta values
        self.theta = self.ZIPFIAN_CONSTANT
        self.alpha = 1.0 / (1.0 - self.theta)
        self.zeta2theta = self._zeta(2, self.theta)
        self.zetan = self.ZETAN
        self.eta = (1.0 - pow(2.0 / self.gen_items, 1.0 - self.theta)) / (1.0 - self.zeta2theta / self.zetan)

    def _zeta(self, n, theta):
        """Calculate zeta value"""
        sum_val = 0.0
        for i in range(1, n + 1):
            sum_val += 1.0 / pow(i, theta)
        return sum_val

    def _fnv_hash(self, val):
        """FNV-1a 64-bit hash"""
        FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325
        FNV_PRIME_64 = 0x100000001b3

        hash_val = FNV_OFFSET_BASIS_64
        val_bytes = str(val).encode('utf-8')
        for byte in val_bytes:
            hash_val ^= byte
            hash_val = (hash_val * FNV_PRIME_64) & 0xffffffffffffffff
        return hash_val

    def next(self):
        """Generate next zipfian value"""
        u = self.rng.random()
        uz = u * self.zetan

        if uz < 1.0:
            zipfian_value = 0
        elif uz < 1.0 + pow(0.5, self.theta):
            zipfian_value = 1
        else:
            zipfian_value = int(self.gen_items * pow(self.eta * u - self.eta + 1, self.alpha))

        # Optionally scramble using FNV hash
        if self.scrambled:
            hashed = self._fnv_hash(zipfian_value) % self.num_items
            return self.min_key + hashed
        else:
            # Clamp to valid range
            if zipfian_value >= self.num_items:
                zipfian_value = self.num_items - 1
            return self.min_key + zipfian_value


class SkewedLatestGenerator:
    """Latest distribution generator (for workload D)"""
    def __init__(self, min_key, max_key, seed=None):
        self.min_key = min_key
        self.max_key = max_key
        # Use non-scrambled zipfian for latest distribution
        self.zipfian_gen = ZipfianGenerator(min_key, max_key, seed, scrambled=False)

    def next(self):
        """Generate key biased toward latest (highest) keys"""
        zipfian_val = self.zipfian_gen.next()
        return self.max_key - (zipfian_val - self.min_key)

    def acknowledge_insert(self, key):
        """Update max_key when new records are inserted"""
        if key > self.max_key:
            self.max_key = key


# Utility functions

def fnv_hash_64(val):
    """FNV-1a 64-bit hash for key hashing"""
    FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325
    FNV_PRIME_64 = 0x100000001b3

    hash_val = FNV_OFFSET_BASIS_64
    val_bytes = str(val).encode('utf-8')
    for byte in val_bytes:
        hash_val ^= byte
        hash_val = (hash_val * FNV_PRIME_64) & 0xffffffffffffffff
    return hash_val


def format_key(key_num):
    """Format key as YCSB-style user key"""
    return f'user{str(key_num).zfill(16)}'


def random_string(length, rng=None):
    """Generate random alphanumeric string of specified length"""
    if rng is None:
        rng = random
    chars = string.ascii_letters + string.digits
    return ''.join(rng.choice(chars) for _ in range(length))


def key_name(key_num, insert_order='ordered'):
    """YCSB key for a key number: sequential, or FNV-hashed so inserts spread across the key space"""
    if insert_order == 'hashed':
        return format_key(fnv_hash_64(key_num))
    return format_key(key_num)


def generate_document(key, field_count=10, field_length=100, rng=None, insert_order='ordered'):
    """Generate YCSB-style document"""
    doc = {'_id': key_name(key, insert_order)}
    for i in range(field_count):
        doc[f'field{i}'] = random_string(field_length, rng)
    return doc


def get_timestamp(epoch_seconds=None):
    """Get ISO 8601 UTC timestamp with milliseconds, e.g. 2026-05-01T13:22:45.123Z (default: now)"""
    if epoch_seconds is None:
        epoch_seconds = time.time()
    return dt.datetime.fromtimestamp(epoch_seconds, dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def format_elapsed(seconds):
    """Format elapsed time as HH:MM:SS.ms"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f'{hours:02d}:{minutes:02d}:{secs:06.3f}'


def get_distribution_generator(distribution_type, min_key, max_key, seed=None):
    """Factory function for distribution generators"""
    if distribution_type == 'uniform':
        return UniformGenerator(min_key, max_key, seed)
    elif distribution_type == 'zipfian':
        return ZipfianGenerator(min_key, max_key, seed)
    elif distribution_type == 'latest':
        return SkewedLatestGenerator(min_key, max_key, seed)
    else:
        raise ValueError(f'Unknown distribution type: {distribution_type}')


def choose_operation(workload_spec, rng):
    """Choose operation based on workload proportions"""
    r = rng.random()
    cumulative = 0.0
    for op_type, proportion in workload_spec.items():
        if op_type == 'distribution':
            continue
        cumulative += proportion
        if r < cumulative:
            return op_type
    # Fallback to first operation
    return [k for k in workload_spec.keys() if k != 'distribution'][0]


# Latency tracking
#
# Workers record every operation latency (integer microseconds) into a compact
# log-linear histogram and ship it to the reporter once per second. Message size
# and reporter work are bounded by the number of distinct buckets, not by the
# number of operations, so a single reporter can keep up with 1000+ workers and
# percentiles can be computed for the entire run without keeping every sample.
#
# Values below 2**HIST_SUB_BITS us are recorded exactly; larger values keep their
# top HIST_SUB_BITS significant bits (bucket width <= 1/128 of its value, so the
# bucket midpoint is within 0.4% of the true latency).

HIST_SUB_BITS = 8
PERF_REPORT_INTERVAL_SECONDS = 1


def latency_bucket(latency_us):
    """Map a latency in microseconds to its histogram bucket (bucket lower bound)"""
    shift = latency_us.bit_length() - HIST_SUB_BITS
    if shift <= 0:
        return latency_us
    return (latency_us >> shift) << shift


def bucket_midpoint_us(bucket):
    """Representative value (midpoint) of a histogram bucket in microseconds"""
    shift = bucket.bit_length() - HIST_SUB_BITS
    if shift <= 0:
        return float(bucket)
    return bucket + ((1 << shift) - 1) / 2.0


def new_op_stats():
    """Empty stats record for one operation type"""
    # count   : operations (documents for load batches)
    # samples : latency measurements (one per operation, or per batch for load)
    return {'count': 0, 'samples': 0, 'total_us': 0, 'min_us': None, 'max_us': 0, 'hist': {}}


def record_latency(perf_stats, op_type, latency_us, op_count=1):
    """Record a single latency measurement in a worker's local stats"""
    stats = perf_stats.get(op_type)
    if stats is None:
        stats = perf_stats[op_type] = new_op_stats()
    stats['count'] += op_count
    stats['samples'] += 1
    stats['total_us'] += latency_us
    if stats['min_us'] is None or latency_us < stats['min_us']:
        stats['min_us'] = latency_us
    if latency_us > stats['max_us']:
        stats['max_us'] = latency_us
    hist = stats['hist']
    bucket = latency_bucket(latency_us)
    hist[bucket] = hist.get(bucket, 0) + 1


def merge_op_stats(target, source):
    """Merge one stats record into another (in place)"""
    target['count'] += source['count']
    target['samples'] += source['samples']
    target['total_us'] += source['total_us']
    if source['min_us'] is not None and (target['min_us'] is None or source['min_us'] < target['min_us']):
        target['min_us'] = source['min_us']
    if source['max_us'] > target['max_us']:
        target['max_us'] = source['max_us']
    target_hist = target['hist']
    for bucket, bucket_count in source['hist'].items():
        target_hist[bucket] = target_hist.get(bucket, 0) + bucket_count


def merge_stats_dict(target, source):
    """Merge a dict of {op_type: stats} into another (in place)"""
    for op_type, stats in source.items():
        if op_type not in target:
            target[op_type] = new_op_stats()
        merge_op_stats(target[op_type], stats)


def combine_op_stats(stats_dict):
    """Combine all operation types into a single stats record"""
    combined = new_op_stats()
    for stats in stats_dict.values():
        merge_op_stats(combined, stats)
    return combined


# All workers share a time grid of PERF_REPORT_INTERVAL_SECONDS slots starting at
# app_config['start_time']. Slot N covers (start + (N-1) * interval, start + N * interval].
# Every stats message is tagged with the slot its operations completed in, so the
# reporter attributes work to the interval it actually happened in, regardless of
# when the message arrives.

def time_slot(start_time, now):
    """Slot number containing a point in time"""
    return max(int((now - start_time) // PERF_REPORT_INTERVAL_SECONDS), 0) + 1


def new_perf_tracker(perf_queue, start_time):
    """Per-worker state for accumulating and shipping latency stats"""
    return {'queue': perf_queue, 'start': start_time, 'slot': time_slot(start_time, time.time()), 'stats': {}}


def flush_perf(tracker):
    """Ship accumulated stats (all completed within the tracker's current slot) to the reporter"""
    if tracker['stats']:
        tracker['queue'].put({'name': 'opStats', 'slot': tracker['slot'], 'stats': tracker['stats']})
        tracker['stats'] = {}


def track_op(tracker, op_type, latency_us, op_count=1):
    """Record a completed operation, first shipping the previous slot's stats if a slot boundary passed"""
    now = time.time()
    if now >= tracker['start'] + tracker['slot'] * PERF_REPORT_INTERVAL_SECONDS:
        flush_perf(tracker)
        tracker['slot'] = time_slot(tracker['start'], now)
    record_latency(tracker['stats'], op_type, latency_us, op_count)


def finish_worker(tracker, thread_num):
    """Ship any remaining stats and tell the reporter this worker is done"""
    flush_perf(tracker)
    tracker['queue'].put({'name': 'processCompleted', 'processNum': thread_num, 'time': time.time()})


def parse_percentiles(value):
    """Parse comma separated percentiles (e.g. '50,90,99.9') into sorted unique strings"""
    result = {}
    for item in value.split(','):
        item = item.strip()
        if not item:
            continue
        try:
            pct = Fraction(item)
        except (ValueError, ZeroDivisionError):
            raise argparse.ArgumentTypeError(f'invalid percentile "{item}"')
        if pct <= 0 or pct > 100:
            raise argparse.ArgumentTypeError(f'percentile "{item}" must be > 0 and <= 100')
        result.setdefault(pct, item)
    if not result:
        raise argparse.ArgumentTypeError('at least one percentile is required')
    return [result[pct] for pct in sorted(result)]


def percentile_label(pct):
    """Column/display label for a percentile string, e.g. '99.9' -> 'p99.9'"""
    return f'p{pct}'


def histogram_percentiles(stats, percentiles):
    """Nearest-rank percentiles (in ms) from a stats record, one value per percentile"""
    total = stats['samples']
    if total == 0:
        return [0.0 for _ in percentiles]

    # nearest-rank: rank = ceil(pct/100 * N), exact rational math avoids float rounding of the rank
    ranks = []
    for pct in percentiles:
        frac = Fraction(pct) * total / 100
        ranks.append(max(-(-frac.numerator // frac.denominator), 1))

    min_us = stats['min_us']
    max_us = stats['max_us']
    results = [0.0] * len(ranks)
    order = sorted(range(len(ranks)), key=lambda i: ranks[i])
    next_idx = 0
    cumulative = 0
    for bucket in sorted(stats['hist']):
        cumulative += stats['hist'][bucket]
        while next_idx < len(order) and ranks[order[next_idx]] <= cumulative:
            if ranks[order[next_idx]] == total:
                value_us = max_us
            else:
                value_us = min(max(bucket_midpoint_us(bucket), min_us), max_us)
            results[order[next_idx]] = value_us / 1000.0
            next_idx += 1
        if next_idx == len(order):
            break
    return results


def avg_latency_ms(stats):
    """Average latency in ms of a stats record"""
    return stats['total_us'] / stats['samples'] / 1000.0 if stats['samples'] > 0 else 0.0


def reportCollectionInfo(app_config):
    """Output collection specifics"""
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    client = pymongo.MongoClient(app_config['uri'])
    db = client[app_config['database']]

    collStats = db.command("collStats", app_config['collection'])

    compressionRatio = collStats['size'] / collStats['storageSize']
    gbDivisor = 1024*1024*1024

    print(f'\nCollection information')
    print(f'  numDocs             = {collStats["count"]:12,d}')
    print(f'  avgObjSize          = {int(collStats["avgObjSize"]):12,d}')
    print(f'  size (GB)           = {collStats["size"]/gbDivisor:12,.4f}')
    print(f'  storageSize (GB)    = {collStats["storageSize"]/gbDivisor:12,.4f}')
    print(f'  compressionRatio    = {compressionRatio:12,.4f}')
    print(f'  totalIndexSize (GB) = {collStats["totalIndexSize"]/gbDivisor:12,.4f}')

    client.close()


# Worker functions

def load_worker(thread_num, perf_queue, app_config):
    """Worker process for loading data"""

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    # Create per-worker MongoClient
    client = pymongo.MongoClient(app_config['uri'])
    db = client[app_config['database']]
    col = db[app_config['collection']]

    # Per-worker random state
    rng = random.Random(app_config['seed'] + thread_num)

    # Partition key space
    record_count = app_config['record_count']
    num_threads = app_config['threads']
    records_per_worker = record_count // num_threads
    start_key = thread_num * records_per_worker
    end_key = start_key + records_per_worker
    if thread_num == num_threads - 1:
        end_key = record_count

    # Load configuration
    batch_size = app_config['batch_size']
    field_count = app_config['field_count']
    field_length = app_config['field_length']
    insert_order = app_config['insert_order']

    tracker = new_perf_tracker(perf_queue, app_config['start_time'])

    def insert_batch(batch):
        op_start = time.perf_counter_ns()
        col.insert_many(batch, ordered=False)
        track_op(tracker, 'insert', (time.perf_counter_ns() - op_start) // 1000, len(batch))

    try:
        # Load data in batches
        batch = []
        for key in range(start_key, end_key):
            batch.append(generate_document(key, field_count, field_length, rng, insert_order))

            if len(batch) >= batch_size:
                insert_batch(batch)
                batch = []

        # Insert remaining batch
        if batch:
            insert_batch(batch)

        client.close()
    finally:
        # always flush stats and signal completion so the reporter never waits forever
        finish_worker(tracker, thread_num)


def allocate_insert_key(insert_counter):
    """Take the next insert key number from the counter shared by all workers"""
    with insert_counter.get_lock():
        key_num = insert_counter.value
        insert_counter.value += 1
    return key_num


def acknowledge_insert_key(latest_key, key_num):
    """Publish a completed insert so 'latest' readers in every worker can target it"""
    with latest_key.get_lock():
        if key_num > latest_key.value:
            latest_key.value = key_num


def insert_new_document(col, insert_counter, field_count, field_length, rng, insert_order):
    """Insert one new document, skipping keys that already exist (e.g. left by an interrupted run)"""
    while True:
        key_num = allocate_insert_key(insert_counter)
        doc = generate_document(key_num, field_count, field_length, rng, insert_order)
        try:
            col.insert_one(doc)
            return key_num
        except pymongo.errors.DuplicateKeyError:
            continue


def run_worker(thread_num, perf_queue, app_config, insert_counter, latest_key):
    """Worker process for running workload

    insert_counter and latest_key are multiprocessing.Value objects shared by all
    workers: the next key number to insert and the newest key known to be inserted.
    """

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    # Create per-worker MongoClient
    client = pymongo.MongoClient(app_config['uri'])
    db = client[app_config['database']]
    col = db[app_config['collection']]

    # Per-worker random state
    rng = random.Random(app_config['seed'] + thread_num)

    # Load configuration
    workload = WORKLOADS[app_config['workload']]
    record_count = app_config['record_count']
    field_count = app_config['field_count']
    field_length = app_config['field_length']
    max_scan_length = app_config['max_scan_length']
    insert_order = app_config['insert_order']

    # Local stats, shipped to the reporter every PERF_REPORT_INTERVAL_SECONDS
    tracker = new_perf_tracker(perf_queue, app_config['start_time'])

    # Distribution generator ('latest' covers every existing key, newest first)
    distribution = workload['distribution']
    max_key = latest_key.value if distribution == 'latest' else record_count - 1
    key_gen = get_distribution_generator(distribution, 0, max_key, app_config['seed'] + thread_num)
    track_latest = isinstance(key_gen, SkewedLatestGenerator)

    # Rate limiting
    rate_limit = app_config['rate_limit']
    rate_limit_per_thread = rate_limit // app_config['threads'] if rate_limit > 0 else 0
    interval_seconds = 2
    next_report_time = time.time() + interval_seconds
    ops_this_interval = 0

    # Operation count and duration limits
    operation_count = app_config.get('operation_count', 0) // app_config['threads']
    run_seconds = app_config.get('run_seconds', 0)
    ops_completed = 0
    start_time = time.time()

    try:
        # Main operation loop
        while True:
            # Check termination conditions
            if operation_count > 0 and ops_completed >= operation_count:
                break
            if run_seconds > 0 and (time.time() - start_time) >= run_seconds:
                break

            # Rate limiting
            if rate_limit_per_thread > 0:
                current_time = time.time()
                if current_time > next_report_time:
                    next_report_time = current_time + interval_seconds
                    ops_this_interval = 0
                elif ops_this_interval >= rate_limit_per_thread * interval_seconds:
                    sleep_time = next_report_time - current_time
                    if sleep_time > 0:
                        # ship stats now so they don't arrive late while sleeping
                        flush_perf(tracker)
                        time.sleep(sleep_time)
                    next_report_time = time.time() + interval_seconds
                    ops_this_interval = 0

            # Choose operation
            op_type = choose_operation(workload, rng)

            # 'latest' follows inserts made by all workers
            if track_latest:
                key_gen.acknowledge_insert(latest_key.value)

            # Execute operation
            op_start = time.perf_counter_ns()

            if op_type == 'read':
                key = key_name(key_gen.next(), insert_order)
                col.find_one({'_id': key})

            elif op_type == 'update':
                key = key_name(key_gen.next(), insert_order)
                field_to_update = f'field{rng.randint(0, field_count - 1)}'
                new_value = random_string(field_length, rng)
                col.update_one({'_id': key}, {'$set': {field_to_update: new_value}})

            elif op_type == 'scan':
                start_key = key_name(key_gen.next(), insert_order)
                scan_length = rng.randint(1, max_scan_length)
                cursor = col.find({'_id': {'$gte': start_key}}).limit(scan_length)
                list(cursor)

            elif op_type == 'readmodifywrite':
                key = key_name(key_gen.next(), insert_order)
                doc = col.find_one({'_id': key})
                if doc:
                    for i in range(field_count):
                        doc[f'field{i}'] = random_string(field_length, rng)
                    col.replace_one({'_id': key}, doc)

            elif op_type == 'insert':
                new_key = insert_new_document(col, insert_counter, field_count, field_length, rng, insert_order)
                acknowledge_insert_key(latest_key, new_key)

            track_op(tracker, op_type, (time.perf_counter_ns() - op_start) // 1000)

            ops_completed += 1
            ops_this_interval += 1

        client.close()
    finally:
        # always flush stats (including the final partial interval) and signal completion
        finish_worker(tracker, thread_num)


# Reporter function

REPORT_INTERVAL_SECONDS = 10
SLOTS_PER_REPORT = REPORT_INTERVAL_SECONDS // PERF_REPORT_INTERVAL_SECONDS
# how long after an interval ends to wait for its stats before reporting it
REPORT_GRACE_SECONDS = 1.0
WINDOW_SIZE = 10


def interval_of_slot(slot):
    """Report interval (1-based) containing a perf slot"""
    return -(-slot // SLOTS_PER_REPORT)


def handle_perf_message(msg, state):
    """Apply one queue message to the reporter state"""
    if msg['name'] == 'opStats':
        # stats arriving after their interval was reported are folded into the next one
        idx = max(interval_of_slot(msg['slot']), state['next_interval'])
        merge_stats_dict(state['pending'].setdefault(idx, {}), msg['stats'])
    elif msg['name'] == 'processCompleted':
        state['num_completed'] += 1
        state['end_time'] = max(state['end_time'], msg['time'])


def open_csv(app_config, pct_labels):
    """Open CSV output (if requested) and write the header"""
    if not app_config.get('output_file'):
        return None, None
    csv_file = open(app_config['output_file'], 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([
        'timestamp', 'elapsed_time', 'elapsed_seconds', 'total_operations',
        'overall_tps', 'interval_tps', 'interval_latency_ms',
        'window_tps', 'window_latency_ms'
    ] + [f'interval_{label}_latency_ms' for label in pct_labels])
    return csv_file, csv_writer


def print_final_stats(total_stats, total_ops, elapsed, percentiles, pct_labels):
    """Print end of run summary, including per-operation latency percentiles"""
    print(f'\n=== Final Statistics ===')
    print(f'Total operations: {total_ops:,}')
    print(f'Total time: {format_elapsed(elapsed)}')
    print(f'Overall throughput: {total_ops / elapsed if elapsed > 0 else 0:,.2f} op/s')

    print(f'\nOperation breakdown:')
    for op_type, stats in sorted(total_stats.items()):
        if stats['count'] > 0:
            pct = (stats['count'] / total_ops) * 100
            print(f'  {op_type:20s}: {stats["count"]:10,} ops ({pct:5.1f}%) @ {avg_latency_ms(stats):.2f} ms avg')

    rows = [(op_type, stats) for op_type, stats in sorted(total_stats.items()) if stats['samples'] > 0]
    if len(rows) > 1:
        rows.append(('all', combine_op_stats(total_stats)))
    if not rows:
        return

    print(f'\nLatency (ms):')
    header = f'  {"operation":20s} {"samples":>12s} {"avg":>10s} {"min":>10s}'
    header += ''.join(f' {label:>10s}' for label in pct_labels) + f' {"max":>10s}'
    print(header)
    for op_type, stats in rows:
        values = histogram_percentiles(stats, percentiles)
        line = f'  {op_type:20s} {stats["samples"]:12,} {avg_latency_ms(stats):10.2f} {stats["min_us"] / 1000:10.2f}'
        line += ''.join(f' {v:10.2f}' for v in values) + f' {stats["max_us"] / 1000:10.2f}'
        print(line)


def report_interval(state, idx, end_elapsed):
    """Print (and write to CSV) one interval, covering elapsed time ((idx-1) * interval, end_elapsed]"""
    interval_stats = state['pending'].pop(idx, {})
    merge_stats_dict(state['total_stats'], interval_stats)
    interval = combine_op_stats(interval_stats)
    state['total_ops'] += interval['count']
    total_ops = state['total_ops']

    # Calculate metrics
    duration = max(end_elapsed - (idx - 1) * REPORT_INTERVAL_SECONDS, 0.001)
    elapsed = max(end_elapsed, 0.001)
    overall_tps = total_ops / elapsed
    interval_tps = interval['count'] / duration
    interval_avg_latency = avg_latency_ms(interval)
    interval_pcts = histogram_percentiles(interval, state['percentiles'])

    # Sliding window
    recent_tps = state['recent_tps']
    recent_latency = state['recent_latency']
    recent_tps.append(interval_tps)
    recent_latency.append(interval_avg_latency)
    if len(recent_tps) > WINDOW_SIZE:
        recent_tps.pop(0)
        recent_latency.pop(0)
    window_tps = sum(recent_tps) / len(recent_tps)
    window_latency = sum(recent_latency) / len(recent_latency)

    # Print to stdout
    timestamp = get_timestamp(state['start_time'] + elapsed)
    pct_text = ' '.join(f'{label} {value:.2f}' for label, value in zip(state['pct_labels'], interval_pcts))
    print(f'[{timestamp}] elapsed {format_elapsed(elapsed)} | '
          f'total ops {total_ops:,} at {overall_tps:,.2f} op/s | '
          f'interval {interval_tps:,.2f} op/s @ {interval_avg_latency:.2f} ms | '
          f'{pct_text} ms | '
          f'last {WINDOW_SIZE} {window_tps:,.2f} op/s @ {window_latency:.2f} ms', flush=True)

    # Write to CSV
    if state['csv_writer']:
        state['csv_writer'].writerow([
            timestamp,
            format_elapsed(elapsed),
            f'{elapsed:.2f}',
            total_ops,
            f'{overall_tps:.2f}',
            f'{interval_tps:.2f}',
            f'{interval_avg_latency:.2f}',
            f'{window_tps:.2f}',
            f'{window_latency:.2f}'
        ] + [f'{value:.2f}' for value in interval_pcts])
        state['csv_file'].flush()


def reporter(perf_queue, app_config):
    """Reporter thread for collecting and displaying performance metrics

    Blocks on the queue until the next report is due rather than sleeping, so
    messages are consumed continuously as workers send them. Stats are grouped
    by the time slot the operations completed in (not by arrival time), and each
    interval is reported REPORT_GRACE_SECONDS after it ends.
    """
    percentiles = app_config['percentiles']
    pct_labels = [percentile_label(p) for p in percentiles]
    num_threads = app_config['threads']
    start_time = app_config['start_time']
    csv_file, csv_writer = open_csv(app_config, pct_labels)

    state = {
        'start_time': start_time, 'percentiles': percentiles, 'pct_labels': pct_labels,
        'csv_file': csv_file, 'csv_writer': csv_writer,
        'pending': {}, 'next_interval': 1, 'num_completed': 0, 'end_time': start_time,
        'total_stats': {}, 'total_ops': 0, 'recent_tps': [], 'recent_latency': [],
    }

    while state['num_completed'] < num_threads:
        idx = state['next_interval']
        deadline = start_time + idx * REPORT_INTERVAL_SECONDS + REPORT_GRACE_SECONDS

        # consume messages until the report is due or all workers have finished
        while state['num_completed'] < num_threads:
            wait_seconds = deadline - time.time()
            if wait_seconds <= 0:
                break
            try:
                msg = perf_queue.get(timeout=wait_seconds)
            except queue.Empty:
                break
            handle_perf_message(msg, state)

        if state['num_completed'] < num_threads:
            report_interval(state, idx, idx * REPORT_INTERVAL_SECONDS)
            state['next_interval'] = idx + 1

    # all workers done: report the remaining intervals, the last one ends when the last worker finished
    end_elapsed = state['end_time'] - start_time
    last_idx = max([state['next_interval'], interval_of_slot(time_slot(start_time, state['end_time']))] + list(state['pending']))
    if end_elapsed <= (last_idx - 1) * REPORT_INTERVAL_SECONDS:
        # last completion arrived after its interval was already reported, end the run now instead
        end_elapsed = time.time() - start_time
    for idx in range(state['next_interval'], last_idx + 1):
        report_interval(state, idx, end_elapsed if idx == last_idx else idx * REPORT_INTERVAL_SECONDS)

    print_final_stats(state['total_stats'], state['total_ops'], end_elapsed, percentiles, pct_labels)

    if csv_file:
        csv_file.close()


# Setup functions

def setup_load(app_config):
    """Setup for load mode"""

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    client = pymongo.MongoClient(app_config['uri'])
    db = client[app_config['database']]
    col = db[app_config['collection']]

    # Drop collection if it exists
    if app_config['drop_collection']:
        print(f"Dropping collection {app_config['database']}.{app_config['collection']} if it exists...")
        col.drop()

    print(f"Loading {app_config['record_count']:,} records into "
          f"{app_config['database']}.{app_config['collection']} "
          f"using {app_config['threads']} threads...")

    client.close()


def find_next_key_num(col, insert_order):
    """First unused key number in the collection (key numbers are allocated densely from 0)"""
    if insert_order == 'ordered':
        # zero padded keys sort numerically, so the largest _id is the largest key number
        doc = col.find_one({'_id': {'$gte': 'user', '$lt': 'user:'}}, projection={'_id': 1}, sort=[('_id', -1)])
        return int(doc['_id'][4:]) + 1 if doc else 0

    # hashed keys have no useful order: exponential + binary search for the end of the dense range
    def exists(key_num):
        return col.find_one({'_id': key_name(key_num, insert_order)}, projection={'_id': 1}) is not None

    if not exists(0):
        return 0
    low, high = 0, 1
    while exists(high):
        low, high = high, high * 2
    while high - low > 1:
        mid = (low + high) // 2
        if exists(mid):
            low = mid
        else:
            high = mid
    return high


def setup_run(app_config):
    """Setup for run mode, returns the first key number to use for new inserts"""

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    print(f"Running workload {app_config['workload']} against "
          f"{app_config['database']}.{app_config['collection']} "
          f"using {app_config['threads']} threads...")

    client = pymongo.MongoClient(app_config['uri'])
    col = client[app_config['database']][app_config['collection']]
    insert_order = app_config['insert_order']

    # reads would silently miss every key if --insert-order differs from the load
    other_order = 'hashed' if insert_order == 'ordered' else 'ordered'
    if (col.find_one({'_id': key_name(0, insert_order)}, projection={'_id': 1}) is None and
            col.find_one({'_id': key_name(0, other_order)}, projection={'_id': 1}) is not None):
        client.close()
        sys.exit(f"Collection appears to be loaded with --insert-order {other_order}, rerun with --insert-order {other_order}")

    insert_start = app_config['record_count']
    workload = WORKLOADS[app_config['workload']]
    if 'insert' in workload or workload['distribution'] == 'latest':
        # continue after keys inserted by earlier runs so new inserts never collide
        insert_start = max(insert_start, find_next_key_num(col, insert_order))
        print(f"New inserts start at key number {insert_start:,}")

    client.close()
    return insert_start


# Main function

def main():
    parser = argparse.ArgumentParser(
        description='whycsb - Python YCSB benchmark for MongoDB-compatible databases'
    )

    # Connection parameters
    parser.add_argument('--uri', required=True, type=str, help='MongoDB connection string')
    parser.add_argument('--database', required=True, type=str, help='Database name')
    parser.add_argument('--collection', type=str, default='usertable', help='Collection name (default: usertable)')

    # Mode selection
    parser.add_argument('--load', action='store_true', help='Load data mode')
    parser.add_argument('--run', action='store_true', help='Run workload mode')

    # Common parameters
    parser.add_argument('--threads', type=int, default=1, help='Number of worker threads (default: 1)')
    parser.add_argument('--record-count', type=int, default=1000, help='Number of records to load or size of dataset (default: 1000)')
    parser.add_argument('--field-count', type=int, default=10, help='Number of fields per document (default: 10)')
    parser.add_argument('--field-length', type=int, default=100, help='Length of each field in bytes (default: 100)')
    parser.add_argument('--insert-order', type=str, choices=['ordered', 'hashed'], default='ordered', help='Key insertion order: ordered (sequential) or hashed (distributed) (default: ordered)')

    # Load-specific parameters
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for insert_many (default: 100)')
    parser.add_argument('--drop-collection',required=False,action='store_true',help='Drop the collection (if it exists)')

    # Run-specific parameters
    parser.add_argument('--workload', type=str, choices=['A', 'B', 'C', 'D', 'E', 'F'], help='YCSB workload to run (A-F)')
    parser.add_argument('--operation-count', type=int, default=0, help='Number of operations to execute (0 = use --run-seconds)')
    parser.add_argument('--run-seconds', type=int, default=0, help='Duration in seconds to run (0 = use --operation-count)')
    parser.add_argument('--max-scan-length', type=int, default=100, help='Maximum scan length for scan operations (default: 100)')
    parser.add_argument('--rate-limit', type=int, default=0, help='Target operations per second across all threads (0 = unlimited)')

    # Output parameters
    parser.add_argument('--output-file', type=str, help='CSV file for performance metrics')
    parser.add_argument('--percentiles', type=parse_percentiles, default='50,90,95,99', help='Comma separated latency percentiles to report (default: 50,90,95,99)')
    parser.add_argument('--seed', type=int, default=42, help='Random seed (default: 42)')

    args = parser.parse_args()

    # Validate arguments
    if not args.load and not args.run:
        parser.error('Must specify either --load or --run')
    if args.load and args.run:
        parser.error('Cannot specify both --load and --run')

    if args.run:
        if not args.workload:
            parser.error('--workload is required for --run mode')
        if args.operation_count == 0 and args.run_seconds == 0:
            parser.error('Must specify either --operation-count or --run-seconds for --run mode')
        if args.operation_count > 0 and args.run_seconds > 0:
            parser.error('Cannot specify both --operation-count and --run-seconds')

    # Build config dict
    app_config = {
        'uri': args.uri,
        'database': args.database,
        'collection': args.collection,
        'threads': args.threads,
        'record_count': args.record_count,
        'field_count': args.field_count,
        'field_length': args.field_length,
        'batch_size': args.batch_size,
        'insert_order': args.insert_order,
        'workload': args.workload,
        'operation_count': args.operation_count,
        'run_seconds': args.run_seconds,
        'max_scan_length': args.max_scan_length,
        'rate_limit': args.rate_limit,
        'output_file': args.output_file,
        'percentiles': args.percentiles,
        'seed': args.seed,
        'mode_load': args.load,
        'mode_run': args.run,
        'drop_collection': args.drop_collection
    }

    print('---------------------------------------------------------------------------------------')
    for thisKey in sorted(app_config):
        if (thisKey == 'uri'):
            thisUri = app_config[thisKey]
            thisParsedUri = pymongo.uri_parser.parse_uri(thisUri)
            thisUsername = thisParsedUri['username']
            thisPassword = thisParsedUri['password']
            if thisUsername is not None:
                thisUri = thisUri.replace(thisUsername,'<USERNAME>')
            if thisPassword is not None:
                thisUri = thisUri.replace(thisPassword,'<PASSWORD>')
            print(f'  config | {thisKey} | {thisUri}')
        else:
            if type(app_config[thisKey]) == int:
                print(f'  config | {thisKey} | {app_config[thisKey]:,d}')
            elif type(app_config[thisKey]) == list:
                print(f'  config | {thisKey} | {",".join(app_config[thisKey])}')
            else:
                print(f'  config | {thisKey} | {app_config[thisKey]}')
    print(f'---------------------------------------------------------------------------------------')

    # Setup
    if args.load:
        setup_load(app_config)
    else:
        insert_start = setup_run(app_config)

    # Create shared queue and processes
    mp.set_start_method('spawn')
    perf_queue = mp.Queue()

    if args.run:
        # shared by all workers: next key number to insert, newest key known to be inserted
        insert_counter = mp.Value('q', insert_start)
        latest_key = mp.Value('q', insert_start - 1)

    # Common time origin for workers and the reporter
    app_config['start_time'] = time.time()

    # Start reporter thread
    reporter_thread = threading.Thread(target=reporter, args=(perf_queue, app_config))
    reporter_thread.start()

    process_list = []

    # Spawn workers
    for i in range(args.threads):
        if args.load:
            p = mp.Process(target=load_worker, args=(i, perf_queue, app_config))
        else:
            p = mp.Process(target=run_worker, args=(i, perf_queue, app_config, insert_counter, latest_key))
        process_list.append(p)

    # Start and join workers
    for p in process_list:
        p.start()
    for p in process_list:
        p.join()

    # Join reporter
    reporter_thread.join()

    # Collection information
    reportCollectionInfo(app_config)

    print('\nBenchmark completed.')


if __name__ == '__main__':
    main()
