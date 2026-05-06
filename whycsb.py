#!/usr/bin/env python3
"""
whycsb - Python implementation of YCSB for MongoDB-compatible databases
"""

import argparse
import csv
import multiprocessing as mp
import random
import string
import threading
import time
import warnings
from collections import defaultdict
import datetime as dt

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

        # Calculate zeta values
        self.theta = self.ZIPFIAN_CONSTANT
        self.alpha = 1.0 / (1.0 - self.theta)
        self.zeta2theta = self._zeta(2, self.theta)
        self.zetan = self.ZETAN
        self.eta = (1.0 - pow(2.0, -self.theta)) / (1.0 - self.zeta2theta / self.zetan)

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
            zipfian_value = int(self.num_items * pow(self.eta * u - self.eta + 1, self.alpha))

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

def format_key(key_num):
    """Format key as YCSB-style user key"""
    return f'user{str(key_num).zfill(16)}'


def random_string(length, rng=None):
    """Generate random alphanumeric string of specified length"""
    if rng is None:
        rng = random
    chars = string.ascii_letters + string.digits
    return ''.join(rng.choice(chars) for _ in range(length))


def generate_document(key, field_count=10, field_length=100, rng=None):
    """Generate YCSB-style document"""
    doc = {'_id': format_key(key)}
    for i in range(field_count):
        doc[f'field{i}'] = random_string(field_length, rng)
    return doc


def get_timestamp():
    """Get ISO 8601 timestamp with milliseconds"""
    return dt.datetime.now(dt.timezone.utc).isoformat()[:-3] + 'Z'


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

    # Load data in batches
    batch = []
    for key in range(start_key, end_key):
        batch.append(generate_document(key, field_count, field_length, rng))

        if len(batch) >= batch_size:
            start_time = time.time()
            col.insert_many(batch, ordered=False)
            latency_ms = (time.time() - start_time) * 1000
            perf_queue.put({
                'name': 'opCompleted',
                'latency': latency_ms,
                'opType': 'insert',
                'opCount': len(batch)
            })
            batch = []

    # Insert remaining batch
    if batch:
        start_time = time.time()
        col.insert_many(batch, ordered=False)
        latency_ms = (time.time() - start_time) * 1000
        perf_queue.put({
            'name': 'opCompleted',
            'latency': latency_ms,
            'opType': 'insert',
            'opCount': len(batch)
        })

    client.close()
    perf_queue.put({'name': 'processCompleted', 'processNum': thread_num})


def run_worker(thread_num, perf_queue, app_config):
    """Worker process for running workload"""

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

    # number of seconds between reporting perf
    perfReportIntervalSeconds = 1
    nextPerfReportTime = time.time() + perfReportIntervalSeconds
    perfDict = {}

    # Distribution generator
    key_gen = get_distribution_generator(
        workload['distribution'],
        0,
        record_count - 1,
        app_config['seed'] + thread_num
    )

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

    # Insert counter for workloads D and E
    insert_counter = 0

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
                    time.sleep(sleep_time)
                next_report_time = time.time() + interval_seconds
                ops_this_interval = 0

        # Choose operation
        op_type = choose_operation(workload, rng)

        # Execute operation
        op_start = time.time()

        if op_type == 'read':
            key = format_key(key_gen.next())
            col.find_one({'_id': key})

        elif op_type == 'update':
            key = format_key(key_gen.next())
            field_to_update = f'field{rng.randint(0, field_count - 1)}'
            new_value = random_string(field_length, rng)
            col.update_one({'_id': key}, {'$set': {field_to_update: new_value}})

        elif op_type == 'scan':
            start_key = format_key(key_gen.next())
            scan_length = rng.randint(1, max_scan_length)
            cursor = col.find({'_id': {'$gte': start_key}}).limit(scan_length)
            list(cursor)

        elif op_type == 'readmodifywrite':
            key = format_key(key_gen.next())
            doc = col.find_one({'_id': key})
            if doc:
                for i in range(field_count):
                    doc[f'field{i}'] = random_string(field_length, rng)
                col.replace_one({'_id': key}, doc)

        elif op_type == 'insert':
            next_key = record_count + insert_counter
            insert_counter += 1
            doc = generate_document(next_key, field_count, field_length, rng)
            col.insert_one(doc)
            # Update latest generator if applicable
            if isinstance(key_gen, SkewedLatestGenerator):
                key_gen.acknowledge_insert(next_key)

        op_latency = (time.time() - op_start) * 1000

        if op_type not in perfDict:
            perfDict[op_type] = {}
            perfDict[op_type]['latency'] = 0.0
            perfDict[op_type]['opCount'] = 0

        perfDict[op_type]['latency'] += op_latency
        perfDict[op_type]['opCount'] += 1

        # Report perf data
        if time.time() > nextPerfReportTime:
            nextPerfReportTime = time.time() + perfReportIntervalSeconds

            for thisKey in perfDict:
                perf_queue.put({
                    'name': 'opCompleted',
                    'latency': perfDict[thisKey]['latency'],
                    'opType': thisKey,
                    'opCount': perfDict[thisKey]['opCount']
                })
                perfDict[thisKey]['latency'] = 0.0
                perfDict[thisKey]['opCount'] = 0

        ops_completed += 1
        ops_this_interval += 1

    client.close()
    perf_queue.put({'name': 'processCompleted', 'processNum': thread_num})


# Reporter function

def reporter(perf_queue, app_config):
    """Reporter thread for collecting and displaying performance metrics"""
    interval_seconds = 10
    window_size = 10
    recent_tps = []
    recent_latency = []

    op_stats = defaultdict(lambda: {'count': 0, 'total_latency': 0})
    total_ops = 0
    start_time = time.time()
    num_completed = 0
    num_threads = app_config['threads']

    # CSV output
    csv_file = None
    csv_writer = None
    if app_config.get('output_file'):
        csv_file = open(app_config['output_file'], 'w', newline='')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([
            'timestamp', 'elapsed_time', 'elapsed_seconds', 'total_operations',
            'overall_tps', 'interval_tps', 'interval_latency_ms',
            'window_tps', 'window_latency_ms'
        ])

    last_report_time = start_time

    while num_completed < num_threads:
        time.sleep(interval_seconds)

        # Drain queue
        interval_ops = 0
        interval_latency = 0
        while not perf_queue.empty():
            try:
                msg = perf_queue.get_nowait()
                if msg['name'] == 'opCompleted':
                    op_type = msg['opType']
                    op_count = msg.get('opCount', 1)
                    op_stats[op_type]['count'] += op_count
                    op_stats[op_type]['total_latency'] += msg['latency']
                    interval_ops += op_count
                    interval_latency += msg['latency']
                    total_ops += op_count
                elif msg['name'] == 'processCompleted':
                    num_completed += 1
            except:
                break

        # Calculate metrics
        elapsed = time.time() - start_time
        overall_tps = total_ops / elapsed if elapsed > 0 else 0

        actual_interval = time.time() - last_report_time
        interval_tps = interval_ops / actual_interval if actual_interval > 0 else 0
        interval_avg_latency = interval_latency / interval_ops if interval_ops > 0 else 0
        last_report_time = time.time()

        # Sliding window
        recent_tps.append(interval_tps)
        recent_latency.append(interval_avg_latency)
        if len(recent_tps) > window_size:
            recent_tps.pop(0)
            recent_latency.pop(0)

        window_tps = sum(recent_tps) / len(recent_tps) if recent_tps else 0
        window_latency = sum(recent_latency) / len(recent_latency) if recent_latency else 0

        # Print to stdout
        print(f'[{get_timestamp()}] elapsed {format_elapsed(elapsed)} | '
              f'total ops {total_ops:,} at {overall_tps:,.2f} op/s | '
              f'interval {interval_tps:,.2f} op/s @ {interval_avg_latency:.2f} ms | '
              f'last {window_size} {window_tps:,.2f} op/s @ {window_latency:.2f} ms')

        # Write to CSV
        if csv_writer:
            csv_writer.writerow([
                get_timestamp(),
                format_elapsed(elapsed),
                f'{elapsed:.2f}',
                total_ops,
                f'{overall_tps:.2f}',
                f'{interval_tps:.2f}',
                f'{interval_avg_latency:.2f}',
                f'{window_tps:.2f}',
                f'{window_latency:.2f}'
            ])
            csv_file.flush()

    # Final report
    elapsed = time.time() - start_time
    print(f'\n=== Final Statistics ===')
    print(f'Total operations: {total_ops:,}')
    print(f'Total time: {format_elapsed(elapsed)}')
    print(f'Overall throughput: {total_ops / elapsed:,.2f} op/s')
    print(f'\nOperation breakdown:')
    for op_type, stats in sorted(op_stats.items()):
        count = stats['count']
        if count > 0:
            avg_latency = stats['total_latency'] / count
            pct = (count / total_ops) * 100
            print(f'  {op_type:20s}: {count:10,} ops ({pct:5.1f}%) @ {avg_latency:.2f} ms avg')

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


def setup_run(app_config):
    """Setup for run mode"""
    print(f"Running workload {app_config['workload']} against "
          f"{app_config['database']}.{app_config['collection']} "
          f"using {app_config['threads']} threads...")


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
        'workload': args.workload,
        'operation_count': args.operation_count,
        'run_seconds': args.run_seconds,
        'max_scan_length': args.max_scan_length,
        'rate_limit': args.rate_limit,
        'output_file': args.output_file,
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
            else:
                print(f'  config | {thisKey} | {app_config[thisKey]}')
    print(f'---------------------------------------------------------------------------------------')

    # Setup
    if args.load:
        setup_load(app_config)
    else:
        setup_run(app_config)

    # Create shared queue and processes
    mp.set_start_method('spawn')
    perf_queue = mp.Manager().Queue()

    # Start reporter thread
    reporter_thread = threading.Thread(target=reporter, args=(perf_queue, app_config))
    reporter_thread.start()

    process_list = []

    # Spawn workers
    for i in range(args.threads):
        if args.load:
            p = mp.Process(target=load_worker, args=(i, perf_queue, app_config))
        else:
            p = mp.Process(target=run_worker, args=(i, perf_queue, app_config))
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
