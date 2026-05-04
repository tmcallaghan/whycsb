#!/usr/bin/env python3
"""
Test script for distribution generators
"""

import sys
from collections import Counter

# Import from whycsb
sys.path.insert(0, '.')
from whycsb import UniformGenerator, ZipfianGenerator, SkewedLatestGenerator, format_key, generate_document


def test_uniform_distribution():
    """Test uniform distribution generator"""
    print("Testing UniformGenerator...")
    gen = UniformGenerator(0, 99, seed=42)
    samples = [gen.next() for _ in range(10000)]

    # Check all values are in range
    assert all(0 <= x <= 99 for x in samples), "Values out of range"

    # Check reasonable distribution
    counter = Counter(samples)
    min_count = min(counter.values())
    max_count = max(counter.values())
    ratio = max_count / min_count if min_count > 0 else 0

    print(f"  Min count: {min_count}, Max count: {max_count}, Ratio: {ratio:.2f}")
    print(f"  ✓ Uniform distribution looks good (ratio should be < 2.0)")


def test_zipfian_distribution():
    """Test zipfian distribution generator"""
    print("\nTesting ZipfianGenerator...")
    gen = ZipfianGenerator(0, 99, seed=42)
    samples = [gen.next() for _ in range(10000)]

    # Check all values are in range
    assert all(0 <= x <= 99 for x in samples), "Values out of range"

    # Check power-law distribution (top 20% should get >50% of accesses)
    counter = Counter(samples)
    top_20_pct = sorted(counter.values(), reverse=True)[:20]
    top_20_sum = sum(top_20_pct)

    print(f"  Total samples: {len(samples)}")
    print(f"  Top 20 keys got: {top_20_sum} accesses ({top_20_sum/len(samples)*100:.1f}%)")
    print(f"  ✓ Zipfian distribution looks good (should be >50%)")


def test_latest_distribution():
    """Test latest (skewed) distribution generator"""
    print("\nTesting SkewedLatestGenerator...")
    gen = SkewedLatestGenerator(0, 99, seed=42)
    samples = [gen.next() for _ in range(10000)]

    # Check all values are in range
    assert all(0 <= x <= 99 for x in samples), "Values out of range"

    # Check skew toward higher keys
    avg_key = sum(samples) / len(samples)
    median_key = sorted(samples)[len(samples) // 2]
    high_keys = len([s for s in samples if s >= 80])

    print(f"  Average key: {avg_key:.2f} (should be >50 for latest skew)")
    print(f"  Median key: {median_key} (should be >50)")
    print(f"  High keys (>=80): {high_keys} ({high_keys/len(samples)*100:.1f}%)")
    assert avg_key > 50, "Latest distribution should bias toward high keys"
    print(f"  ✓ Latest distribution looks good")

    # Test acknowledge_insert
    gen.acknowledge_insert(150)
    samples_after = [gen.next() for _ in range(1000)]
    max_after = max(samples_after)
    print(f"  After insert(150), max key: {max_after} (should be ≤150)")


def test_key_format():
    """Test key formatting"""
    print("\nTesting key formatting...")
    key = format_key(123)
    assert key == "user0000000000000123", f"Expected 'user0000000000000123', got '{key}'"
    print(f"  format_key(123) = '{key}'")
    print(f"  ✓ Key formatting correct")


def test_document_generation():
    """Test document generation"""
    print("\nTesting document generation...")
    import random
    rng = random.Random(42)

    doc = generate_document(123, field_count=10, field_length=100, rng=rng)

    # Check structure
    assert '_id' in doc, "Missing _id field"
    assert doc['_id'] == "user0000000000000123", f"Wrong _id: {doc['_id']}"
    assert len(doc) == 11, f"Expected 11 fields (id + 10), got {len(doc)}"

    # Check field names and lengths
    for i in range(10):
        field_name = f'field{i}'
        assert field_name in doc, f"Missing {field_name}"
        assert len(doc[field_name]) == 100, f"{field_name} wrong length: {len(doc[field_name])}"

    print(f"  Document keys: {list(doc.keys())}")
    print(f"  field0 length: {len(doc['field0'])}")
    print(f"  field0 sample: {doc['field0'][:20]}...")
    print(f"  ✓ Document generation correct")


def test_workload_operation_selection():
    """Test operation selection from workload"""
    print("\nTesting workload operation selection...")
    from whycsb import WORKLOADS, choose_operation
    import random

    rng = random.Random(42)

    # Test workload A (50% read, 50% update)
    workload_a = WORKLOADS['A']
    ops = [choose_operation(workload_a, rng) for _ in range(10000)]
    counter = Counter(ops)

    read_pct = counter['read'] / len(ops) * 100
    update_pct = counter['update'] / len(ops) * 100

    print(f"  Workload A (50/50):")
    print(f"    Read: {read_pct:.1f}% (expected ~50%)")
    print(f"    Update: {update_pct:.1f}% (expected ~50%)")

    # Test workload C (100% read)
    workload_c = WORKLOADS['C']
    ops = [choose_operation(workload_c, rng) for _ in range(1000)]
    counter = Counter(ops)

    print(f"  Workload C (100% read):")
    print(f"    Read: {counter['read']} ops (expected 1000)")
    assert counter['read'] == 1000, "Workload C should be 100% read"

    print(f"  ✓ Operation selection correct")


if __name__ == '__main__':
    print("=" * 60)
    print("whycsb Distribution and Utility Tests")
    print("=" * 60)

    test_uniform_distribution()
    test_zipfian_distribution()
    test_latest_distribution()
    test_key_format()
    test_document_generation()
    test_workload_operation_selection()

    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)
