# Parquet vs Vortex: When to Use Each Format

## Quick Decision Guide

| Use Case | Recommended Format |
|----------|-------------------|
| Default / general purpose | Parquet |
| Read-heavy analytics | Vortex |
| Frequent small writes | Parquet |
| Large batch exports | Vortex |
| Interoperability with other tools | Parquet |
| Minimizing storage | Vortex |
| String-heavy data | Vortex |

## Performance Summary

Based on benchmarks run on Apple Silicon (see [benchmarks.md](benchmarks.md)):

### File Size

Vortex produces significantly smaller files at scale:

| Data Type | 1K Rows | 10K Rows |
|-----------|---------|----------|
| Numeric | 0.74x | 0.52x |
| String | 0.87x | 0.76x |
| Mixed | 0.79x | 0.37x |

*Values < 1.0 mean Vortex is smaller. At very small scales (< 100 rows), Vortex has higher fixed overhead.*

### Read Speed

Vortex reads faster across all data types:

| Data Type | 10K Rows Speedup |
|-----------|-----------------|
| Numeric | 1.9x faster |
| String | 2.9x faster |
| Mixed | 1.9x faster |

### Write Speed

Parquet writes faster, especially for small datasets:

| Data Type | 10K Rows Speedup |
|-----------|-----------------|
| Numeric | 0.89x (Parquet ~10% faster) |
| String | 0.26x (Parquet ~4x faster) |
| Mixed | 0.33x (Parquet ~3x faster) |

### DuckDB Query Performance

Query performance is comparable. Parquet has a slight edge because DuckDB has a native Parquet reader, while Vortex goes through the Arrow bridge:

- Simple queries: ~equivalent
- Aggregations: Parquet slightly faster
- Filtered scans: ~equivalent

## Recommendations

### Use Parquet When

- **You're starting out**: Parquet is the default and most widely supported format
- **Frequent writes**: Ingesting data frequently (Parquet write overhead is lower)
- **Interoperability**: Other tools (Spark, Trino, Athena) need to read your data
- **Small datasets**: Below ~1000 rows, Parquet's fixed overhead is smaller

### Use Vortex When

- **Read-heavy workloads**: Your data is written once and read many times
- **Storage is a concern**: Vortex files are 37-76% smaller at scale
- **String-heavy data**: Vortex excels at compressing text columns
- **Batch exports**: Exporting large datasets for archival or sharing
- **Analytics snapshots**: Creating point-in-time exports for analysis

### Hybrid Approach

The lakehouse supports using both formats simultaneously:

1. Keep Iceberg tables in Parquet (for write performance and compatibility)
2. Export read-heavy datasets to Vortex (for query performance and storage)
3. Query both formats seamlessly via DuckDB

```bash
# Iceberg table stays in Parquet
lakehouse query "SELECT * FROM expenses"

# Export a snapshot to Vortex for fast analytics
lakehouse convert-table expenses -o ./analytics --compact

# Query the Vortex export
lakehouse query-vortex ./analytics/expenses.vortex "SELECT category, sum(amount) FROM data GROUP BY category"
```

## Technical Details

### Why Vortex is Smaller

Vortex uses adaptive encoding that selects the best compression strategy per column. For mixed and string data, this often produces better results than Parquet's row-group-level compression.

### Why Parquet Writes Faster

Parquet's writer is highly optimized and has lower fixed overhead. Vortex's adaptive encoding requires more analysis during writes, which adds latency (especially for small datasets).

### Why Read Performance Differs

Vortex's columnar layout and encoding are optimized for sequential reads. The format's metadata structure allows efficient column projection and predicate evaluation.
