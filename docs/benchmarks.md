# Parquet vs Vortex Performance Benchmarks

## Environment

- **Date**: 2026-02-09 10:34 UTC
- **Python**: 3.12.12
- **Platform**: macOS-26.2-arm64-arm-64bit
- **Processor**: arm
- **DuckDB**: 1.4.3
- **PyArrow**: 22.0.0
- **Vortex**: 0.58.0

## Test Configuration

- **Row counts**: 100, 1,000, 10,000
- **Data types**: numeric, string, mixed
- **Timing**: best of 3 runs

## Results

### File Size

| test | data_type | rows | parquet_bytes | vortex_bytes | ratio |
| --- | --- | --- | --- | --- | --- |
| size_comparison | numeric | 100 | 3,737 | 8,516 | 2.279s |
| size_comparison | numeric | 1000 | 23,464 | 17,452 | 744.0ms |
| size_comparison | numeric | 10,000 | 232,444 | 120,364 | 518.0ms |
| size_comparison | string | 100 | 5,591 | 11,580 | 2.071s |
| size_comparison | string | 1000 | 35,880 | 31,068 | 866.0ms |
| size_comparison | string | 10,000 | 303,824 | 230,324 | 758.0ms |
| size_comparison | mixed | 100 | 3,888 | 11,728 | 3.016s |
| size_comparison | mixed | 1000 | 17,165 | 13,528 | 788.0ms |
| size_comparison | mixed | 10,000 | 154,542 | 56,824 | 368.0ms |

### Query Performance (DuckDB)

| test | data_type | rows | parquet_time | vortex_time | speedup |
| --- | --- | --- | --- | --- | --- |
| select_all | numeric | 100 | 3.5ms | 3.7ms | 970.0ms |
| filtered | numeric | 100 | 3.2ms | 3.5ms | 920.0ms |
| aggregation | numeric | 100 | 3.1ms | 3.3ms | 930.0ms |
| group_by | numeric | 100 | 3.1ms | 3.6ms | 870.0ms |
| select_all | numeric | 1000 | 3.2ms | 3.4ms | 920.0ms |
| filtered | numeric | 1000 | 3.0ms | 3.4ms | 890.0ms |
| aggregation | numeric | 1000 | 3.0ms | 3.2ms | 940.0ms |
| group_by | numeric | 1000 | 3.1ms | 3.3ms | 920.0ms |
| select_all | numeric | 10,000 | 3.5ms | 3.5ms | 990.0ms |
| filtered | numeric | 10,000 | 3.3ms | 3.5ms | 950.0ms |
| aggregation | numeric | 10,000 | 3.2ms | 3.4ms | 930.0ms |
| group_by | numeric | 10,000 | 3.0ms | 3.5ms | 870.0ms |
| select_all | string | 100 | 3.2ms | 3.6ms | 880.0ms |
| filtered | string | 100 | 3.1ms | 3.3ms | 920.0ms |
| aggregation | string | 100 | 3.0ms | 3.2ms | 940.0ms |
| group_by | string | 100 | 3.3ms | 3.4ms | 970.0ms |
| select_all | string | 1000 | 3.5ms | 3.5ms | 990.0ms |
| filtered | string | 1000 | 3.2ms | 3.6ms | 900.0ms |
| aggregation | string | 1000 | 3.0ms | 3.4ms | 900.0ms |
| group_by | string | 1000 | 3.2ms | 3.5ms | 910.0ms |
| select_all | string | 10,000 | 4.7ms | 5.0ms | 930.0ms |
| filtered | string | 10,000 | 4.1ms | 4.4ms | 930.0ms |
| aggregation | string | 10,000 | 3.1ms | 3.8ms | 830.0ms |
| group_by | string | 10,000 | 3.2ms | 3.9ms | 820.0ms |
| select_all | mixed | 100 | 3.2ms | 3.4ms | 940.0ms |
| filtered | mixed | 100 | 3.2ms | 3.4ms | 930.0ms |
| aggregation | mixed | 100 | 3.1ms | 3.3ms | 920.0ms |
| group_by | mixed | 100 | 3.1ms | 3.4ms | 920.0ms |
| select_all | mixed | 1000 | 3.3ms | 3.5ms | 950.0ms |
| filtered | mixed | 1000 | 3.2ms | 3.4ms | 930.0ms |
| aggregation | mixed | 1000 | 3.0ms | 3.4ms | 890.0ms |
| group_by | mixed | 1000 | 3.1ms | 3.4ms | 910.0ms |
| select_all | mixed | 10,000 | 3.8ms | 4.2ms | 910.0ms |
| filtered | mixed | 10,000 | 3.5ms | 4.5ms | 780.0ms |
| aggregation | mixed | 10,000 | 3.6ms | 3.5ms | 1.040s |
| group_by | mixed | 10,000 | 3.2ms | 3.6ms | 910.0ms |

### Read Performance (Full Scan)

| test | data_type | rows | parquet_time | vortex_time | speedup |
| --- | --- | --- | --- | --- | --- |
| full_scan | numeric | 100 | 0.23ms | 0.18ms | 1.300s |
| full_scan | numeric | 1000 | 0.18ms | 0.14ms | 1.260s |
| full_scan | numeric | 10,000 | 0.21ms | 0.11ms | 1.910s |
| full_scan | string | 100 | 0.19ms | 0.14ms | 1.320s |
| full_scan | string | 1000 | 0.21ms | 0.15ms | 1.440s |
| full_scan | string | 10,000 | 0.77ms | 0.27ms | 2.850s |
| full_scan | mixed | 100 | 0.18ms | 0.13ms | 1.420s |
| full_scan | mixed | 1000 | 0.17ms | 0.13ms | 1.290s |
| full_scan | mixed | 10,000 | 0.28ms | 0.15ms | 1.880s |

### Write Performance

| test | data_type | rows | parquet_time | vortex_time | speedup |
| --- | --- | --- | --- | --- | --- |
| write | numeric | 100 | 0.13ms | 0.88ms | 150.0ms |
| write | numeric | 1000 | 0.20ms | 0.90ms | 220.0ms |
| write | numeric | 10,000 | 1.1ms | 1.2ms | 890.0ms |
| write | string | 100 | 0.16ms | 2.6ms | 60.0ms |
| write | string | 1000 | 0.34ms | 3.8ms | 90.0ms |
| write | string | 10,000 | 2.0ms | 7.6ms | 260.0ms |
| write | mixed | 100 | 0.14ms | 2.6ms | 50.0ms |
| write | mixed | 1000 | 0.23ms | 2.7ms | 90.0ms |
| write | mixed | 10,000 | 1.3ms | 4.0ms | 330.0ms |


## Notes

- **Speedup > 1.0** means Vortex is faster than Parquet
- **Speedup < 1.0** means Parquet is faster than Vortex
- **Ratio** for file size: Vortex size / Parquet size (< 1.0 = Vortex is smaller)
- Vortex queries use Arrow bridge (read via vortex-data, register in DuckDB)
- Parquet queries use DuckDB native `read_parquet()`
