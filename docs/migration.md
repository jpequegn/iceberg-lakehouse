# Migration Guide: Converting Data Between Formats

## Converting Iceberg Tables to Vortex

Export an Iceberg table's current data to a Vortex file:

```bash
# Basic export
lakehouse convert-table expenses

# Export to a specific directory
lakehouse convert-table expenses -o ./exports

# Compact mode (smaller file, slower write)
lakehouse convert-table expenses -o ./exports --compact
```

The Iceberg table itself remains unchanged (still in Parquet). This creates a separate `.vortex` file with the current snapshot's data.

### Python API

```python
from lakehouse.catalog import get_catalog
from lakehouse.vortex_io import convert_table_to_vortex

catalog = get_catalog()
result = convert_table_to_vortex(catalog, "expenses", "./exports")
# result: {"table": "default.expenses", "output": "./exports/expenses.vortex",
#          "rows": 1000, "size_bytes": 12345}
```

## Converting Between File Formats

### Parquet to Vortex

```bash
lakehouse convert data.parquet --to vortex
lakehouse convert data.parquet --to vortex -o output.vortex --compact
```

```python
from lakehouse.vortex_io import convert_parquet_to_vortex

result = convert_parquet_to_vortex("data.parquet")
# result: {"input": "data.parquet", "output": "data.vortex",
#          "rows": 1000, "input_size": 50000, "output_size": 30000}
```

### Vortex to Parquet

```bash
lakehouse convert data.vortex --to parquet
lakehouse convert data.vortex --to parquet -o output.parquet
```

```python
from lakehouse.vortex_io import convert_vortex_to_parquet

result = convert_vortex_to_parquet("data.vortex")
# result: {"input": "data.vortex", "output": "data.parquet",
#          "rows": 1000, "input_size": 30000, "output_size": 50000}
```

## Setting Default Format for New Data

### Global Default

Set the default format for all tables:

```bash
lakehouse config set-format vortex
```

This writes to `~/.lakehouse/config.toml`:

```toml
[defaults]
file_format = "vortex"
```

### Per-Table Format

Override the format for specific tables:

```bash
lakehouse config set-format parquet --table expenses
lakehouse config set-format vortex --table metrics
```

Config file:

```toml
[defaults]
file_format = "parquet"

[tables.metrics]
file_format = "vortex"
```

### Iceberg Table Properties

Set the format via Iceberg metadata (persisted with the table):

```bash
lakehouse alter expenses set-property write.format.default vortex
```

## Migration Workflow

### Step 1: Benchmark Your Data

Run benchmarks with your actual data sizes to see if Vortex is beneficial:

```bash
lakehouse benchmark --rows 1000,10000,100000
```

### Step 2: Export Test Data

Export a single table to Vortex and compare:

```bash
# Export to Vortex
lakehouse convert-table expenses -o ./test_export

# Query both formats
lakehouse query "SELECT count(*), sum(amount) FROM expenses"
lakehouse query-vortex ./test_export/expenses.vortex "SELECT count(*), sum(amount) FROM data"
```

### Step 3: Configure Format Preferences

If Vortex is beneficial for your workload:

```bash
# Set globally (all new data)
lakehouse config set-format vortex

# Or per-table (selective)
lakehouse config set-format vortex --table metrics
lakehouse config set-format vortex --table logs
```

### Step 4: Export Existing Tables

Export tables that would benefit from Vortex:

```bash
lakehouse convert-table metrics -o ./vortex_data --compact
lakehouse convert-table logs -o ./vortex_data --compact
```

## Data Integrity

All conversions preserve data integrity:

- Column names, types, and order are preserved
- Null values are preserved
- No data loss during Parquet ↔ Vortex roundtrips
- Verified by roundtrip tests in the test suite

## Limitations

- **Iceberg native Vortex**: PyIceberg does not yet support Vortex as a native storage format. Tables are stored in Parquet; Vortex is used for exports.
- **Schema metadata**: Some Iceberg-specific metadata (partition specs, sort orders) is not included in Vortex exports.
- **Incremental export**: Each export is a full snapshot. Incremental exports are not yet supported.
