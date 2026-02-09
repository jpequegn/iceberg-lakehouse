# Vortex Format Guide

Vortex is a columnar file format by SpiralDB (now under Linux Foundation LFAI&Data) designed for fast analytical reads with smaller file sizes than Parquet.

## How Vortex Works in the Lakehouse

The lakehouse uses Apache Iceberg as its primary table format (with Parquet as the default storage format). Vortex integrates as an alternative storage format:

- **Iceberg tables** store data in Parquet (default) or can be exported to Vortex
- **Vortex files** can be queried directly via DuckDB using the Arrow bridge
- **Format conversion** between Parquet and Vortex is supported in both directions

## Reading and Writing Vortex Files

### Python API

```python
import pyarrow as pa
from lakehouse.vortex_io import write_vortex, read_vortex

# Write an Arrow table to Vortex
table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
result = write_vortex(table, "data.vortex")
# result: {"path": "data.vortex", "rows": 3, "size_bytes": 1234}

# Compact mode (smaller files, slower writes)
write_vortex(table, "data_compact.vortex", compact=True)

# Read a Vortex file back to Arrow
table = read_vortex("data.vortex")
```

### CLI

```bash
# Convert Parquet to Vortex
lakehouse convert data.parquet --to vortex

# Convert with compact mode
lakehouse convert data.parquet --to vortex --compact -o output.vortex

# Convert Vortex back to Parquet
lakehouse convert data.vortex --to parquet

# Export an Iceberg table to Vortex
lakehouse convert-table expenses -o ./exports
```

## Querying Vortex Files

### Via CLI

```bash
# Query a Vortex file directly
lakehouse query-vortex data.vortex "SELECT * FROM data WHERE amount > 100"

# Custom table name in SQL
lakehouse query-vortex data.vortex "SELECT * FROM expenses" --table-name expenses

# Output formats
lakehouse query-vortex data.vortex "SELECT count(*) FROM data" --format json
```

### Via Python

```python
from lakehouse.query import QueryEngine

engine = QueryEngine()

# Query a Vortex file directly
df = engine.query_vortex("SELECT * FROM data", "data.vortex")

# Register a Vortex file as a table for multiple queries
engine.register_vortex("metrics", "metrics.vortex")
df = engine.execute("SELECT * FROM metrics WHERE value > 100")
```

### Via MCP (LLM)

The `query_vortex` MCP tool lets Claude query Vortex files:

```
"Query the metrics.vortex file for the top 10 values"
→ Uses query_vortex tool with sql="SELECT * FROM data ORDER BY value DESC LIMIT 10"
```

## DuckDB Vortex Extension

The lakehouse automatically detects and uses the DuckDB Vortex extension when available. This provides native read support without going through the Arrow bridge.

```python
engine = QueryEngine()

# Check if the native extension is available
if engine.has_vortex:
    print("Native DuckDB Vortex extension loaded")
else:
    print("Using Arrow bridge fallback")
```

When the native extension is available:
- `register_vortex()` creates a DuckDB view using `read_vortex()`
- Queries can push down predicates directly to the Vortex reader

When using the Arrow bridge fallback:
- Vortex files are read into Arrow tables via `vortex-data`
- Arrow tables are registered with DuckDB via `conn.register()`

## Format Configuration

### Global Default

```bash
# Set Vortex as the default format
lakehouse config set-format vortex

# Check current default
lakehouse config get-format
```

### Per-Table Override

```bash
# Set format for a specific table
lakehouse config set-format parquet --table expenses

# View all configuration
lakehouse config show
```

### Iceberg Table Properties

```bash
# Set format via Iceberg table properties
lakehouse alter expenses set-property write.format.default vortex

# Check the property
lakehouse alter expenses get-property write.format.default
```

### Resolution Order

When determining which format to use, the lakehouse checks (in order):

1. **Write-time override** — explicit format passed to the operation
2. **Iceberg table property** — `write.format.default` on the table
3. **Per-table config** — `[tables.NAME]` section in config.toml
4. **Global default** — `[defaults]` section in config.toml
5. **Hardcoded fallback** — `parquet`

## In-Memory Conversion

For programmatic use without file I/O:

```python
from lakehouse.vortex_io import arrow_to_vortex, vortex_to_arrow

# Arrow → Vortex (in-memory)
vx_array = arrow_to_vortex(arrow_table)

# Vortex → Arrow (in-memory)
arrow_table = vortex_to_arrow(vx_array)
```

## File Metadata

```python
from lakehouse.vortex_io import vortex_file_info

info = vortex_file_info("data.vortex")
# {"path": "data.vortex", "size_bytes": 1234, "dtype": "..."}
```
