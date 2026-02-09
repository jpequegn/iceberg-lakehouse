"""Basic Vortex file operations.

Demonstrates reading, writing, and querying Vortex files.

Usage:
    uv run python examples/vortex_basic.py
"""

import pyarrow as pa
from pathlib import Path
from lakehouse.vortex_io import write_vortex, read_vortex, vortex_file_info
from lakehouse.query import QueryEngine

# Create sample data
data = pa.table({
    "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    "name": pa.array(["Alice", "Bob", "Charlie", "Diana", "Eve"], type=pa.string()),
    "score": pa.array([95.5, 87.3, 92.1, 78.9, 88.4], type=pa.float64()),
})

output_dir = Path("./example_output")
output_dir.mkdir(exist_ok=True)

# Write to Vortex
print("=== Writing Vortex file ===")
result = write_vortex(data, output_dir / "scores.vortex")
print(f"Wrote {result['rows']} rows ({result['size_bytes']} bytes)")

# Write in compact mode
result_compact = write_vortex(data, output_dir / "scores_compact.vortex", compact=True)
print(f"Compact: {result_compact['size_bytes']} bytes")

# Read back
print("\n=== Reading Vortex file ===")
table = read_vortex(output_dir / "scores.vortex")
print(f"Read {table.num_rows} rows, {table.num_columns} columns")
print(f"Columns: {table.column_names}")

# File info
print("\n=== File info ===")
info = vortex_file_info(output_dir / "scores.vortex")
print(f"Size: {info['size_bytes']} bytes")
print(f"DType: {info['dtype']}")

# Query via DuckDB
print("\n=== DuckDB queries ===")
engine = QueryEngine()
df = engine.query_vortex(
    "SELECT name, score FROM data WHERE score > 90 ORDER BY score DESC",
    output_dir / "scores.vortex",
)
print("Students with score > 90:")
print(df.to_string(index=False))

# Register and query
print("\n=== Register and query ===")
engine.register_vortex("students", output_dir / "scores.vortex")
df = engine.execute("SELECT count(*) as total, avg(score) as avg_score FROM students")
print(f"Total: {df['total'].iloc[0]}, Average: {df['avg_score'].iloc[0]:.1f}")

print("\nDone! Files written to ./example_output/")
