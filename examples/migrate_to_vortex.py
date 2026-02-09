"""Migrating data from Parquet to Vortex format.

Demonstrates converting files and Iceberg tables to Vortex.

Usage:
    uv run python examples/migrate_to_vortex.py
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from lakehouse.vortex_io import (
    convert_parquet_to_vortex,
    convert_vortex_to_parquet,
    read_vortex,
)

output_dir = Path("./example_output")
output_dir.mkdir(exist_ok=True)

# Step 1: Create a sample Parquet file
print("=== Step 1: Create sample Parquet file ===")
data = pa.table({
    "id": pa.array(range(1000), type=pa.int64()),
    "category": pa.array(
        ["groceries", "transport", "dining", "utilities", "entertainment"] * 200,
        type=pa.string(),
    ),
    "amount": pa.array([float(i) * 1.5 + 10 for i in range(1000)], type=pa.float64()),
    "currency": pa.array(["USD", "EUR", "GBP", "JPY"] * 250, type=pa.string()),
})

parquet_path = output_dir / "expenses.parquet"
pq.write_table(data, str(parquet_path))
print(f"Created {parquet_path} ({parquet_path.stat().st_size:,} bytes, {data.num_rows} rows)")

# Step 2: Convert to Vortex
print("\n=== Step 2: Convert Parquet → Vortex ===")
result = convert_parquet_to_vortex(parquet_path, output_dir / "expenses.vortex")
print(f"Input:  {result['input_size']:,} bytes")
print(f"Output: {result['output_size']:,} bytes")
ratio = result['output_size'] / result['input_size']
print(f"Ratio:  {ratio:.2f}x ({(1 - ratio) * 100:.0f}% smaller)")

# Step 3: Verify data integrity
print("\n=== Step 3: Verify data integrity ===")
original = pq.read_table(str(parquet_path))
converted = read_vortex(output_dir / "expenses.vortex")
assert original.num_rows == converted.num_rows
assert original.column("id").to_pylist() == converted.column("id").to_pylist()
print(f"Rows match: {original.num_rows} == {converted.num_rows}")
print("Data integrity verified!")

# Step 4: Convert with compact mode
print("\n=== Step 4: Compact conversion ===")
result_compact = convert_parquet_to_vortex(
    parquet_path, output_dir / "expenses_compact.vortex", compact=True
)
print(f"Standard: {result['output_size']:,} bytes")
print(f"Compact:  {result_compact['output_size']:,} bytes")

# Step 5: Roundtrip (Parquet → Vortex → Parquet)
print("\n=== Step 5: Full roundtrip ===")
final_result = convert_vortex_to_parquet(
    output_dir / "expenses.vortex",
    output_dir / "expenses_roundtrip.parquet",
)
final = pq.read_table(str(output_dir / "expenses_roundtrip.parquet"))
assert final.num_rows == original.num_rows
assert final.column("amount").to_pylist() == original.column("amount").to_pylist()
print(f"Roundtrip verified: {final.num_rows} rows, data intact")

print("\nMigration complete! Files in ./example_output/")
