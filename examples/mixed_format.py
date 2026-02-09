"""Querying data across Parquet and Vortex formats.

Demonstrates registering tables from different formats and joining them
in a single DuckDB query.

Usage:
    uv run python examples/mixed_format.py
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from lakehouse.vortex_io import write_vortex
from lakehouse.query import QueryEngine

output_dir = Path("./example_output")
output_dir.mkdir(exist_ok=True)

# Create sample data in different formats
print("=== Creating sample data ===")

# Expenses in Parquet (frequent writes)
expenses = pa.table({
    "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    "category": pa.array(
        ["groceries", "transport", "dining", "utilities", "groceries"],
        type=pa.string(),
    ),
    "amount": pa.array([85.50, 24.00, 45.00, 120.00, 35.00], type=pa.float64()),
})
expenses_path = output_dir / "expenses.parquet"
pq.write_table(expenses, str(expenses_path))
print(f"Expenses (Parquet): {expenses.num_rows} rows")

# Category budgets in Vortex (read-heavy reference data)
budgets = pa.table({
    "category": pa.array(
        ["groceries", "transport", "dining", "utilities", "entertainment"],
        type=pa.string(),
    ),
    "monthly_budget": pa.array([400.0, 150.0, 200.0, 200.0, 100.0], type=pa.float64()),
})
budgets_path = output_dir / "budgets.vortex"
write_vortex(budgets, budgets_path)
print(f"Budgets (Vortex): {budgets.num_rows} rows")

# Query across both formats
print("\n=== Mixed-format queries ===")

engine = QueryEngine()

# Register the Vortex file
engine.register_vortex("budgets", budgets_path)

# Register the Parquet file via DuckDB
conn = engine._get_connection()
conn.execute(f"CREATE VIEW expenses AS SELECT * FROM read_parquet('{expenses_path}')")

# Join query across formats
print("\n--- Spending vs Budget by Category ---")
df = engine.execute("""
    SELECT
        e.category,
        sum(e.amount) as total_spent,
        b.monthly_budget,
        round(sum(e.amount) / b.monthly_budget * 100, 1) as pct_used
    FROM expenses e
    JOIN budgets b ON e.category = b.category
    GROUP BY e.category, b.monthly_budget
    ORDER BY pct_used DESC
""")
print(df.to_string(index=False))

# Aggregation query
print("\n--- Summary ---")
df = engine.execute("""
    SELECT
        count(*) as transactions,
        round(sum(e.amount), 2) as total_spent,
        round(sum(b.monthly_budget), 2) as total_budget
    FROM expenses e
    JOIN budgets b ON e.category = b.category
""")
print(df.to_string(index=False))

# Categories over budget
print("\n--- Over Budget ---")
df = engine.execute("""
    SELECT e.category, sum(e.amount) as spent, b.monthly_budget
    FROM expenses e
    JOIN budgets b ON e.category = b.category
    GROUP BY e.category, b.monthly_budget
    HAVING sum(e.amount) > b.monthly_budget * 0.5
    ORDER BY spent DESC
""")
if df.empty:
    print("No categories over 50% of budget")
else:
    print(df.to_string(index=False))

print("\nDone! Mixed-format queries work seamlessly.")
