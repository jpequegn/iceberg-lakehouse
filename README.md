# Iceberg Lakehouse

Local-first data lakehouse with Apache Iceberg storage, Vortex columnar format, and LLM access via MCP.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         LLM (Claude)                            │
│                    "Query my expenses..."                       │
└─────────────────────────┬───────────────────────────────────────┘
                          │ MCP Protocol
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MCP Server (lakehouse)                       │
│  Tools: query, insert, update, delete, upsert, convert, ...    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DuckDB                                   │
│         (in-memory, Iceberg + Vortex extensions)                │
└───────────────┬─────────────────────────┬───────────────────────┘
                │ PyIceberg               │ Arrow bridge
                ▼                         ▼
┌──────────────────────────┐ ┌────────────────────────────────────┐
│     Iceberg Tables       │ │        Vortex Files                │
│  ~/.lakehouse/warehouse/ │ │   (exported .vortex files)         │
│  ├── expenses/           │ │   Faster reads, smaller files      │
│  ├── health/             │ └────────────────────────────────────┘
│  └── notes/              │
└──────────────────────────┘
```

## Quick Start

```bash
# Install dependencies
cd iceberg-lakehouse
uv sync

# Initialize lakehouse (creates catalog + sample tables)
uv run lakehouse init --with-sample-data

# Query via CLI
uv run lakehouse query "SELECT * FROM expenses LIMIT 10"

# Start MCP server (for Claude Desktop)
uv run lakehouse serve
```

## Features

- **Iceberg Storage**: Full table versioning, time travel, schema evolution
- **Vortex Format**: Columnar format with 37-76% smaller files and 1.3-2.8x faster reads
- **DuckDB Queries**: Fast analytical queries with SQL
- **LLM Access**: Natural language queries via MCP (18 tools)
- **Local-First**: All data stays on your machine
- **Full CRUD**: Insert, update, delete, upsert, batch operations
- **Time Travel**: Query any historical snapshot of your data
- **Schema Evolution**: Add, drop, rename columns without rewriting data
- **Format Conversion**: Convert between Parquet and Vortex formats
- **Configurable Formats**: Global and per-table format preferences

## CLI Commands

```bash
# Data operations
lakehouse query "SELECT * FROM expenses WHERE amount > 100"
lakehouse query "SELECT * FROM expenses" --as-of 2025-12-01T00:00:00 --table-name expenses
lakehouse ingest data.csv expenses --format csv

# Table management
lakehouse tables                          # List all tables
lakehouse describe expenses               # Show table schema
lakehouse snapshots expenses              # List snapshots
lakehouse rollback expenses --snapshot-id 12345
lakehouse expire expenses --retain-last 5

# Schema evolution
lakehouse alter expenses add-column tags string
lakehouse alter expenses drop-column tags
lakehouse alter expenses rename-column desc description

# Batch operations
lakehouse batch '[{"action":"insert","table_name":"expenses","rows":[{"id":10,"amount":50}]}]'
lakehouse upsert expenses id '[{"id":1,"amount":90}]'
lakehouse delete expenses "id = 5" --force

# Vortex format
lakehouse convert data.parquet --to vortex
lakehouse convert data.vortex --to parquet
lakehouse convert-table expenses -o ./exports --compact
lakehouse query-vortex data.vortex "SELECT * FROM data"

# Configuration
lakehouse config show
lakehouse config set-format vortex
lakehouse config set-format parquet --table expenses
lakehouse alter expenses set-property write.format.default vortex

# Table properties
lakehouse alter expenses set-property write.format.default vortex
lakehouse alter expenses get-property write.format.default
lakehouse alter expenses remove-property write.format.default

# Benchmarks
lakehouse benchmark --rows 1000,10000,100000
lakehouse benchmark -o docs/benchmarks.md
```

## MCP Tools

The MCP server exposes 18 tools for LLM access:

| Tool | Description |
|------|-------------|
| `query` | Execute SQL queries (with time travel support) |
| `list_tables` | List available tables |
| `describe_table` | Get table schema |
| `insert` | Insert rows |
| `update` | Update rows matching a filter |
| `delete` | Delete rows matching a filter |
| `upsert` | Insert or update on key match |
| `alter_table` | Add, drop, rename columns |
| `batch` | Execute multiple operations |
| `rollback` | Rollback to a previous snapshot |
| `expire_snapshots` | Clean up old snapshots |
| `list_snapshots` | List available snapshots |
| `refresh` | Refresh table data |
| `convert_format` | Export table to Vortex |
| `query_vortex` | Query a Vortex file directly |
| `get_format_config` | Get format configuration |
| `set_format_config` | Set format preferences |
| `set_table_property` | Set Iceberg table properties |

## Claude Desktop Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "lakehouse": {
      "command": "uv",
      "args": ["--directory", "/path/to/iceberg-lakehouse", "run", "lakehouse", "serve"]
    }
  }
}
```

## Project Structure

```
iceberg-lakehouse/
├── pyproject.toml              # Dependencies (uv)
├── src/lakehouse/
│   ├── __init__.py
│   ├── cli.py                  # CLI commands (Click)
│   ├── server.py               # MCP server (18 tools)
│   ├── catalog.py              # Iceberg catalog + CRUD operations
│   ├── query.py                # DuckDB query engine + Vortex integration
│   ├── config.py               # Format configuration (TOML)
│   ├── vortex_io.py            # Vortex I/O and conversion utilities
│   └── _vortex_compat.py       # Substrait compatibility shim
├── benchmarks/
│   └── format_comparison.py    # Parquet vs Vortex benchmarks
├── docs/
│   ├── vortex.md               # Vortex format guide
│   ├── format-comparison.md    # When to use Parquet vs Vortex
│   ├── migration.md            # Data migration guide
│   ├── benchmarks.md           # Benchmark results
│   └── vortex-research.md      # Vortex research notes
├── examples/
│   ├── vortex_basic.py         # Basic Vortex usage
│   ├── migrate_to_vortex.py    # Migration example
│   └── mixed_format.py         # Mixed format queries
└── tests/                      # 172 tests
```

## Documentation

- [Vortex Format Guide](docs/vortex.md) — How to use Vortex with the lakehouse
- [Format Comparison](docs/format-comparison.md) — When to use Parquet vs Vortex
- [Migration Guide](docs/migration.md) — Converting existing data
- [Benchmark Results](docs/benchmarks.md) — Performance comparison

## Roadmap

- [x] Phase 1: Basic MCP server + Iceberg reads
- [x] Phase 2: Write support (insert, update, delete, upsert, batch, schema evolution, time travel, snapshots)
- [x] Phase 3: Vortex data format integration
