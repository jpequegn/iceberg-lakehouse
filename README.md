# Iceberg Lakehouse

Local-first data lakehouse with Apache Iceberg storage and LLM access via MCP.

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
│  Tools: query, list_tables, describe_table, ingest             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DuckDB                                   │
│              (in-memory, Iceberg extension)                     │
└─────────────────────────┬───────────────────────────────────────┘
                          │ iceberg_scan()
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Iceberg Tables                               │
│  ~/.lakehouse/warehouse/                                        │
│  ├── expenses/     (financial data)                             │
│  ├── health/       (health metrics)                             │
│  ├── productivity/ (time tracking)                              │
│  └── media/        (transcripts, notes)                         │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Install dependencies
cd iceberg-lakehouse
uv sync

# Initialize lakehouse (creates catalog + sample data)
uv run lakehouse init

# Start MCP server (for Claude Desktop)
uv run lakehouse serve

# Or query directly via CLI
uv run lakehouse query "SELECT * FROM expenses LIMIT 10"
```

## Claude Desktop Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "lakehouse": {
      "command": "uv",
      "args": ["--directory", "/path/to/iceberg-lakehouse", "run", "lakehouse", "serve"],
      "env": {
        "LAKEHOUSE_PATH": "~/.lakehouse"
      }
    }
  }
}
```

## Project Structure

```
iceberg-lakehouse/
├── pyproject.toml           # Dependencies (uv)
├── src/
│   └── lakehouse/
│       ├── __init__.py
│       ├── cli.py           # CLI commands
│       ├── server.py        # MCP server
│       ├── catalog.py       # Iceberg catalog management
│       ├── query.py         # DuckDB query execution
│       └── ingest.py        # Data ingestion utilities
├── tests/
└── README.md
```

## Features

- **Iceberg Storage**: Full table versioning, time travel, schema evolution
- **DuckDB Queries**: Fast analytical queries with SQL
- **LLM Access**: Natural language queries via MCP
- **Local-First**: All data stays on your machine
- **PyIceberg Writes**: Full CRUD operations via Python

## Roadmap

- [ ] Phase 1: Basic MCP server + Iceberg reads
- [ ] Phase 2: Write support via PyIceberg
- [ ] Phase 3: Vortex data format integration (when stable)
