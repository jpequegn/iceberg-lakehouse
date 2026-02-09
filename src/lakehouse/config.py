"""Configuration management for Iceberg Lakehouse.

Supports a TOML config file at ~/.lakehouse/config.toml with global defaults
and per-table format overrides.
"""

from pathlib import Path
from typing import Optional

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

import tomli_w


DEFAULT_CONFIG_PATH = Path.home() / ".lakehouse" / "config.toml"

VALID_FORMATS = {"parquet", "vortex"}


def _load_config(config_path: Optional[Path] = None) -> dict:
    """Load config from TOML file."""
    path = config_path or DEFAULT_CONFIG_PATH
    if not path.exists():
        return {}
    with open(path, "rb") as f:
        return tomllib.load(f)


def _save_config(data: dict, config_path: Optional[Path] = None) -> None:
    """Save config to TOML file."""
    path = config_path or DEFAULT_CONFIG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        tomli_w.dump(data, f)


def get_default_format(config_path: Optional[Path] = None) -> str:
    """Get the global default file format.

    Returns:
        Format string ('parquet' or 'vortex'). Defaults to 'parquet'.
    """
    config = _load_config(config_path)
    return config.get("defaults", {}).get("file_format", "parquet")


def set_default_format(fmt: str, config_path: Optional[Path] = None) -> None:
    """Set the global default file format.

    Args:
        fmt: Format string ('parquet' or 'vortex')
        config_path: Optional config file path

    Raises:
        ValueError: If format is not valid
    """
    if fmt not in VALID_FORMATS:
        raise ValueError(f"Invalid format '{fmt}'. Must be one of: {', '.join(sorted(VALID_FORMATS))}")

    config = _load_config(config_path)
    if "defaults" not in config:
        config["defaults"] = {}
    config["defaults"]["file_format"] = fmt
    _save_config(config, config_path)


def get_table_format(
    table_name: str,
    config_path: Optional[Path] = None,
) -> str:
    """Get the configured file format for a specific table.

    Resolution order:
    1. Per-table config override
    2. Global default
    3. 'parquet' (hardcoded fallback)

    Args:
        table_name: Table name (short name without namespace)

    Returns:
        Format string ('parquet' or 'vortex')
    """
    config = _load_config(config_path)

    # Check per-table override
    short_name = table_name.split(".")[-1] if "." in table_name else table_name
    table_fmt = config.get("tables", {}).get(short_name, {}).get("file_format")
    if table_fmt and table_fmt in VALID_FORMATS:
        return table_fmt

    # Fall back to global default
    return config.get("defaults", {}).get("file_format", "parquet")


def set_table_format(
    table_name: str,
    fmt: str,
    config_path: Optional[Path] = None,
) -> None:
    """Set the file format for a specific table.

    Args:
        table_name: Table name (short name without namespace)
        fmt: Format string ('parquet' or 'vortex')
        config_path: Optional config file path

    Raises:
        ValueError: If format is not valid
    """
    if fmt not in VALID_FORMATS:
        raise ValueError(f"Invalid format '{fmt}'. Must be one of: {', '.join(sorted(VALID_FORMATS))}")

    short_name = table_name.split(".")[-1] if "." in table_name else table_name
    config = _load_config(config_path)

    if "tables" not in config:
        config["tables"] = {}
    if short_name not in config["tables"]:
        config["tables"][short_name] = {}

    config["tables"][short_name]["file_format"] = fmt
    _save_config(config, config_path)


def get_config_summary(config_path: Optional[Path] = None) -> dict:
    """Get a summary of all configuration.

    Returns:
        Dict with 'default_format' and 'table_overrides'
    """
    config = _load_config(config_path)
    default_fmt = config.get("defaults", {}).get("file_format", "parquet")

    table_overrides = {}
    for table_name, table_config in config.get("tables", {}).items():
        fmt = table_config.get("file_format")
        if fmt:
            table_overrides[table_name] = fmt

    return {
        "default_format": default_fmt,
        "table_overrides": table_overrides,
    }


def resolve_format(
    table_name: str,
    override: Optional[str] = None,
    config_path: Optional[Path] = None,
) -> str:
    """Resolve the effective file format for a table.

    Resolution order:
    1. Write-time override (if provided)
    2. Iceberg table property 'write.format.default' (checked externally)
    3. Per-table config
    4. Global default
    5. 'parquet' (hardcoded fallback)

    Note: Iceberg table property is not checked here — use
    resolve_format_with_table() for full resolution including table properties.

    Args:
        table_name: Table name
        override: Write-time format override
        config_path: Optional config file path

    Returns:
        Resolved format string
    """
    if override:
        if override not in VALID_FORMATS:
            raise ValueError(f"Invalid format '{override}'. Must be one of: {', '.join(sorted(VALID_FORMATS))}")
        return override

    return get_table_format(table_name, config_path)


def resolve_format_with_table(
    table_name: str,
    table_properties: Optional[dict] = None,
    override: Optional[str] = None,
    config_path: Optional[Path] = None,
) -> str:
    """Resolve the effective file format including Iceberg table properties.

    Resolution order:
    1. Write-time override
    2. Iceberg table property 'write.format.default'
    3. Per-table config
    4. Global default
    5. 'parquet' (hardcoded fallback)

    Args:
        table_name: Table name
        table_properties: Iceberg table properties dict
        override: Write-time format override
        config_path: Optional config file path

    Returns:
        Resolved format string
    """
    if override:
        if override not in VALID_FORMATS:
            raise ValueError(f"Invalid format '{override}'. Must be one of: {', '.join(sorted(VALID_FORMATS))}")
        return override

    # Check Iceberg table property
    if table_properties:
        prop_fmt = table_properties.get("write.format.default")
        if prop_fmt and prop_fmt.lower() in VALID_FORMATS:
            return prop_fmt.lower()

    return get_table_format(table_name, config_path)
