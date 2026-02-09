"""Tests for format configuration management."""

import pytest

from lakehouse.config import (
    get_default_format,
    set_default_format,
    get_table_format,
    set_table_format,
    get_config_summary,
    resolve_format,
    resolve_format_with_table,
    VALID_FORMATS,
)


@pytest.fixture
def config_path(tmp_path):
    """Return a temporary config file path."""
    return tmp_path / "config.toml"


class TestDefaultFormat:
    """Test global default format management."""

    def test_default_is_parquet(self, config_path):
        """Test that default format is parquet when no config exists."""
        assert get_default_format(config_path) == "parquet"

    def test_set_default_format(self, config_path):
        """Test setting the global default format."""
        set_default_format("vortex", config_path)
        assert get_default_format(config_path) == "vortex"

    def test_set_default_format_parquet(self, config_path):
        """Test setting format back to parquet."""
        set_default_format("vortex", config_path)
        set_default_format("parquet", config_path)
        assert get_default_format(config_path) == "parquet"

    def test_set_invalid_format_raises(self, config_path):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid format"):
            set_default_format("csv", config_path)

    def test_config_file_created(self, config_path):
        """Test that setting format creates the config file."""
        assert not config_path.exists()
        set_default_format("vortex", config_path)
        assert config_path.exists()


class TestTableFormat:
    """Test per-table format configuration."""

    def test_table_format_defaults_to_global(self, config_path):
        """Test that table format falls back to global default."""
        assert get_table_format("expenses", config_path) == "parquet"

    def test_table_format_uses_global_when_set(self, config_path):
        """Test that table format uses global default when changed."""
        set_default_format("vortex", config_path)
        assert get_table_format("expenses", config_path) == "vortex"

    def test_set_table_format_override(self, config_path):
        """Test setting a per-table format override."""
        set_table_format("expenses", "vortex", config_path)
        assert get_table_format("expenses", config_path) == "vortex"

    def test_table_override_takes_priority(self, config_path):
        """Test that per-table format overrides global default."""
        set_default_format("parquet", config_path)
        set_table_format("expenses", "vortex", config_path)
        assert get_table_format("expenses", config_path) == "vortex"
        # Other tables still use global default
        assert get_table_format("health", config_path) == "parquet"

    def test_set_table_invalid_format_raises(self, config_path):
        """Test that invalid table format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid format"):
            set_table_format("expenses", "csv", config_path)

    def test_qualified_table_name_stripped(self, config_path):
        """Test that namespace is stripped from qualified names."""
        set_table_format("default.expenses", "vortex", config_path)
        assert get_table_format("expenses", config_path) == "vortex"
        assert get_table_format("default.expenses", config_path) == "vortex"


class TestConfigSummary:
    """Test get_config_summary."""

    def test_empty_config(self, config_path):
        """Test summary with no config file."""
        summary = get_config_summary(config_path)
        assert summary["default_format"] == "parquet"
        assert summary["table_overrides"] == {}

    def test_full_config(self, config_path):
        """Test summary with global and table overrides."""
        set_default_format("vortex", config_path)
        set_table_format("expenses", "parquet", config_path)
        set_table_format("health", "vortex", config_path)

        summary = get_config_summary(config_path)
        assert summary["default_format"] == "vortex"
        assert summary["table_overrides"] == {
            "expenses": "parquet",
            "health": "vortex",
        }


class TestResolveFormat:
    """Test format resolution logic."""

    def test_override_takes_priority(self, config_path):
        """Test that write-time override takes highest priority."""
        set_default_format("parquet", config_path)
        set_table_format("expenses", "parquet", config_path)
        assert resolve_format("expenses", override="vortex", config_path=config_path) == "vortex"

    def test_invalid_override_raises(self, config_path):
        """Test that invalid override raises ValueError."""
        with pytest.raises(ValueError, match="Invalid format"):
            resolve_format("expenses", override="csv", config_path=config_path)

    def test_table_config_used(self, config_path):
        """Test that per-table config is used when no override."""
        set_table_format("expenses", "vortex", config_path)
        assert resolve_format("expenses", config_path=config_path) == "vortex"

    def test_global_fallback(self, config_path):
        """Test fallback to global default."""
        set_default_format("vortex", config_path)
        assert resolve_format("expenses", config_path=config_path) == "vortex"


class TestResolveFormatWithTable:
    """Test format resolution including Iceberg table properties."""

    def test_override_highest_priority(self, config_path):
        """Test write-time override beats everything."""
        props = {"write.format.default": "parquet"}
        set_table_format("expenses", "parquet", config_path)
        assert resolve_format_with_table(
            "expenses", props, override="vortex", config_path=config_path
        ) == "vortex"

    def test_table_property_beats_config(self, config_path):
        """Test Iceberg table property beats config file."""
        props = {"write.format.default": "vortex"}
        set_default_format("parquet", config_path)
        assert resolve_format_with_table(
            "expenses", props, config_path=config_path
        ) == "vortex"

    def test_config_used_when_no_property(self, config_path):
        """Test config file used when no table property."""
        set_table_format("expenses", "vortex", config_path)
        assert resolve_format_with_table(
            "expenses", {}, config_path=config_path
        ) == "vortex"

    def test_full_fallback_chain(self, config_path):
        """Test full resolution chain."""
        # No override, no table property, no table config -> global default
        set_default_format("vortex", config_path)
        assert resolve_format_with_table(
            "expenses", None, config_path=config_path
        ) == "vortex"

    def test_case_insensitive_property(self, config_path):
        """Test that table property value is case-insensitive."""
        props = {"write.format.default": "Vortex"}
        assert resolve_format_with_table(
            "expenses", props, config_path=config_path
        ) == "vortex"

    def test_invalid_property_ignored(self, config_path):
        """Test that invalid table property is ignored."""
        props = {"write.format.default": "csv"}
        set_default_format("vortex", config_path)
        assert resolve_format_with_table(
            "expenses", props, config_path=config_path
        ) == "vortex"
