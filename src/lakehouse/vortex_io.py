"""Vortex file format I/O utilities.

Provides read/write operations for Vortex files and Arrow <-> Vortex conversion.
Uses the vortex-data Python bindings.
"""

from pathlib import Path

import pyarrow as pa

import lakehouse._vortex_compat  # noqa: F401 — patches substrait for vortex
import vortex as vx


def write_vortex(table: pa.Table, path: str | Path, *, compact: bool = False) -> dict:
    """Write an Arrow table to a Vortex file.

    Args:
        table: PyArrow table to write
        path: Output file path
        compact: If True, optimize for smaller file size over read speed

    Returns:
        Dict with file path and size info
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if compact:
        vx.io.VortexWriteOptions.compact().write_path(table, str(path))
    else:
        vx.io.write(table, str(path))

    return {
        "path": str(path),
        "rows": table.num_rows,
        "size_bytes": path.stat().st_size,
    }


def read_vortex(path: str | Path) -> pa.Table:
    """Read a Vortex file into an Arrow table.

    Args:
        path: Path to the Vortex file

    Returns:
        PyArrow table with the file contents

    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Vortex file not found: {path}")

    vxf = vx.open(str(path))
    array = vxf.scan().read_all()
    return array.to_arrow_table()


def arrow_to_vortex(table: pa.Table) -> "vx.Array":
    """Convert an Arrow table to a Vortex array (in-memory).

    Args:
        table: PyArrow table to convert

    Returns:
        Vortex array
    """
    return vx.array(table)


def vortex_to_arrow(array: "vx.Array") -> pa.Table:
    """Convert a Vortex array to an Arrow table (in-memory).

    Args:
        array: Vortex array to convert

    Returns:
        PyArrow table
    """
    return array.to_arrow_table()


def convert_parquet_to_vortex(
    input_path: str | Path,
    output_path: str | Path | None = None,
    *,
    compact: bool = False,
) -> dict:
    """Convert a Parquet file to Vortex format.

    Args:
        input_path: Path to the Parquet file
        output_path: Path for the Vortex file (default: same name with .vortex extension)
        compact: If True, optimize for smaller file size

    Returns:
        Dict with conversion details
    """
    import pyarrow.parquet as pq

    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {input_path}")

    if output_path is None:
        output_path = input_path.with_suffix(".vortex")
    else:
        output_path = Path(output_path)

    table = pq.read_table(str(input_path))
    result = write_vortex(table, output_path, compact=compact)

    return {
        "input": str(input_path),
        "output": str(output_path),
        "rows": table.num_rows,
        "input_size": input_path.stat().st_size,
        "output_size": result["size_bytes"],
    }


def convert_vortex_to_parquet(
    input_path: str | Path,
    output_path: str | Path | None = None,
) -> dict:
    """Convert a Vortex file to Parquet format.

    Args:
        input_path: Path to the Vortex file
        output_path: Path for the Parquet file (default: same name with .parquet extension)

    Returns:
        Dict with conversion details
    """
    import pyarrow.parquet as pq

    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Vortex file not found: {input_path}")

    if output_path is None:
        output_path = input_path.with_suffix(".parquet")
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    table = read_vortex(input_path)
    pq.write_table(table, str(output_path))

    return {
        "input": str(input_path),
        "output": str(output_path),
        "rows": table.num_rows,
        "input_size": input_path.stat().st_size,
        "output_size": output_path.stat().st_size,
    }


def convert_table_to_vortex(
    catalog: "Catalog",
    table_name: str,
    output_dir: str | Path,
    *,
    compact: bool = False,
) -> dict:
    """Export an Iceberg table's current data to a Vortex file.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        output_dir: Directory to write the Vortex file
        compact: If True, optimize for smaller file size

    Returns:
        Dict with export details
    """
    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    arrow_table = table.scan().to_arrow()
    short_name = table_name.split(".")[-1]

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{short_name}.vortex"

    result = write_vortex(arrow_table, output_path, compact=compact)

    return {
        "table": table_name,
        "output": str(output_path),
        "rows": arrow_table.num_rows,
        "size_bytes": result["size_bytes"],
    }


def vortex_file_info(path: str | Path) -> dict:
    """Get metadata about a Vortex file without reading all data.

    Args:
        path: Path to the Vortex file

    Returns:
        Dict with file metadata

    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Vortex file not found: {path}")

    vxf = vx.open(str(path))
    dtype = vxf.dtype

    return {
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "dtype": str(dtype),
    }
