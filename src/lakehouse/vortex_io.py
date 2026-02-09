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
