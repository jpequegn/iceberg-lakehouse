# Vortex Format Research

Research for issue #12: Evaluate Vortex as an alternative/complement to Parquet in our Iceberg lakehouse.

## 1. What is Vortex?

Vortex is a next-generation columnar file format created by SpiralDB, now an Incubation Stage project at LFAI&Data (Linux Foundation). It is designed as a drop-in replacement for Parquet with significant performance improvements.

- **License**: Apache-2.0
- **Language**: Rust core with Python, Java, and C bindings
- **File format stability**: Stable since v0.36.0 with backwards compatibility guarantees
- **Latest release**: v0.58.0 (January 2026)

Key performance claims vs Parquet:
- 100x faster random access reads
- 10-20x faster scans
- 5x faster writes
- Zero-copy compatibility with Apache Arrow

## 2. Python Bindings

**Package**: `vortex-data` on PyPI

```bash
uv add vortex-data
# or
pip install vortex-data
```

The Python bindings provide:
- Read/write Vortex files
- Convert between Arrow and Vortex arrays
- Access to Vortex's compression and encoding

Note: The Python/Java/C bindings do **not** follow semantic versioning and are released at high frequency. The file format itself has stability guarantees, but the API surface may change between releases.

## 3. Vortex vs Parquet

| Feature | Parquet | Vortex |
|---------|---------|--------|
| Maturity | Very mature, universal | Newer, growing adoption |
| Compression | Snappy, Zstd, etc. | Lightweight (ALP, FSST), operates on compressed data |
| Random access | Slow (row group level) | Fast (200x faster than Parquet) |
| Scan speed | Baseline | 10-20x faster |
| Write speed | Baseline | 5x faster |
| Arrow compat | Via deserialization | Zero-copy |
| Ecosystem | Universal | DuckDB, Spark, DataFusion, Polars |
| Iceberg support | Native | Experimental (Java JNI, not in PyIceberg) |

Vortex's key innovation: it can execute filter expressions on compressed data without decompression, and uses lightweight encodings (ALP for floats, FSST for strings) that are faster to decode than traditional compression.

## 4. DuckDB Integration

DuckDB has official Vortex support via an extension (announced January 2026):

```sql
INSTALL vortex;
LOAD vortex;

-- Read
SELECT * FROM read_vortex('data.vortex');

-- Write
COPY table TO 'output.vortex' (FORMAT vortex);
```

TPC-H benchmarks showed 18% faster performance vs Parquet V2 with lower variance.

This works via DuckDB's Python API as well, which is how our query engine operates.

## 5. Apache Iceberg Integration

Vortex + Iceberg integration exists but is **Java/Spark only**:

- Built using JNI bindings (Rust <-> Java bridge)
- Based on a Microsoft fork of Iceberg with pluggable file format support
- TPC-DS SF=1000 on Spark showed 30% runtime reduction, 20% storage reduction
- Some queries improved 2-4x

**PyIceberg does NOT support Vortex.** PyIceberg only supports Parquet as its data file format. There is no current work to add Vortex support to PyIceberg.

## 6. Integration Path for Our Project

### Option A: Vortex via DuckDB (Recommended)

Use Vortex as a **query-time optimization** via DuckDB's extension, without changing the Iceberg storage format:

1. Keep Parquet as the Iceberg storage format (PyIceberg limitation)
2. Add ability to export query results as Vortex files
3. Add ability to read external Vortex files via DuckDB
4. Benchmark against our current Parquet-based queries

This is low-risk and provides immediate value for read-heavy workloads.

### Option B: Dual Format Storage

Store data in both Parquet (via Iceberg) and Vortex (as sidecar files):

1. After Iceberg writes, convert Parquet to Vortex for faster reads
2. Query via Vortex when available, fall back to Parquet
3. Higher complexity and storage cost

### Option C: Wait for PyIceberg Vortex Support

Wait for the Iceberg pluggable file format work to land in PyIceberg:

- Currently Java-only via Microsoft's fork
- No timeline for PyIceberg support
- Could be months or years

## 7. Go/No-Go Recommendation

**Go** for Option A (DuckDB integration). Rationale:

- `vortex-data` Python package is available and stable
- DuckDB extension is official and production-ready
- No changes needed to our Iceberg catalog layer
- Low risk: Vortex is additive, not replacing Parquet
- Can benchmark immediately against our workloads

**Not yet** for replacing Parquet in Iceberg (Options B/C). Rationale:

- PyIceberg has no Vortex support
- The Iceberg file format plugin API is still in development
- Our data sizes don't yet warrant the complexity

## 8. Estimated Effort

| Task | Effort |
|------|--------|
| Add `vortex-data` dependency | 1 hour |
| Basic Vortex I/O utilities | 2-3 hours |
| DuckDB Vortex extension integration | 2-3 hours |
| Parquet <-> Vortex conversion tools | 3-4 hours |
| Table-level format configuration | 3-4 hours |
| Performance benchmarks | 4-6 hours |
| Documentation | 2-3 hours |
| **Total** | **~2-3 days** |

## References

- [Vortex GitHub](https://github.com/spiraldb/vortex)
- [vortex-data on PyPI](https://pypi.org/project/vortex-data/)
- [Vortex on Ice (Iceberg integration)](https://spiraldb.com/post/vortex-on-ice)
- [DuckDB Vortex Extension](https://duckdb.org/2026/01/23/duckdb-vortex-extension)
- [Towards Vortex 1.0](https://spiraldb.com/post/towards-vortex-10)
- [LF AI & Data Foundation announcement](https://www.linuxfoundation.org/press/lf-ai-data-foundation-hosts-vortex-project-to-power-high-performance-data-access-for-ai-and-analytics)
