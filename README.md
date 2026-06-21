```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ p a r q u e t ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-parquet/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[PARQUET TOOLKIT FOR STRYKE // SCHEMA + STATS + ROW-GROUPS + HEAD/TAIL + CSV/JSON IN-OUT + MERGE + RECOMPRESS]`

> *"See into parquet without loading it."*

Parquet file inspector for stryke — schema, footer stats, row-group
breakdown, head/tail, recompression. Diagnostic counterpart to
[stryke-arrow](../stryke-arrow). Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-arrow`](https://github.com/MenkeTechnologies/stryke-arrow) · [`stryke-duckdb`](https://github.com/MenkeTechnologies/stryke-duckdb) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] What this is (and what it isn't)](#0x00-what-this-is-and-what-it-isnt)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] FFI layer](#0x03-ffi-layer)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x06\] Supported compression codecs](#0x06-supported-compression-codecs)
- [\[0x07\] Tests](#0x07-tests)
- [\[0x08\] Dev workflow](#0x08-dev-workflow)
- [\[0x09\] Layout](#0x09-layout)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] What this is (and what it isn't)

| | stryke-arrow | **stryke-parquet** |
|---|---|---|
| Surface | Full Arrow data pipeline: parquet / IPC / CSV / JSON / Feather read+write, conversion, DataFrame bridge | **Parquet-only diagnostics** — inspect, schema, head/tail, row-group breakdown, per-column stats, recompress |
| Best for | "I want to read or write parquet data and do things with it" | "I have a parquet file and I want to understand it" |
| Reference | arrow-rs | `pqrs` / `parquet-tools` |
| Binary size | ~8.5 MB | ~6 MB |
| Output | NDJSON / columnar / Arrow IPC | NDJSON / one-line JSON |

The two packages share the same `parquet` + `arrow` crates underneath but
serve different user intents. Use stryke-arrow when parquet is one stop
in a data pipeline; use stryke-parquet when parquet itself is the
artifact you're investigating.

## [0x01] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-parquet
```

From a local checkout:

```sh
cd ~/projects/stryke-parquet
cargo build --release            # produces target/release/libstryke_parquet.{dylib,so}
s pkg install -g .               # installs into ~/.stryke/store/parquet@<version>/
```

Or:

```sh
make install
```

## [0x02] Quick start

```stryke
use Parquet

# Footer-only — fast, no row scan.
p to_json Parquet::inspect "events.parquet"
p Parquet::count   "events.parquet"
p to_json Parquet::schema "events.parquet"

# Per-row-group breakdown.
val @rgs = Parquet::rowgroups "events.parquet"
for val $rg (@rgs) {
    p "rg $rg->{ordinal}: $rg->{num_rows} rows, $rg->{total_compressed_size} bytes"
}

# Per-column stats (aggregated across row groups).
Parquet::stats "events.parquet" |> ep
Parquet::stats "events.parquet", column => "user_id"

# Peek at rows.
val @first = Parquet::head "events.parquet", n => 20
val @last  = Parquet::tail "events.parquet", n => 5
val @cols  = Parquet::head "events.parquet", n => 10, columns => ["user_id", "ts"]

# Stream every row (no full-result buffering — for big files).
Parquet::stream "events.parquet",
    callback => fn { process _ }

# Convert / recompress.
val $csv = Parquet::to_csv "events.parquet"                       # → scalar
Parquet::to_csv "events.parquet", output => "events.csv"          # → file
Parquet::compress "events.parquet", "events.zst.parquet",
                  codec => "zstd"                                  # recompress
```

## [0x03] FFI layer

Each `Parquet::*` wrapper builds a JSON args dict and calls a sibling
`parquet__*` symbol resolved out of `libstryke_parquet.{dylib,so}`. The
cdylib is dlopened in-process on first `use Parquet` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook). Its exports span
inspection (`version`, `inspect`, `schema`, `dtypes`, `count`, `rowgroups`,
`stats`, `metadata`), row read (`head`, `tail`, `to_json`, `to_csv`,
`to_ndjson`), row predicates / reshaping (`filter`, `where_count`,
`filter_to`, `distinct`, `sort`, `column`, `sum`, `describe`, `group_by`,
`random_sample`), conversion (`from_csv`, `from_json`, `write`,
`write_partitioned`, `compress`, `repartition`, `merge`, `select`), and
diagnostics (`validate`, `column_chunk_stats`, `size_report`, `null_summary`,
`encoding_summary`, `row_group_summary`, `sample`, `features`). The
authoritative list is `[ffi].exports` in `stryke.toml`.

Stateless package — parquet operations are file transforms; no
process-level cache.

## [0x04] API reference

```stryke
Parquet::inspect    $path → \%info
Parquet::schema     $path → { fields, num_fields }
Parquet::count      $path → $n
Parquet::rowgroups  $path → @rgs
Parquet::row_group_summary $path → { num_row_groups, total_rows, total_compressed_size, rows_per_group:{min,max,mean}, compressed_bytes_per_group:{min,max,mean} }   # footer-only sizing rollup
Parquet::stats      $path, %opts → @stats         # opts: column
Parquet::head       $path, %opts → @rows          # opts: n, columns
Parquet::slice      $path, %opts → @rows          # opts: offset, length (to end if omitted), columns — offset window
Parquet::tail       $path, %opts → @rows
Parquet::reverse    $path, %opts → @rows          # every row in reverse file order (newest-first); opts: columns
Parquet::gather     $path, \@indices, %opts → @rows  # rows by explicit 0-based index list (arbitrary order, repeats OK, out-of-range dies); opts: columns
Parquet::top_k      $path, $column, $k, %opts → @rows # the k rows with the largest values in $column (polars top_k); opts: descending => 0 for the smallest (bottom_k); nulls sort last, k caps at row count
Parquet::value_counts $path, $column → @rows         # { value, count } per distinct value in $column (pandas/polars value_counts); sorted by count desc then value asc
Parquet::to_json    $path, %opts → @rows
Parquet::stream     $path, %opts → $count         # callback per row
Parquet::to_csv     $path, %opts → $csv | $path   # opts: output, columns
Parquet::compress   $src, $dst, %opts → \%resp    # opts: codec, row_group
Parquet::from_csv   $src, $dst, %opts → \%resp    # CSV → parquet; opts: header, delimiter, codec
Parquet::from_json  $src, $dst, %opts → \%resp    # NDJSON → parquet; opts: codec
Parquet::write      \@rows, $dst, %opts → \%resp  # in-memory rows (hashrefs) → parquet; opts: codec
Parquet::write_partitioned \@rows, $dst, $column, %opts → \%resp  # Hive col=val/ dirs; opts: codec
Parquet::merge      \@srcs, $dst, %opts → \%resp  # concat same-schema files (vertical); opts: codec
Parquet::hstack     $path, $other, $dst, %opts → \%resp  # horizontal: append $other's columns at matching row count; column-name collision errors
Parquet::select     $path, $dst, \@cols, %opts → \%resp  # project a column subset into a new file (column pruning); unknown column errors
Parquet::drop       $path, $dst, \@cols, %opts → \%resp  # complement of select: keep all columns but \@cols; unknown column / drop-all errors
Parquet::rename     $path, $dst, \%map, %opts → \%resp   # relabel columns { old => new }; preserves types/order/rows; unknown column / name-collision errors
Parquet::repartition $src, $dst, %opts → \%resp   # rewrite with a target max row-group row count; opts: row_group (default 65536), codec
Parquet::to_ndjson  $path, $dst → \%resp          # parquet → NDJSON file (inverse of from_json)
Parquet::dtypes     $path → \%resp                # each column's Arrow logical type: { num_fields, columns:[{name, dtype, nullable}] }
Parquet::metadata   $path → \%resp                # writer kv metadata + created_by + version
```

### Row predicates / reshaping (polars-style)

Materialize the file once, then transform the JSON rows. Comparison is numeric
for numbers, lexicographic for strings; nulls always sort last.

```stryke
Parquet::filter      $path, $column, $op, %opts → @rows   # rows where $column OP $value (eq/ne/gt/ge/lt/le, = != > >= < <=, is_null/is_not_null); opts: value, columns
Parquet::where_count $path, $column, $op, %opts → $n      # count-only companion to filter (same grammar); opts: value
Parquet::filter_to   $path, $dst, $column, $op, %opts → \%resp  # write only matching rows to a new file; empty result errors; opts: value, codec
Parquet::distinct    $path, %opts → @rows                 # unique rows (polars unique); opts: columns (key + projection); first occurrence wins
Parquet::sort        $path, $column, %opts → @rows        # ORDER BY $column; opts: descending => 1, columns; nulls last, stable
Parquet::column      $path, $column → @values             # a single column's cells as a flat list (polars Series)
Parquet::sum         $path, $column → $sum                # numeric sum of $column (nulls/non-numeric skipped)
Parquet::describe    $path → \%resp                       # per-column { count, null_count, min, max, mean, sum }; non-numeric → null numerics
Parquet::group_by    $path, $by, %opts → @groups          # GROUP BY $by → { key, count, value }; opts: agg, func (count/sum/min/max/mean)
Parquet::random_sample $path, %opts → @rows               # n random rows (reservoir, reproducible per seed); opts: n, seed, columns; keeps file order
```

### Diagnostics

```stryke
Parquet::validate            $path → { ok, rows, row_groups } | { ok:false, stage, detail }
Parquet::column_chunk_stats  $path → @{ {row_group, num_rows, columns:[{column, compression,
                                          encodings, compressed_size, uncompressed_size,
                                          num_values, min, max, null_count}]} }
Parquet::size_report         $path → { total_compressed_size, total_uncompressed_size,
                                       compression_ratio, num_rows, bytes_per_row,
                                       columns:[{column, compressed_size, uncompressed_size,
                                                 compression_ratio}] }   # columns sorted by size desc
Parquet::null_summary        $path → { num_rows, total_nulls,
                                       columns:[{column, null_count, null_fraction}] }   # null_count null = unknown
Parquet::encoding_summary    $path → { columns:[{column, encodings, compression}] }   # footer-only physical-encoding rollup per column
Parquet::bloom_filter_summary $path → { columns:[{column, has_bloom_filter, chunks_with_filter}], columns_with_bloom_filter, columns_total }   # footer-only bloom-filter presence per column
Parquet::sorting_columns_summary $path → { row_groups:[{row_group, sorting_columns:[{column, column_idx, descending, nulls_first}]}], has_sorting_columns }   # footer-only declared sort order
Parquet::sample              $path, %opts → @rows   # opts: offset, n, columns — arbitrary window
Parquet::features            $path → { has_bloom_filter, has_column_index, has_offset_index,
                                       columns:[{column, bloom_filter, column_index, offset_index}] }
```

`validate` reads every row group and reports failure as data (it never
`die`s on a corrupt file — check `ok`). `column_chunk_stats`, `size_report`,
and `features` read only the footer — `size_report` rolls the per-chunk byte
sizes up to file and per-column compression totals; `sample` fills the window
`head`/`tail` can't express.

### Convenience composites

Pure-stryke helpers over `schema` / `count` — no extra file reads beyond
the call they wrap.

```stryke
Parquet::column_names $path → \@names      # schema field names, in file order
Parquet::column_count $path → $n           # number of columns
Parquet::is_empty     $path → 1 | 0        # count == 0 (schema may still exist)
```

### `inspect` shape

```json
{
  "path": "events.parquet",
  "file_size": 12483921,
  "num_rows": 250000,
  "num_row_groups": 4,
  "num_columns": 7,
  "total_compressed_size": 11_001_234,
  "total_uncompressed_size": 18_993_211,
  "compression_ratio": 0.579,
  "compressions": { "zstd(zstdlevel(3))": 28 },
  "created_by": "parquet-rs version 58.3.0",
  "version": 1
}
```

### `stats` shape (one NDJSON line per column)

```json
{
  "name": "score",
  "type": "float64",
  "nullable": true,
  "null_count": 1247,
  "distinct_count": null,
  "min": -3.14,
  "max": 99.5
}
```

Min/max/null_count come from the footer per row-group statistics
aggregated across the whole file. `distinct_count` only fills in when the
writer wrote it (most parquet writers don't).

### `rowgroups` shape (one NDJSON line per RG)

```json
{
  "ordinal": 0,
  "num_rows": 65536,
  "total_byte_size": 4_932_111,
  "total_compressed_size": 2_812_009,
  "columns": [
    {
      "column": "user_id",
      "type": "int64",
      "compression": "zstd(zstdlevel(3))",
      "encodings": ["plain", "rle", "rle_dictionary"],
      "num_values": 65536,
      "compressed_size": 421_882,
      "uncompressed_size": 524_288,
      "data_page_offset": 4,
      "dictionary_page_offset": 0,
      "has_index_page": true
    }
    /* … one per column */
  ]
}
```

## [0x06] Supported compression codecs

| Codec | Library | Notes |
|---|---|---|
| `snappy` | `snap` | parquet-rs default; fast, modest ratio |
| `zstd` | `zstd` | best ratio per CPU |
| `gzip` | `flate2` | broad compatibility |
| `lz4` | `lz4_flex` | LZ4_RAW frame |
| `brotli` | `brotli` | high ratio, slow |
| `uncompressed` | — | fastest write, biggest file |

## [0x07] Tests

```sh
cargo test                                # compiles, no live calls
s test t/                                 # self-contained round-trip
```

The suite uses `parquet mkdemo` to generate a fixture parquet in `/tmp/`
then exercises every diagnostic command against it. No external services
required.

## [0x08] Dev workflow

```sh
make             # release build
make test
make install
make clean
```

## [0x09] Layout

```
stryke-parquet/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # cdylib crate manifest
  Makefile
  src/lib.rs                       # cdylib — parquet__* extern "C" exports
  lib/
    Parquet.stk                    # `use Parquet` — thin wrapper around the FFI symbols
  t/
    test_parquet.stk
    test_stryke_parquet_surface.stk
  examples/
    diagnostics.stk
    discover.stk
    inspect.stk
    head_stats.stk
    recompress.stk
    stats.stk
  .github/workflows/
    ci.yml                         # mkdemo + diagnostic ops
    release.yml                    # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
