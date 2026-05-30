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

### `[PARQUET FILE INSPECTOR FOR STRYKE // SCHEMA + STATS + ROW-GROUPS + HEAD/TAIL + RECOMPRESS]`

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
- [\[0x03\] CLI: `parquet`](#0x03-cli-parquet)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Helper protocol](#0x05-helper-protocol)
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

```sh
cd ~/projects/stryke-parquet
cargo build --release            # produces target/release/stryke-parquet-helper
s pkg install -g .               # installs `parquet` and `parquet-build` CLIs
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
my @rgs = Parquet::rowgroups "events.parquet"
for my $rg (@rgs) {
    p "rg $rg->{ordinal}: $rg->{num_rows} rows, $rg->{total_compressed_size} bytes"
}

# Per-column stats (aggregated across row groups).
Parquet::stats("events.parquet") |> ep
Parquet::stats "events.parquet", column => "user_id"

# Peek at rows.
my @first = Parquet::head "events.parquet", n => 20
my @last  = Parquet::tail "events.parquet", n => 5
my @cols  = Parquet::head "events.parquet", n => 10, columns => ["user_id", "ts"]

# Stream every row (no full-result buffering — for big files).
Parquet::stream "events.parquet",
    callback => sub ($row) { process $row }

# Convert / recompress.
my $csv = Parquet::to_csv "events.parquet"                       # → scalar
Parquet::to_csv "events.parquet", output => "events.csv"          # → file
Parquet::compress "events.parquet", "events.zst.parquet",
                  codec => "zstd"                                  # recompress
```

## [0x03] CLI: `parquet`

```sh
parquet inspect    events.parquet
parquet schema     events.parquet
parquet count      events.parquet
parquet rowgroups  events.parquet
parquet stats      events.parquet [--column=COL]

parquet head       events.parquet --n=20 [--columns=a,b]
parquet tail       events.parquet --n=5
parquet to-json    events.parquet [--columns=a,b]
parquet to-csv     events.parquet [--output=PATH] [--columns=a,b]

parquet compress   in.parquet out.parquet --codec=zstd [--row-group=N]
parquet mkdemo     /tmp/demo.parquet       # tiny test fixture

parquet build                               # cargo build --release
parquet version
```

## [0x04] API reference

```stryke
Parquet::inspect    $path → \%info
Parquet::schema     $path → { fields, num_fields }
Parquet::count      $path → $n
Parquet::rowgroups  $path → @rgs
Parquet::stats      $path, %opts → @stats         # opts: column
Parquet::head       $path, %opts → @rows          # opts: n, columns
Parquet::tail       $path, %opts → @rows
Parquet::to_json    $path, %opts → @rows
Parquet::stream     $path, %opts → $count         # callback per row
Parquet::to_csv     $path, %opts → $csv | $path   # opts: output, columns
Parquet::compress   $src, $dst, %opts → \%resp    # opts: codec, row_group
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

## [0x05] Helper protocol

```sh
stryke-parquet-helper inspect events.parquet
stryke-parquet-helper head events.parquet --n=5 --columns=user_id,ts
stryke-parquet-helper stats events.parquet --column=score
stryke-parquet-helper compress events.parquet events-zstd.parquet --codec=zstd
stryke-parquet-helper mkdemo /tmp/demo.parquet
```

Output:

* `inspect`, `count`, `schema`, `compress`, `mkdemo` → single JSON object
* `rowgroups`, `stats` → NDJSON lines
* `head`, `tail`, `to-json` → NDJSON rows
* `to-csv` → CSV on stdout (or to `--output=PATH`)

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
make debug
make test
make install
make clean
```

## [0x09] Layout

```
stryke-parquet/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/main.rs                      # single-file helper
  lib/
    Parquet.stk                    # `use Parquet`
  bin/
    parquet.stk                    # `parquet` CLI
    parquet-build.stk
  t/
    test_parquet.stk
  examples/
    inspect.stk
    head_stats.stk
    recompress.stk
  .github/workflows/
    ci.yml                         # mkdemo + diagnostic ops
    release.yml                    # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
