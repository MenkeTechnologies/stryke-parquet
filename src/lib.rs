//! stryke-parquet — Parquet diagnostic + transform cdylib loaded in-process
//! by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn parquet__*` is a JSON-string-in /
//! JSON-string-out wrapper around the `parquet` + `arrow` crates. stryke's
//! FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use Parquet`, registers each one as a stryke-callable function, and on
//! each call passes a JSON-encoded args dict and copies the returned JSON
//! into a stryke string.
//!
//! Stateless package — parquet operations are file transforms; no
//! process-level cache.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_json::writer::{LineDelimited, WriterBuilder as JsonWriterBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde_json::{json, Value};

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

// ── helpers ─────────────────────────────────────────────────────────────────

fn open_parquet_reader(
    path: &Path,
    batch_size: usize,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader> {
    open_parquet_reader_with_columns(path, batch_size, None)
}

/// Open a parquet reader with an optional column projection. When `columns`
/// is `Some(names)`, only those columns are decoded — every other column is
/// skipped at the parquet level (no Arrow filter, no wasted decode cost).
fn open_parquet_reader_with_columns(
    path: &Path,
    batch_size: usize,
    columns: Option<&[String]>,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader> {
    let file =
        File::open(path).with_context(|| format!("opening parquet file `{}`", path.display()))?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(batch_size);
    if let Some(names) = columns {
        let mask = parquet::arrow::ProjectionMask::columns(
            builder.parquet_schema(),
            names.iter().map(|s| s.as_str()),
        );
        builder = builder.with_projection(mask);
    }
    Ok(builder.build()?)
}

fn parse_columns(v: &Value) -> Option<Vec<String>> {
    v.as_array().and_then(|a| {
        let names: Vec<String> = a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect();
        // Pre-fix `columns: []` produced Some(vec![]) which downstream
        // interpreted as "project these columns" and built an all-false mask,
        // dropping every field. Treat empty array as "no projection" so the
        // caller gets the full schema — matches Pandas/Polars behavior on
        // `read_parquet(..., columns=[])`.
        if names.is_empty() {
            None
        } else {
            Some(names)
        }
    })
}

fn open_serialized(path: &Path) -> Result<SerializedFileReader<File>> {
    let file =
        File::open(path).with_context(|| format!("opening parquet file `{}`", path.display()))?;
    Ok(SerializedFileReader::new(file)?)
}

fn compression_for(name: &str) -> Result<Compression> {
    match name.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "lz4" => Ok(Compression::LZ4_RAW),
        "brotli" => Ok(Compression::BROTLI(Default::default())),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        other => bail!("unknown compression `{}`", other),
    }
}

fn stat_minmax(stats: &Statistics) -> (Value, Value) {
    // Helper: render binary byte buffer as UTF-8 string when valid, otherwise
    // as a base64-encoded blob. Parquet stores Utf8 columns as ByteArray;
    // pre-fix these fell through the `_ =>` arm and produced (Null, Null)
    // silently, so op_stats on string columns showed no min/max.
    let bytes_to_value = |b: &[u8]| -> Value {
        match std::str::from_utf8(b) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => {
                // Non-UTF8 bytes — render as hex sentinel so the caller still
                // sees that min/max is non-null and can probe the raw column
                // if needed. Keeps stryke-parquet dep-free of base64.
                let hex: String = b.iter().map(|x| format!("{:02x}", x)).collect();
                Value::String(format!("hex:{hex}"))
            }
        }
    };
    match stats {
        Statistics::Boolean(s) => (
            s.min_opt().map(|v| json!(v)).unwrap_or(Value::Null),
            s.max_opt().map(|v| json!(v)).unwrap_or(Value::Null),
        ),
        Statistics::Int32(s) => (
            s.min_opt().map(|v| json!(v)).unwrap_or(Value::Null),
            s.max_opt().map(|v| json!(v)).unwrap_or(Value::Null),
        ),
        Statistics::Int64(s) => (
            s.min_opt().map(|v| json!(v)).unwrap_or(Value::Null),
            s.max_opt().map(|v| json!(v)).unwrap_or(Value::Null),
        ),
        Statistics::Float(s) => (
            s.min_opt().map(|v| json!(v)).unwrap_or(Value::Null),
            s.max_opt().map(|v| json!(v)).unwrap_or(Value::Null),
        ),
        Statistics::Double(s) => (
            s.min_opt().map(|v| json!(v)).unwrap_or(Value::Null),
            s.max_opt().map(|v| json!(v)).unwrap_or(Value::Null),
        ),
        Statistics::ByteArray(s) => (
            s.min_opt()
                .map(|v| bytes_to_value(v.data()))
                .unwrap_or(Value::Null),
            s.max_opt()
                .map(|v| bytes_to_value(v.data()))
                .unwrap_or(Value::Null),
        ),
        Statistics::FixedLenByteArray(s) => (
            s.min_opt()
                .map(|v| bytes_to_value(v.data()))
                .unwrap_or(Value::Null),
            s.max_opt()
                .map(|v| bytes_to_value(v.data()))
                .unwrap_or(Value::Null),
        ),
        _ => (Value::Null, Value::Null),
    }
}

// ── ops: read-side ──────────────────────────────────────────────────────────

fn op_inspect(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let f = m.file_metadata();
    let num_rows = f.num_rows();
    let num_row_groups = m.num_row_groups();
    let num_columns = f.schema_descr().num_columns();
    let mut total_compressed: i64 = 0;
    let mut compressions: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    for i in 0..num_row_groups {
        let rg = m.row_group(i);
        total_compressed += rg.total_byte_size();
        for j in 0..rg.num_columns() {
            let c = rg.column(j);
            *compressions
                .entry(format!("{:?}", c.compression()))
                .or_insert(0) += 1;
        }
    }
    let dominant_compression = compressions
        .iter()
        .max_by_key(|e| e.1)
        .map(|(k, _)| k.clone());
    Ok(json!({
        "path": path,
        "num_rows": num_rows,
        "num_row_groups": num_row_groups,
        "num_columns": num_columns,
        "total_compressed_bytes": total_compressed,
        "compression": dominant_compression,
        "writer_version": format!("{:?}", f.version()),
    }))
}

fn op_schema(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let descr = r.metadata().file_metadata().schema_descr();
    let fields: Vec<Value> = (0..descr.num_columns())
        .map(|i| {
            let col = descr.column(i);
            json!({
                "name": col.name(),
                "path": col.path().string(),
                "physical_type": format!("{:?}", col.physical_type()),
                "logical_type": col
                    .logical_type_ref()
                    .as_ref()
                    .map(|t| format!("{:?}", t)),
                "repetition": format!("{:?}", col.self_type().get_basic_info().repetition()),
            })
        })
        .collect();
    Ok(json!({"num_fields": fields.len(), "fields": fields}))
}

fn op_count(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    Ok(json!({"num_rows": r.metadata().file_metadata().num_rows()}))
}

fn op_rowgroups(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let groups: Vec<Value> = (0..m.num_row_groups())
        .map(|i| {
            let rg = m.row_group(i);
            json!({
                "index": i,
                "num_rows": rg.num_rows(),
                "total_byte_size": rg.total_byte_size(),
                "num_columns": rg.num_columns(),
            })
        })
        .collect();
    Ok(json!({"row_groups": groups}))
}

/// Footer-only rollup of row-group SIZING: the total row/compressed-byte counts
/// plus per-row-group `min`/`max`/`mean` of both. Reads only the footer — no
/// column data is decoded. Surfaces uneven row groups (a wide max-vs-min spread
/// hurts parallel scan), which `rowgroups` (raw per-group list) and `size_report`
/// (per-column bytes) do not aggregate. Compressed bytes are summed across each
/// group's column chunks. opts: path (required). Pure.
fn op_row_group_summary(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let n = m.num_row_groups();
    if n == 0 {
        return Ok(json!({
            "num_row_groups": 0,
            "total_rows": 0,
            "total_compressed_size": 0,
            "rows_per_group": Value::Null,
            "compressed_bytes_per_group": Value::Null,
        }));
    }
    let mut row_counts: Vec<i64> = Vec::with_capacity(n);
    let mut comp_sizes: Vec<i64> = Vec::with_capacity(n);
    for j in 0..n {
        let rg = m.row_group(j);
        row_counts.push(rg.num_rows());
        comp_sizes.push(
            (0..rg.num_columns())
                .map(|i| rg.column(i).compressed_size())
                .sum(),
        );
    }
    let stat = |v: &[i64]| -> Value {
        json!({
            "min": *v.iter().min().unwrap(),
            "max": *v.iter().max().unwrap(),
            "mean": v.iter().sum::<i64>() as f64 / v.len() as f64,
        })
    };
    Ok(json!({
        "num_row_groups": n,
        "total_rows": row_counts.iter().sum::<i64>(),
        "total_compressed_size": comp_sizes.iter().sum::<i64>(),
        "rows_per_group": stat(&row_counts),
        "compressed_bytes_per_group": stat(&comp_sizes),
    }))
}

fn op_stats(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    let mut cols: Vec<Value> = Vec::new();
    for i in 0..descr.num_columns() {
        let col_descr = descr.column(i);
        let name = col_descr.path().string();
        let mut null_count: u64 = 0;
        let mut min: Value = Value::Null;
        let mut max: Value = Value::Null;
        for j in 0..m.num_row_groups() {
            let col = m.row_group(j).column(i);
            if let Some(s) = col.statistics() {
                null_count += s.null_count_opt().unwrap_or(0);
                let (mn, mx) = stat_minmax(s);
                // Pre-fix: `min` was set only on the FIRST non-Null row group
                // and never folded across subsequent ones — for a file whose
                // first row group's min is 50 and a later row group's min is
                // 1, op_stats reported min=50. Now fold correctly: take the
                // smaller of `min` and `mn` when both are non-Null.
                min = match (&min, &mn) {
                    (Value::Null, _) => mn,
                    (_, Value::Null) => min,
                    _ => {
                        if cmp_lt(&mn, &min) {
                            mn
                        } else {
                            min
                        }
                    }
                };
                if max == Value::Null || cmp_max(&max, &mx) {
                    max = mx;
                }
            }
        }
        cols.push(json!({
            "name": name,
            "null_count": null_count,
            "min": min,
            "max": max,
        }));
    }
    Ok(json!({"columns": cols}))
}

fn cmp_max(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => y > x,
        _ => false,
    }
}

/// Numeric "is a less than b" — like cmp_max but for the min-fold.
fn cmp_lt(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x < y,
        _ => false,
    }
}

// ── ops: row read / convert ─────────────────────────────────────────────────

fn op_head(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let n = args["n"].as_u64().unwrap_or(10) as usize;
    let cols = parse_columns(&args["columns"]);
    let reader = open_parquet_reader_with_columns(Path::new(path), 8192, cols.as_deref())?;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        let mut emitted = 0;
        for batch in reader {
            let mut batch = batch?;
            if batch.num_rows() + emitted > n {
                batch = batch.slice(0, n - emitted);
            }
            w.write(&batch)?;
            emitted += batch.num_rows();
            if emitted >= n {
                break;
            }
        }
        w.finish()?;
    }
    let rows = ndjson_to_rows(&buf)?;
    Ok(json!({"rows": rows}))
}

fn op_tail(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let n = args["n"].as_u64().unwrap_or(10) as usize;
    let cols = parse_columns(&args["columns"]);
    // Pre-fix this hard-coded `with_row_groups(vec![num_groups - 1])` so a
    // tail(n) where n exceeded the last row group's row count silently
    // returned fewer than n rows. Now: walk backward from the last row
    // group, accumulating groups until their cumulative row count meets n.
    // We still skip the prefix of earlier rows during the final emit.
    let file = File::open(path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(8192);
    let md = builder.metadata().clone();
    let num_groups = md.num_row_groups();
    if num_groups > 0 {
        let mut rg_indices: Vec<usize> = Vec::new();
        let mut acc: i64 = 0;
        for j in (0..num_groups).rev() {
            rg_indices.push(j);
            acc += md.row_group(j).num_rows();
            if acc as usize >= n {
                break;
            }
        }
        rg_indices.reverse();
        builder = builder.with_row_groups(rg_indices);
    }
    if let Some(names) = &cols {
        let mask = parquet::arrow::ProjectionMask::columns(
            builder.parquet_schema(),
            names.iter().map(|s| s.as_str()),
        );
        builder = builder.with_projection(mask);
    }
    let reader = builder.build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for b in reader {
        batches.push(b?);
    }
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let take = n.min(total);
    let skip = total - take;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        let mut skipped = 0;
        let mut emitted = 0;
        for batch in batches {
            let n_rows = batch.num_rows();
            let mut batch = batch;
            if skipped < skip {
                let to_skip = (skip - skipped).min(n_rows);
                skipped += to_skip;
                if to_skip == n_rows {
                    continue;
                }
                batch = batch.slice(to_skip, n_rows - to_skip);
            }
            let remaining = take - emitted;
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
            }
            w.write(&batch)?;
            emitted += batch.num_rows();
            if emitted >= take {
                break;
            }
        }
        w.finish()?;
    }
    Ok(json!({"rows": ndjson_to_rows(&buf)?}))
}

/// Return every row in reverse file order — the whole-file companion to
/// `head`/`tail` (e.g. newest-first when the file is append-ordered). Reads all
/// row groups, then reverses the row sequence. Supports the same `columns`
/// projection as `head`. Returns `{rows}`.
fn op_reverse(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let cols = parse_columns(&args["columns"]);
    let file = File::open(path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(8192);
    if let Some(names) = &cols {
        let mask = parquet::arrow::ProjectionMask::columns(
            builder.parquet_schema(),
            names.iter().map(|s| s.as_str()),
        );
        builder = builder.with_projection(mask);
    }
    let reader = builder.build()?;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        for b in reader {
            w.write(&b?)?;
        }
        w.finish()?;
    }
    let mut rows = ndjson_to_rows(&buf)?;
    rows.reverse();
    Ok(json!({ "rows": rows }))
}

/// Select rows by an explicit list of 0-based indices — polars `gather` / pandas
/// `.iloc[[…]]`. Unlike `slice` (a contiguous window), `head`/`tail` (the ends)
/// or `sample` (random), the index list is arbitrary: it may repeat a row and
/// emits rows in exactly the order given. Each index is bounds-checked against
/// the row count (out-of-range dies). Supports the same `columns` projection.
/// Returns `{rows}`.
fn op_gather(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let indices = args["indices"]
        .as_array()
        .ok_or_else(|| anyhow!("missing indices (array of 0-based row numbers)"))?;
    let cols = parse_columns(&args["columns"]);
    let reader = open_parquet_reader_with_columns(Path::new(path), 8192, cols.as_deref())?;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        for batch in reader {
            w.write(&batch?)?;
        }
        w.finish()?;
    }
    let all = ndjson_to_rows(&buf)?;
    let n = all.len();
    let mut out = Vec::with_capacity(indices.len());
    for v in indices {
        let i = v
            .as_u64()
            .ok_or_else(|| anyhow!("gather: each index must be a non-negative integer"))?
            as usize;
        if i >= n {
            return Err(anyhow!("gather: index {i} out of range (rows: {n})"));
        }
        out.push(all[i].clone());
    }
    Ok(json!({ "rows": out }))
}

/// Frequency of each distinct value in a `column` — pandas/polars `value_counts`,
/// mirroring `Arrow::value_counts`. Projects just that column, tallies each
/// value (nulls form their own group), and returns one `{value, count}` row per
/// distinct value sorted by count descending then value ascending. opts: `path`,
/// `column`. Returns `{rows: [{value, count}], distinct}`.
fn op_value_counts(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let column = args["column"]
        .as_str()
        .ok_or_else(|| anyhow!("missing column"))?;
    let projection = vec![column.to_string()];
    let reader = open_parquet_reader_with_columns(Path::new(path), 8192, Some(&projection))?;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        for batch in reader {
            w.write(&batch?)?;
        }
        w.finish()?;
    }
    let all = ndjson_to_rows(&buf)?;
    let mut order: Vec<String> = Vec::new();
    let mut counts: std::collections::HashMap<String, (Value, u64)> =
        std::collections::HashMap::new();
    for row in &all {
        let val = row.get(column).cloned().unwrap_or(Value::Null);
        let key = val.to_string();
        match counts.get_mut(&key) {
            Some(entry) => entry.1 += 1,
            None => {
                order.push(key.clone());
                counts.insert(key, (val, 1));
            }
        }
    }
    let distinct = order.len();
    let mut pairs: Vec<(Value, u64)> = order.iter().map(|k| counts[k].clone()).collect();
    // Count descending, then value's JSON representation ascending — deterministic.
    pairs.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.to_string().cmp(&b.0.to_string()))
    });
    let rows: Vec<Value> = pairs
        .into_iter()
        .map(|(v, c)| json!({ "value": v, "count": c }))
        .collect();
    Ok(json!({ "rows": rows, "distinct": distinct }))
}

/// Return an arbitrary row window — `length` rows starting at `offset` (0-based)
/// — the offset-aware companion to `head`/`tail`. `length` (or `n`) is optional;
/// when omitted the window runs to the end of the file. An `offset` past the end
/// yields no rows. Supports the same `columns` projection as `head`. Returns
/// `{rows}`.
fn op_slice(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let offset = args["offset"].as_u64().unwrap_or(0) as usize;
    let length = args["length"]
        .as_u64()
        .or_else(|| args["n"].as_u64())
        .map(|v| v as usize);
    let cols = parse_columns(&args["columns"]);
    let reader = open_parquet_reader_with_columns(Path::new(path), 8192, cols.as_deref())?;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        let mut scanned = 0usize; // original rows consumed before the current batch
        let mut emitted = 0usize;
        for batch in reader {
            let batch = batch?;
            let n_rows = batch.num_rows();
            let batch_start = scanned;
            scanned += n_rows;
            // Whole batch lies before the window start.
            if batch_start + n_rows <= offset {
                continue;
            }
            // Trim the prefix that falls before `offset`.
            let local_off = offset.saturating_sub(batch_start);
            let mut b = if local_off > 0 {
                batch.slice(local_off, n_rows - local_off)
            } else {
                batch
            };
            if let Some(len) = length {
                if emitted >= len {
                    break;
                }
                let remaining = len - emitted;
                if b.num_rows() > remaining {
                    b = b.slice(0, remaining);
                }
            }
            w.write(&b)?;
            emitted += b.num_rows();
            if length.is_some_and(|len| emitted >= len) {
                break;
            }
        }
        w.finish()?;
    }
    Ok(json!({"rows": ndjson_to_rows(&buf)?}))
}

fn op_to_json(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let limit = args["limit"].as_u64().map(|n| n as usize);
    let reader = open_parquet_reader(Path::new(path), 8192)?;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        let mut emitted = 0;
        for batch in reader {
            let mut batch = batch?;
            if let Some(l) = limit {
                let remaining = l.saturating_sub(emitted);
                if remaining == 0 {
                    break;
                }
                if batch.num_rows() > remaining {
                    batch = batch.slice(0, remaining);
                }
            }
            w.write(&batch)?;
            emitted += batch.num_rows();
            if limit.is_some_and(|l| emitted >= l) {
                break;
            }
        }
        w.finish()?;
    }
    Ok(json!({"rows": ndjson_to_rows(&buf)?}))
}

fn op_to_csv(args: Value) -> Result<Value> {
    let src = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let with_header = args["header"].as_bool().unwrap_or(true);
    let reader = open_parquet_reader(Path::new(src), 8192)?;
    let file = File::create(dst)?;
    let mut w = CsvWriterBuilder::new()
        .with_header(with_header)
        .build(BufWriter::new(file));
    let mut rows = 0;
    for batch in reader {
        let b = batch?;
        rows += b.num_rows();
        w.write(&b)?;
    }
    Ok(json!({"path": dst, "rows": rows}))
}

fn op_compress(args: Value) -> Result<Value> {
    let src = args["src"].as_str().ok_or_else(|| anyhow!("missing src"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    let row_group = args["row_group"].as_u64().unwrap_or(65536) as usize;
    let reader = open_parquet_reader(Path::new(src), 8192)?;
    let schema: SchemaRef = reader.schema();
    let props = WriterProperties::builder()
        .set_compression(compression_for(&compression)?)
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
    let mut rows = 0;
    for batch in reader {
        let b = batch?;
        rows += b.num_rows();
        w.write(&b)?;
    }
    w.close()?;
    Ok(json!({
        "dst": dst,
        "rows": rows,
        "num_rows": rows,
        "compression": compression,
    }))
}

fn op_from_csv(args: Value) -> Result<Value> {
    use arrow_csv::reader::{Format, ReaderBuilder as CsvReaderBuilder};
    let src = args["src"].as_str().ok_or_else(|| anyhow!("missing src"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let header = args["header"].as_bool().unwrap_or(true);
    let delimiter = args["delimiter"]
        .as_str()
        .and_then(|s| s.bytes().next())
        .unwrap_or(b',');
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    let format = Format::default()
        .with_header(header)
        .with_delimiter(delimiter);
    // First pass infers the schema; second pass (fresh handle) reads the data.
    let (schema, _) = format.infer_schema(File::open(src)?, Some(1024))?;
    let schema: SchemaRef = Arc::new(schema);
    let csv = CsvReaderBuilder::new(Arc::clone(&schema))
        .with_format(format)
        .build(File::open(src)?)?;
    let props = WriterProperties::builder()
        .set_compression(compression_for(&compression)?)
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
    let mut rows = 0;
    for batch in csv {
        let b = batch?;
        rows += b.num_rows();
        w.write(&b)?;
    }
    w.close()?;
    Ok(json!({"dst": dst, "rows": rows, "compression": compression}))
}

fn op_from_json(args: Value) -> Result<Value> {
    use arrow_json::reader::{infer_json_schema_from_seekable, ReaderBuilder as JsonReaderBuilder};
    use std::io::BufReader;
    let src = args["src"].as_str().ok_or_else(|| anyhow!("missing src"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    // NDJSON (one object per line). Infer schema, then re-open for the data pass.
    let (schema, _) =
        infer_json_schema_from_seekable(BufReader::new(File::open(src)?), Some(1024))?;
    let schema: SchemaRef = Arc::new(schema);
    let json =
        JsonReaderBuilder::new(Arc::clone(&schema)).build(BufReader::new(File::open(src)?))?;
    let props = WriterProperties::builder()
        .set_compression(compression_for(&compression)?)
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
    let mut rows = 0;
    for batch in json {
        let b = batch?;
        rows += b.num_rows();
        w.write(&b)?;
    }
    w.close()?;
    Ok(json!({"dst": dst, "rows": rows, "compression": compression}))
}

fn op_merge(args: Value) -> Result<Value> {
    let srcs = args["srcs"]
        .as_array()
        .ok_or_else(|| anyhow!("missing srcs (array of parquet paths)"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    if srcs.is_empty() {
        return Err(anyhow!("srcs must be non-empty"));
    }
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    // All inputs must share the first file's schema (ArrowWriter rejects mismatches).
    let first = srcs[0]
        .as_str()
        .ok_or_else(|| anyhow!("srcs must be strings"))?;
    let r0 = open_parquet_reader(Path::new(first), 8192)?;
    let schema: SchemaRef = r0.schema();
    let props = WriterProperties::builder()
        .set_compression(compression_for(&compression)?)
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
    let mut rows = 0;
    for batch in r0 {
        let b = batch?;
        rows += b.num_rows();
        w.write(&b)?;
    }
    for s in &srcs[1..] {
        let p = s.as_str().ok_or_else(|| anyhow!("srcs must be strings"))?;
        let r = open_parquet_reader(Path::new(p), 8192)?;
        for batch in r {
            let b = batch?;
            rows += b.num_rows();
            w.write(&b)?;
        }
    }
    w.close()?;
    Ok(json!({"dst": dst, "files": srcs.len(), "rows": rows, "compression": compression}))
}

/// Horizontally stack a second parquet file's columns onto `path` — the
/// column-wise counterpart of `merge` (which appends rows). Both files must have
/// the same row count; the output is `path`'s columns followed by `other`'s, and a
/// column-name collision is rejected. opts: `path` (or `src`), `other` (required),
/// `dst`, optional compression (default zstd). Returns
/// `{dst, rows, columns, compression}`.
fn op_hstack(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .or_else(|| args["src"].as_str())
        .ok_or_else(|| anyhow!("missing path"))?;
    let other = args["other"]
        .as_str()
        .ok_or_else(|| anyhow!("missing other (the second parquet)"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    let lreader = open_parquet_reader(Path::new(path), 8192)?;
    let lschema = lreader.schema();
    let lbatches: Vec<RecordBatch> = lreader.collect::<std::result::Result<_, _>>()?;
    let left = arrow::compute::concat_batches(&lschema, &lbatches)?;
    let rreader = open_parquet_reader(Path::new(other), 8192)?;
    let rschema = rreader.schema();
    let rbatches: Vec<RecordBatch> = rreader.collect::<std::result::Result<_, _>>()?;
    let right = arrow::compute::concat_batches(&rschema, &rbatches)?;
    if left.num_rows() != right.num_rows() {
        return Err(anyhow!(
            "hstack: row counts differ ({} vs {})",
            left.num_rows(),
            right.num_rows()
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut fields: Vec<Arc<arrow::datatypes::Field>> = Vec::new();
    for f in lschema.fields().iter().chain(rschema.fields().iter()) {
        if !seen.insert(f.name().clone()) {
            return Err(anyhow!("hstack: duplicate column name `{}`", f.name()));
        }
        fields.push(f.clone());
    }
    let out_schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(fields));
    let mut cols = left.columns().to_vec();
    cols.extend(right.columns().iter().cloned());
    let out = RecordBatch::try_new(Arc::clone(&out_schema), cols)?;
    let kept: Vec<String> = out_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let props = WriterProperties::builder()
        .set_compression(compression_for(&compression)?)
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, Arc::clone(&out_schema), Some(props))?;
    w.write(&out)?;
    w.close()?;
    Ok(json!({"dst": dst, "rows": out.num_rows(), "columns": kept, "compression": compression}))
}

/// Project a subset of columns into a new parquet file (column pruning) — the
/// write-to-file companion to `head`/`tail`'s preview projection. `columns` is a
/// non-empty array of names; every name must exist (a `ProjectionMask` silently
/// drops unknown names, which would quietly write a file missing a column, so we
/// validate up front). Output keeps the file's column order, not the requested
/// order, and the row count is preserved. opts: path, dst, columns, optional
/// compression (default zstd). Pure transform.
/// Read `path`, keep only `keep` columns (projected at the parquet level), and
/// write the result to `dst` with `compression`. Shared by `op_select` (keep
/// the named columns) and `op_drop` (keep all but the named columns). The
/// reader emits columns in file-schema order regardless of `keep`'s order.
fn write_projection(path: &str, dst: &str, keep: &[String], compression: &str) -> Result<Value> {
    let reader = open_parquet_reader_with_columns(Path::new(path), 8192, Some(keep))?;
    let schema = reader.schema();
    let kept: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let props = WriterProperties::builder()
        .set_compression(compression_for(compression)?)
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
    let mut rows = 0;
    for batch in reader {
        let b = batch?;
        rows += b.num_rows();
        w.write(&b)?;
    }
    w.close()?;
    Ok(json!({"dst": dst, "rows": rows, "columns": kept, "compression": compression}))
}

/// Column names of a parquet file in schema order.
fn column_names_of(path: &str) -> Result<Vec<String>> {
    let probe = open_parquet_reader(Path::new(path), 1)?;
    Ok(probe
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect())
}

fn op_select(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let cols = parse_columns(&args["columns"])
        .ok_or_else(|| anyhow!("missing columns (a non-empty array of names)"))?;
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    // Validate every requested column exists; ProjectionMask ignores unknowns.
    let have: std::collections::HashSet<String> = column_names_of(path)?.into_iter().collect();
    for c in &cols {
        if !have.contains(c) {
            bail!("select: no column `{c}` in `{path}`");
        }
    }
    write_projection(path, dst, &cols, &compression)
}

fn op_drop(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let drop = parse_columns(&args["columns"])
        .ok_or_else(|| anyhow!("missing columns (a non-empty array of names to drop)"))?;
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    let all = column_names_of(path)?;
    let drop_set: std::collections::HashSet<&String> = drop.iter().collect();
    // Validate every dropped column exists so a typo fails loud, not silent.
    for c in &drop {
        if !all.contains(c) {
            bail!("drop: no column `{c}` in `{path}`");
        }
    }
    let keep: Vec<String> = all.into_iter().filter(|c| !drop_set.contains(c)).collect();
    if keep.is_empty() {
        bail!("drop: refusing to drop every column of `{path}`");
    }
    write_projection(path, dst, &keep, &compression)
}

/// Rename columns in a parquet file — the relabeling companion to `select` (keep)
/// and `drop` (remove). `rename` is an object `{old: new, …}`; every key must be
/// an existing column (a typo fails loud). Types, nullability, column order and
/// row count are all preserved — only the schema field names change, so the data
/// pages are re-written unchanged under the new names. The resulting names must be
/// unique (a rename that collides with another column is rejected). opts: path,
/// dst, rename, optional compression (default zstd). Returns
/// `{dst, rows, columns, compression}`.
fn op_rename(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let map = args["rename"]
        .as_object()
        .ok_or_else(|| anyhow!("missing rename (an object {{old: new}})"))?;
    if map.is_empty() {
        bail!("rename: empty rename map");
    }
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    let all = column_names_of(path)?;
    let have: std::collections::HashSet<&String> = all.iter().collect();
    let mut renames: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for (old, new) in map {
        if !have.contains(old) {
            bail!("rename: no column `{old}` in `{path}`");
        }
        let new = new
            .as_str()
            .ok_or_else(|| anyhow!("rename: new name for `{old}` must be a string"))?;
        renames.insert(old.clone(), new.to_string());
    }
    // The output names must stay unique (a rename can't collide with another col).
    let out_names: Vec<String> = all
        .iter()
        .map(|n| renames.get(n).cloned().unwrap_or_else(|| n.clone()))
        .collect();
    let mut seen = std::collections::HashSet::new();
    for n in &out_names {
        if !seen.insert(n) {
            bail!("rename: duplicate column name `{n}` in the result");
        }
    }
    let reader = open_parquet_reader(Path::new(path), 8192)?;
    let in_schema = reader.schema();
    let fields: Vec<Arc<arrow::datatypes::Field>> = in_schema
        .fields()
        .iter()
        .map(|f| match renames.get(f.name()) {
            Some(nn) => Arc::new(f.as_ref().clone().with_name(nn)),
            None => f.clone(),
        })
        .collect();
    let out_schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(fields));
    let props = WriterProperties::builder()
        .set_compression(compression_for(&compression)?)
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, Arc::clone(&out_schema), Some(props))?;
    let mut rows = 0;
    for batch in reader {
        let b = batch?;
        rows += b.num_rows();
        // Same column arrays, re-labeled under the renamed schema.
        let rb = RecordBatch::try_new(Arc::clone(&out_schema), b.columns().to_vec())?;
        w.write(&rb)?;
    }
    w.close()?;
    Ok(json!({"dst": dst, "rows": rows, "columns": out_names, "compression": compression}))
}

fn op_write(args: Value) -> Result<Value> {
    use arrow_json::reader::{infer_json_schema_from_seekable, ReaderBuilder as JsonReaderBuilder};
    use std::io::Cursor;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let rows = args["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (an array of objects)"))?;
    if rows.is_empty() {
        return Err(anyhow!("rows must be non-empty"));
    }
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    // Serialize the in-memory rows to NDJSON once, then drive the same
    // arrow-json schema-inference + read path `from_json` uses on a file.
    let mut nd = Vec::<u8>::new();
    for r in rows {
        serde_json::to_writer(&mut nd, r)?;
        nd.push(b'\n');
    }
    let (schema, _) = infer_json_schema_from_seekable(Cursor::new(nd.as_slice()), None)?;
    let schema: SchemaRef = Arc::new(schema);
    let reader = JsonReaderBuilder::new(Arc::clone(&schema)).build(Cursor::new(nd.as_slice()))?;
    let props = WriterProperties::builder()
        .set_compression(compression_for(&compression)?)
        .build();
    let file = File::create(dst)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
    let mut written = 0;
    for batch in reader {
        let b = batch?;
        written += b.num_rows();
        w.write(&b)?;
    }
    w.close()?;
    Ok(json!({"dst": dst, "rows": written, "compression": compression}))
}

fn op_write_partitioned(args: Value) -> Result<Value> {
    use arrow_json::reader::{infer_json_schema_from_seekable, ReaderBuilder as JsonReaderBuilder};
    use std::collections::BTreeMap;
    use std::io::Cursor;
    let dst = args["dst"]
        .as_str()
        .ok_or_else(|| anyhow!("missing dst (base dir)"))?;
    let rows = args["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (an array of objects)"))?;
    if rows.is_empty() {
        return Err(anyhow!("rows must be non-empty"));
    }
    let part_col = args["partition_by"]
        .as_str()
        .ok_or_else(|| anyhow!("missing partition_by (column name)"))?;
    let compression = args["compression"].as_str().unwrap_or("zstd").to_string();
    // One schema for every partition (inferred from all rows) so the dataset is
    // self-consistent. The partition column is retained in each file.
    let mut all_nd = Vec::<u8>::new();
    for r in rows {
        serde_json::to_writer(&mut all_nd, r)?;
        all_nd.push(b'\n');
    }
    let (schema, _) = infer_json_schema_from_seekable(Cursor::new(all_nd.as_slice()), None)?;
    let schema: SchemaRef = Arc::new(schema);
    // Bucket rows by the partition column's (stringified) value, deterministically.
    let mut buckets: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for r in rows {
        let v = r
            .get(part_col)
            .ok_or_else(|| anyhow!("row missing partition column `{}`", part_col))?;
        let key = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let buf = buckets.entry(key).or_default();
        serde_json::to_writer(&mut *buf, r)?;
        buf.push(b'\n');
    }
    let mut parts = Vec::new();
    for (value, nd) in &buckets {
        // Hive-style `col=value/` directory. `/` in a value would break the
        // path, so it's replaced with `_`.
        let dir = format!("{}/{}={}", dst, part_col, value.replace('/', "_"));
        std::fs::create_dir_all(&dir)?;
        let path = format!("{}/part-0.parquet", dir);
        let props = WriterProperties::builder()
            .set_compression(compression_for(&compression)?)
            .build();
        let reader =
            JsonReaderBuilder::new(Arc::clone(&schema)).build(Cursor::new(nd.as_slice()))?;
        let file = File::create(&path)?;
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;
        let mut written = 0;
        for batch in reader {
            let b = batch?;
            written += b.num_rows();
            w.write(&b)?;
        }
        w.close()?;
        parts.push(json!({"value": value, "path": path, "rows": written}));
    }
    Ok(json!({
        "dst": dst,
        "partition_by": part_col,
        "partitions": parts,
        "total_rows": rows.len(),
    }))
}

fn op_metadata(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let fm = r.metadata().file_metadata();
    // Key-value file metadata (writer-stamped). Distinct from column stats.
    let kv: serde_json::Map<String, Value> = match fm.key_value_metadata() {
        Some(pairs) => pairs
            .iter()
            .map(|p| (p.key.clone(), json!(p.value)))
            .collect(),
        None => serde_json::Map::new(),
    };
    Ok(json!({
        "path": path,
        "metadata": kv,
        "created_by": fm.created_by(),
        "version": fm.version(),
    }))
}

fn op_mkdemo(args: Value) -> Result<Value> {
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]));
    let ids: Int64Array = (1..=5).collect();
    let names = StringArray::from(vec!["alice", "bob", "carol", "dave", "eve"]);
    let scores = Float64Array::from(vec![Some(1.5), Some(2.0), None, Some(3.25), Some(4.0)]);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
    )?;
    let file = File::create(path)?;
    let mut w = ArrowWriter::try_new(file, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    Ok(json!({"path": path, "rows": batch.num_rows()}))
}

// ── shared ──────────────────────────────────────────────────────────────────

fn ndjson_to_rows(buf: &[u8]) -> Result<Vec<Value>> {
    let s = std::str::from_utf8(buf)?;
    s.lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Value>(l).map_err(Into::into))
        .collect()
}

// ── ops: integrity / footer detail / sampling ───────────────────────────────

/// Full read: decode every row group and report row count, or the first
/// decode error and the stage it surfaced at. Footer-corrupt files fail at
/// `footer`; page/data corruption surfaces at `scan`.
fn op_validate(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let p = Path::new(path);
    let r = match open_serialized(p) {
        Ok(r) => r,
        Err(e) => return Ok(json!({"ok": false, "stage": "footer", "detail": e.to_string()})),
    };
    let num_rgs = r.metadata().num_row_groups();
    let reader = match open_parquet_reader_with_columns(p, 8192, None) {
        Ok(rd) => rd,
        Err(e) => return Ok(json!({"ok": false, "stage": "reader", "detail": e.to_string()})),
    };
    let mut rows = 0usize;
    for batch in reader {
        match batch {
            Ok(b) => rows += b.num_rows(),
            Err(e) => {
                return Ok(json!({
                    "ok": false, "stage": "scan", "rows_read": rows, "detail": e.to_string()
                }))
            }
        }
    }
    Ok(json!({"ok": true, "rows": rows, "row_groups": num_rgs}))
}

/// Per-row-group, per-column footer detail — compression, encodings, on-disk
/// sizes and column statistics — without scanning page data. `op_stats` folds
/// min/max across row groups; this exposes each chunk individually.
fn op_column_chunk_stats(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    let mut groups: Vec<Value> = Vec::new();
    for j in 0..m.num_row_groups() {
        let rg = m.row_group(j);
        let mut chunks: Vec<Value> = Vec::new();
        for i in 0..rg.num_columns() {
            let col = rg.column(i);
            let name = descr.column(i).path().string();
            let (min, max, null_count) = match col.statistics() {
                Some(s) => {
                    let (mn, mx) = stat_minmax(s);
                    (
                        mn,
                        mx,
                        s.null_count_opt().map(|v| json!(v)).unwrap_or(Value::Null),
                    )
                }
                None => (Value::Null, Value::Null, Value::Null),
            };
            let encodings: Vec<String> = col.encodings().map(|e| format!("{e:?}")).collect();
            chunks.push(json!({
                "column": name,
                "compression": format!("{:?}", col.compression()),
                "encodings": encodings,
                "compressed_size": col.compressed_size(),
                "uncompressed_size": col.uncompressed_size(),
                "num_values": col.num_values(),
                "min": min,
                "max": max,
                "null_count": null_count,
            }));
        }
        groups.push(json!({"row_group": j, "num_rows": rg.num_rows(), "columns": chunks}));
    }
    Ok(json!({"row_groups": groups}))
}

/// Aggregate the footer's per-column-chunk byte sizes into a file-level
/// compression report: total compressed/uncompressed bytes, overall ratio,
/// bytes-per-row, and a per-column rollup (summed across every row group)
/// sorted by compressed size descending. Reads only the footer — no column
/// data is decoded. Complements `column_chunk_stats`, which stays per-group.
fn op_size_report(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    // Per-column running totals, keyed by column path and kept in first-seen
    // order so the rollup is deterministic before the size sort.
    let mut order: Vec<String> = Vec::new();
    let mut comp: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut uncomp: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut total_comp: i64 = 0;
    let mut total_uncomp: i64 = 0;
    let mut num_rows: i64 = 0;
    for j in 0..m.num_row_groups() {
        let rg = m.row_group(j);
        num_rows += rg.num_rows();
        for i in 0..rg.num_columns() {
            let col = rg.column(i);
            let name = descr.column(i).path().string();
            if !comp.contains_key(&name) {
                order.push(name.clone());
            }
            *comp.entry(name.clone()).or_insert(0) += col.compressed_size();
            *uncomp.entry(name).or_insert(0) += col.uncompressed_size();
            total_comp += col.compressed_size();
            total_uncomp += col.uncompressed_size();
        }
    }
    let ratio = |u: i64, c: i64| -> Value {
        if c > 0 {
            json!(u as f64 / c as f64)
        } else {
            Value::Null
        }
    };
    let mut columns: Vec<(String, i64, i64)> = order
        .into_iter()
        .map(|n| {
            let c = comp[&n];
            let u = uncomp[&n];
            (n, c, u)
        })
        .collect();
    columns.sort_by_key(|c| std::cmp::Reverse(c.1));
    let columns: Vec<Value> = columns
        .into_iter()
        .map(|(n, c, u)| {
            json!({
                "column": n,
                "compressed_size": c,
                "uncompressed_size": u,
                "compression_ratio": ratio(u, c),
            })
        })
        .collect();
    Ok(json!({
        "total_compressed_size": total_comp,
        "total_uncompressed_size": total_uncomp,
        "compression_ratio": ratio(total_uncomp, total_comp),
        "num_rows": num_rows,
        "bytes_per_row": if num_rows > 0 { json!(total_comp as f64 / num_rows as f64) } else { Value::Null },
        "columns": columns,
    }))
}

/// Roll the footer's per-column-chunk null counts up to a file-level data-quality
/// report: `num_rows`, `total_nulls`, and per-column `{column, null_count,
/// null_fraction}`. Reads only the footer. A column whose count is `null` had a
/// chunk with no statistics — its nulls are genuinely unknown, never silently
/// zero. The data-quality companion to `size_report`.
fn op_null_summary(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    let mut order: Vec<String> = Vec::new();
    // None = unknown (some chunk for this column carried no statistics).
    let mut nulls: std::collections::HashMap<String, Option<i64>> =
        std::collections::HashMap::new();
    let mut num_rows: i64 = 0;
    for j in 0..m.num_row_groups() {
        let rg = m.row_group(j);
        num_rows += rg.num_rows();
        for i in 0..rg.num_columns() {
            let col = rg.column(i);
            let name = descr.column(i).path().string();
            if !nulls.contains_key(&name) {
                order.push(name.clone());
                nulls.insert(name.clone(), Some(0));
            }
            let chunk_nulls = col.statistics().and_then(|s| s.null_count_opt());
            let entry = nulls.get_mut(&name).unwrap();
            match (*entry, chunk_nulls) {
                (Some(acc), Some(c)) => *entry = Some(acc + c as i64),
                _ => *entry = None,
            }
        }
    }
    let mut total: Option<i64> = Some(0);
    let columns: Vec<Value> = order
        .iter()
        .map(|n| {
            let nc = nulls[n];
            match (total, nc) {
                (Some(t), Some(c)) => total = Some(t + c),
                _ => total = None,
            }
            let frac = match nc {
                Some(c) if num_rows > 0 => json!(c as f64 / num_rows as f64),
                _ => Value::Null,
            };
            json!({
                "column": n,
                "null_count": nc.map(|c| json!(c)).unwrap_or(Value::Null),
                "null_fraction": frac,
            })
        })
        .collect();
    Ok(json!({
        "num_rows": num_rows,
        "total_nulls": total.map(|t| json!(t)).unwrap_or(Value::Null),
        "columns": columns,
    }))
}

/// Roll the footer's per-column-chunk physical-encoding metadata up to a
/// file-level report: for each column, the distinct `encodings` and
/// `compression` codecs used across every row group (each a sorted, de-duped
/// list). Reads only the footer — no column data is decoded. Answers "how is
/// this file physically encoded" without walking every row group the way
/// `column_chunk_stats` does. The encoding companion to `size_report`.
fn op_encoding_summary(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    let mut order: Vec<String> = Vec::new();
    let mut encs: std::collections::HashMap<String, std::collections::BTreeSet<String>> =
        std::collections::HashMap::new();
    let mut comps: std::collections::HashMap<String, std::collections::BTreeSet<String>> =
        std::collections::HashMap::new();
    for j in 0..m.num_row_groups() {
        let rg = m.row_group(j);
        for i in 0..rg.num_columns() {
            let col = rg.column(i);
            let name = descr.column(i).path().string();
            if !encs.contains_key(&name) {
                order.push(name.clone());
            }
            let e = encs.entry(name.clone()).or_default();
            for enc in col.encodings() {
                e.insert(format!("{enc:?}"));
            }
            comps
                .entry(name)
                .or_default()
                .insert(format!("{:?}", col.compression()));
        }
    }
    let columns: Vec<Value> = order
        .into_iter()
        .map(|n| {
            json!({
                "column": n,
                "encodings": encs[&n].iter().cloned().collect::<Vec<_>>(),
                "compression": comps[&n].iter().cloned().collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(json!({ "columns": columns }))
}

/// Report which columns carry a bloom filter, read from the footer's per-chunk
/// `bloom_filter_offset`. Bloom filters accelerate point lookups (`col = x`) but
/// are written only when explicitly enabled, so auditing their presence matters.
/// A column counts as having one if any of its chunks does. Returns per-column
/// `{column, has_bloom_filter, chunks_with_filter}` plus file-level counts.
fn op_bloom_filter_summary(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    let mut order: Vec<String> = Vec::new();
    let mut chunks_with: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for j in 0..m.num_row_groups() {
        let rg = m.row_group(j);
        for i in 0..rg.num_columns() {
            let col = rg.column(i);
            let name = descr.column(i).path().string();
            if !chunks_with.contains_key(&name) {
                order.push(name.clone());
                chunks_with.insert(name.clone(), 0);
            }
            if col.bloom_filter_offset().is_some() {
                *chunks_with.get_mut(&name).unwrap() += 1;
            }
        }
    }
    let mut with_filter = 0i64;
    let columns: Vec<Value> = order
        .into_iter()
        .map(|n| {
            let c = chunks_with[&n];
            if c > 0 {
                with_filter += 1;
            }
            json!({
                "column": n,
                "has_bloom_filter": c > 0,
                "chunks_with_filter": c,
            })
        })
        .collect();
    let total = columns.len() as i64;
    Ok(json!({
        "columns": columns,
        "columns_with_bloom_filter": with_filter,
        "columns_total": total,
    }))
}

/// Report the declared sort order of each row group, from the footer's
/// `sorting_columns`. A writer can record that data is sorted by certain columns
/// (ascending/descending, nulls-first) so readers can skip or merge efficiently;
/// most files leave it unset. Each row group lists `{column, column_idx,
/// descending, nulls_first}`. Returns `{row_groups: [{row_group, sorting_columns}],
/// has_sorting_columns}`. Reads only the footer. Pure.
fn op_sorting_columns_summary(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    let mut any = false;
    let mut row_groups: Vec<Value> = Vec::with_capacity(m.num_row_groups());
    for j in 0..m.num_row_groups() {
        let rg = m.row_group(j);
        let cols: Vec<Value> = match rg.sorting_columns() {
            Some(sc) if !sc.is_empty() => {
                any = true;
                sc.iter()
                    .map(|s| {
                        let idx = s.column_idx as usize;
                        let name = if idx < descr.num_columns() {
                            descr.column(idx).path().string()
                        } else {
                            format!("col_{idx}")
                        };
                        json!({
                            "column": name,
                            "column_idx": s.column_idx,
                            "descending": s.descending,
                            "nulls_first": s.nulls_first,
                        })
                    })
                    .collect()
            }
            _ => Vec::new(),
        };
        row_groups.push(json!({"row_group": j, "sorting_columns": cols}));
    }
    Ok(json!({
        "row_groups": row_groups,
        "has_sorting_columns": any,
    }))
}

/// Read `n` rows starting at absolute row `offset` (default offset 0, n 10).
/// `head` reads from the start and `tail` from the end; this fills the gap
/// with an arbitrary window. Honors an optional `columns` projection.
fn op_sample(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let offset = args["offset"].as_u64().unwrap_or(0) as usize;
    let n = args["n"].as_u64().unwrap_or(10) as usize;
    let cols = parse_columns(&args["columns"]);
    let reader = open_parquet_reader_with_columns(Path::new(path), 8192, cols.as_deref())?;
    let mut buf = Vec::<u8>::new();
    {
        let mut w = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        let mut seen = 0usize;
        let mut emitted = 0usize;
        for batch in reader {
            let mut batch = batch?;
            let bn = batch.num_rows();
            let batch_start = seen;
            seen += bn;
            if batch_start + bn <= offset {
                continue;
            }
            let local = offset.saturating_sub(batch_start);
            if local > 0 {
                batch = batch.slice(local, bn - local);
            }
            let remaining = n - emitted;
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
            }
            if batch.num_rows() == 0 {
                if emitted >= n {
                    break;
                }
                continue;
            }
            w.write(&batch)?;
            emitted += batch.num_rows();
            if emitted >= n {
                break;
            }
        }
        w.finish()?;
    }
    let rows = ndjson_to_rows(&buf)?;
    Ok(json!({"rows": rows}))
}

/// Which optional parquet index/filter structures the footer references, per
/// column and aggregated. Presence is detected from the column-chunk offsets
/// (bloom filter, column index, offset index) — no page reads.
fn op_features(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let r = open_serialized(Path::new(path))?;
    let m = r.metadata();
    let descr = m.file_metadata().schema_descr();
    let mut cols: Vec<Value> = Vec::new();
    let mut any_bloom = false;
    let mut any_colidx = false;
    let mut any_offidx = false;
    for i in 0..descr.num_columns() {
        let name = descr.column(i).path().string();
        let mut bloom = false;
        let mut colidx = false;
        let mut offidx = false;
        for j in 0..m.num_row_groups() {
            let col = m.row_group(j).column(i);
            bloom |= col.bloom_filter_offset().is_some();
            colidx |= col.column_index_offset().is_some();
            offidx |= col.offset_index_offset().is_some();
        }
        any_bloom |= bloom;
        any_colidx |= colidx;
        any_offidx |= offidx;
        cols.push(json!({
            "column": name,
            "bloom_filter": bloom,
            "column_index": colidx,
            "offset_index": offidx,
        }));
    }
    Ok(json!({
        "has_bloom_filter": any_bloom,
        "has_column_index": any_colidx,
        "has_offset_index": any_offidx,
        "columns": cols,
    }))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-parquet handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn parquet__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn parquet__inspect(args: *const c_char) -> *const c_char {
    ffi_call(args, op_inspect)
}

#[no_mangle]
pub extern "C" fn parquet__schema(args: *const c_char) -> *const c_char {
    ffi_call(args, op_schema)
}

#[no_mangle]
pub extern "C" fn parquet__count(args: *const c_char) -> *const c_char {
    ffi_call(args, op_count)
}

#[no_mangle]
pub extern "C" fn parquet__rowgroups(args: *const c_char) -> *const c_char {
    ffi_call(args, op_rowgroups)
}

#[no_mangle]
pub extern "C" fn parquet__row_group_summary(args: *const c_char) -> *const c_char {
    ffi_call(args, op_row_group_summary)
}

#[no_mangle]
pub extern "C" fn parquet__stats(args: *const c_char) -> *const c_char {
    ffi_call(args, op_stats)
}

#[no_mangle]
pub extern "C" fn parquet__head(args: *const c_char) -> *const c_char {
    ffi_call(args, op_head)
}

#[no_mangle]
pub extern "C" fn parquet__tail(args: *const c_char) -> *const c_char {
    ffi_call(args, op_tail)
}

#[no_mangle]
pub extern "C" fn parquet__reverse(args: *const c_char) -> *const c_char {
    ffi_call(args, op_reverse)
}

#[no_mangle]
pub extern "C" fn parquet__gather(args: *const c_char) -> *const c_char {
    ffi_call(args, op_gather)
}

#[no_mangle]
pub extern "C" fn parquet__value_counts(args: *const c_char) -> *const c_char {
    ffi_call(args, op_value_counts)
}

#[no_mangle]
pub extern "C" fn parquet__slice(args: *const c_char) -> *const c_char {
    ffi_call(args, op_slice)
}

#[no_mangle]
pub extern "C" fn parquet__to_json(args: *const c_char) -> *const c_char {
    ffi_call(args, op_to_json)
}

#[no_mangle]
pub extern "C" fn parquet__to_csv(args: *const c_char) -> *const c_char {
    ffi_call(args, op_to_csv)
}

#[no_mangle]
pub extern "C" fn parquet__compress(args: *const c_char) -> *const c_char {
    ffi_call(args, op_compress)
}

#[no_mangle]
pub extern "C" fn parquet__from_csv(args: *const c_char) -> *const c_char {
    ffi_call(args, op_from_csv)
}

#[no_mangle]
pub extern "C" fn parquet__from_json(args: *const c_char) -> *const c_char {
    ffi_call(args, op_from_json)
}

#[no_mangle]
pub extern "C" fn parquet__merge(args: *const c_char) -> *const c_char {
    ffi_call(args, op_merge)
}

#[no_mangle]
pub extern "C" fn parquet__hstack(args: *const c_char) -> *const c_char {
    ffi_call(args, op_hstack)
}

#[no_mangle]
pub extern "C" fn parquet__select(args: *const c_char) -> *const c_char {
    ffi_call(args, op_select)
}

#[no_mangle]
pub extern "C" fn parquet__drop(args: *const c_char) -> *const c_char {
    ffi_call(args, op_drop)
}

#[no_mangle]
pub extern "C" fn parquet__rename(args: *const c_char) -> *const c_char {
    ffi_call(args, op_rename)
}

#[no_mangle]
pub extern "C" fn parquet__write(args: *const c_char) -> *const c_char {
    ffi_call(args, op_write)
}

#[no_mangle]
pub extern "C" fn parquet__write_partitioned(args: *const c_char) -> *const c_char {
    ffi_call(args, op_write_partitioned)
}

#[no_mangle]
pub extern "C" fn parquet__metadata(args: *const c_char) -> *const c_char {
    ffi_call(args, op_metadata)
}

#[no_mangle]
pub extern "C" fn parquet__mkdemo(args: *const c_char) -> *const c_char {
    ffi_call(args, op_mkdemo)
}

#[no_mangle]
pub extern "C" fn parquet__validate(args: *const c_char) -> *const c_char {
    ffi_call(args, op_validate)
}

#[no_mangle]
pub extern "C" fn parquet__column_chunk_stats(args: *const c_char) -> *const c_char {
    ffi_call(args, op_column_chunk_stats)
}

#[no_mangle]
pub extern "C" fn parquet__size_report(args: *const c_char) -> *const c_char {
    ffi_call(args, op_size_report)
}

#[no_mangle]
pub extern "C" fn parquet__null_summary(args: *const c_char) -> *const c_char {
    ffi_call(args, op_null_summary)
}

#[no_mangle]
pub extern "C" fn parquet__encoding_summary(args: *const c_char) -> *const c_char {
    ffi_call(args, op_encoding_summary)
}

#[no_mangle]
pub extern "C" fn parquet__bloom_filter_summary(args: *const c_char) -> *const c_char {
    ffi_call(args, op_bloom_filter_summary)
}

#[no_mangle]
pub extern "C" fn parquet__sorting_columns_summary(args: *const c_char) -> *const c_char {
    ffi_call(args, op_sorting_columns_summary)
}

#[no_mangle]
pub extern "C" fn parquet__sample(args: *const c_char) -> *const c_char {
    ffi_call(args, op_sample)
}

#[no_mangle]
pub extern "C" fn parquet__features(args: *const c_char) -> *const c_char {
    ffi_call(args, op_features)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── compression_for ──

    #[test]
    fn compression_canonical_names() {
        assert!(matches!(
            compression_for("none").unwrap(),
            Compression::UNCOMPRESSED
        ));
        assert!(matches!(
            compression_for("snappy").unwrap(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            compression_for("lz4").unwrap(),
            Compression::LZ4_RAW
        ));
    }

    #[test]
    fn compression_case_insensitive() {
        assert!(matches!(
            compression_for("SNAPPY").unwrap(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            compression_for("Gzip").unwrap(),
            Compression::GZIP(_)
        ));
    }

    #[test]
    fn compression_aliases() {
        assert!(matches!(
            compression_for("uncompressed").unwrap(),
            Compression::UNCOMPRESSED
        ));
    }

    #[test]
    fn compression_unknown_errors_with_name() {
        let err = compression_for("lzma").unwrap_err().to_string();
        assert!(err.contains("lzma"), "{err}");
    }

    // ── cmp_max ──

    #[test]
    fn cmp_max_true_when_b_greater() {
        assert!(cmp_max(&json!(1.0), &json!(2.0)));
        assert!(cmp_max(&json!(1), &json!(2)));
    }

    #[test]
    fn cmp_max_false_when_b_lesser_or_equal() {
        assert!(!cmp_max(&json!(2.0), &json!(1.0)));
        assert!(!cmp_max(&json!(2.0), &json!(2.0)));
    }

    #[test]
    fn cmp_max_non_numeric_is_false() {
        // Stats reduction folds over batches; non-numeric (string, null,
        // array) values must not promote a "new max" — keep the existing.
        assert!(!cmp_max(&json!("a"), &json!("b")));
        assert!(!cmp_max(&Value::Null, &json!(5)));
        assert!(!cmp_max(&json!(5), &Value::Null));
    }

    // ── ndjson_to_rows ──

    #[test]
    fn ndjson_parses_single_line() {
        let buf = b"{\"a\":1}\n";
        let rows = ndjson_to_rows(buf).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], json!(1));
    }

    #[test]
    fn ndjson_parses_multi_line() {
        let buf = b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";
        let rows = ndjson_to_rows(buf).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2]["a"], json!(3));
    }

    #[test]
    fn ndjson_skips_blank_lines() {
        let buf = b"{\"a\":1}\n\n{\"a\":2}\n\n";
        let rows = ndjson_to_rows(buf).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn ndjson_empty_buf_yields_empty_rows() {
        assert!(ndjson_to_rows(b"").unwrap().is_empty());
        assert!(ndjson_to_rows(b"\n\n\n").unwrap().is_empty());
    }

    #[test]
    fn ndjson_invalid_json_errors() {
        let buf = b"{\"a\":1}\nnot-json\n";
        assert!(ndjson_to_rows(buf).is_err());
    }

    #[test]
    fn ndjson_invalid_utf8_errors() {
        let buf = &[0xFF_u8, 0xFE, b'\n'];
        assert!(ndjson_to_rows(buf).is_err());
    }

    /// `parse_columns` accepts a JSON array of strings, filters non-strings
    /// from the array (a single bad element doesn't poison the whole list),
    /// and returns None for non-array inputs (so a caller passing a bare
    /// string `"id,name"` doesn't get silently wrapped). Pin the contract
    /// so refactors can't accidentally start panicking on `[]`, returning
    /// `Some([])` on null, or stringifying numbers.
    #[test]
    fn parse_columns_array_of_strings_round_trips() {
        let v = parse_columns(&json!(["id", "name"]));
        assert_eq!(v, Some(vec!["id".to_string(), "name".to_string()]));
    }

    #[test]
    fn parse_columns_filters_non_strings() {
        let v = parse_columns(&json!(["id", 42, "name", null]));
        assert_eq!(v, Some(vec!["id".to_string(), "name".to_string()]));
    }

    #[test]
    fn parse_columns_non_array_is_none() {
        assert_eq!(parse_columns(&json!("id,name")), None);
        assert_eq!(parse_columns(&Value::Null), None);
        assert_eq!(parse_columns(&json!({"cols": ["id"]})), None);
    }

    /// `parse_columns(&json!([]))` now returns None (no projection) instead
    /// of Some(vec![]) — matches pandas/polars `read_parquet(columns=[])`
    /// semantics and prevents the downstream ProjectionMask from dropping
    /// every schema field.
    #[test]
    fn parse_columns_empty_array_is_none_for_no_projection() {
        assert_eq!(parse_columns(&json!([])), None);
    }

    // ── multi-row-group bug tests ──
    //
    // The next two tests construct a parquet file with multiple row groups
    // and exercise op_stats / op_tail. They target two distinct logic bugs:
    //
    //   1. op_stats: `min` is set from the FIRST non-null row group and
    //      never folded across remaining row groups. A descending column
    //      written across multiple row groups reports the wrong min.
    //   2. op_tail: only the LAST row group is read. If the user asks for
    //      more rows than the last group contains, the call silently
    //      returns fewer rows than requested — wrong semantics for `tail`.

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as ArcAlias;

    fn unique_tmp_path(name: &str) -> std::path::PathBuf {
        // Avoid pulling in `tempfile` as a dev-dep; use a per-test path
        // under std::env::temp_dir() qualified by pid+nanos+name.
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut p = std::env::temp_dir();
        p.push(format!("stryke-parquet-test-{pid}-{nanos}-{name}"));
        p
    }

    fn write_multi_rg_parquet(path: &std::path::Path) {
        // 3 row groups of 1 row each. Values: [30, 20, 10] — descending
        // across groups, so a correct stats fold would yield min=10, max=30.
        let schema = ArcAlias::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let file = File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, ArcAlias::clone(&schema), Some(props)).unwrap();
        for value in [30_i64, 20, 10] {
            let arr: Int64Array = vec![value].into();
            let batch =
                RecordBatch::try_new(ArcAlias::clone(&schema), vec![ArcAlias::new(arr)]).unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }

    #[test]
    fn op_stats_folds_min_across_row_groups() {
        // Bug class: op_stats sets `min` once from the first row group
        // (src/lib.rs:230-232) and never updates it for subsequent groups.
        // A file written with values [30, 20, 10] in separate row groups
        // should report min=10; the current code reports min=30.
        let path = unique_tmp_path("desc.parquet");
        write_multi_rg_parquet(&path);

        let val = op_stats(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        let cols = val["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 1, "one column expected");
        let min = &cols[0]["min"];
        let max = &cols[0]["max"];
        assert_eq!(
            min,
            &json!(10),
            "min must fold to smallest across groups (got {min})"
        );
        assert_eq!(
            max,
            &json!(30),
            "max must fold to largest across groups (got {max})"
        );
    }

    #[test]
    fn op_tail_spans_multiple_row_groups() {
        // Bug class: op_tail reads only the last row group
        // (src/lib.rs:296-299). Requesting tail(n=3) on a 3-row-group file
        // where each group has 1 row should return all three rows in
        // file order; current implementation returns only the last row.
        let path = unique_tmp_path("tail.parquet");
        write_multi_rg_parquet(&path);

        let val = op_tail(json!({ "path": path.to_str().unwrap(), "n": 3 })).unwrap();
        let _ = std::fs::remove_file(&path);
        let rows = val["rows"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            3,
            "tail(3) on 3 single-row groups must return 3 rows, got {}",
            rows.len()
        );
        // Last row of the file should be the last value written: 10.
        assert_eq!(rows[rows.len() - 1]["v"], json!(10));
    }

    #[test]
    fn op_slice_returns_an_offset_window() {
        // Write ids 1..=5 in one file.
        let path = unique_tmp_path("slice.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Int64Array = vec![1_i64, 2, 3, 4, 5].into();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
        {
            let file = File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let ids_of = |v: &Value| -> Vec<i64> {
            v["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect()
        };
        // offset 1, length 2 → rows 2,3.
        let w =
            op_slice(json!({"path": path.to_str().unwrap(), "offset": 1, "length": 2})).unwrap();
        assert_eq!(ids_of(&w), vec![2, 3]);
        // offset only → to the end.
        let e = op_slice(json!({"path": path.to_str().unwrap(), "offset": 3})).unwrap();
        assert_eq!(ids_of(&e), vec![4, 5]);
        // length exceeding the remainder is capped.
        let c =
            op_slice(json!({"path": path.to_str().unwrap(), "offset": 4, "length": 99})).unwrap();
        assert_eq!(ids_of(&c), vec![5]);
        // offset past the end → empty.
        let p =
            op_slice(json!({"path": path.to_str().unwrap(), "offset": 10, "length": 3})).unwrap();
        assert_eq!(p["rows"].as_array().unwrap().len(), 0);
        // offset 0 with no length is the whole file.
        let all = op_slice(json!({"path": path.to_str().unwrap()})).unwrap();
        assert_eq!(ids_of(&all), vec![1, 2, 3, 4, 5]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn op_reverse_returns_all_rows_in_reverse_order() {
        // Single-file ids 1..=5 → reverse is 5,4,3,2,1.
        let path = unique_tmp_path("reverse.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Int64Array = vec![1_i64, 2, 3, 4, 5].into();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
        {
            let file = File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let ids_of = |v: &Value| -> Vec<i64> {
            v["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect()
        };
        let r = op_reverse(json!({"path": path.to_str().unwrap()})).unwrap();
        assert_eq!(ids_of(&r), vec![5, 4, 3, 2, 1]);
        // Column projection is honored.
        let proj = op_reverse(json!({"path": path.to_str().unwrap(), "columns": ["id"]})).unwrap();
        assert_eq!(ids_of(&proj), vec![5, 4, 3, 2, 1]);
        let _ = std::fs::remove_file(&path);

        // Across multiple row groups, the global row order is reversed (last
        // written row, 10, comes first).
        let mpath = unique_tmp_path("reverse_multi.parquet");
        write_multi_rg_parquet(&mpath);
        let mr = op_reverse(json!({"path": mpath.to_str().unwrap()})).unwrap();
        let rows = mr["rows"].as_array().unwrap();
        assert_eq!(
            rows[0]["v"],
            json!(10),
            "reverse puts the last-written row first"
        );
        let _ = std::fs::remove_file(&mpath);
    }

    #[test]
    fn op_gather_takes_rows_by_explicit_index_list() {
        let path = unique_tmp_path("gather.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Int64Array = vec![10_i64, 20, 30, 40, 50].into();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
        {
            let file = File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let ids_of = |v: &Value| -> Vec<i64> {
            v["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect()
        };
        // Arbitrary order, a repeat, and a subset.
        let r = op_gather(json!({
            "path": path.to_str().unwrap(),
            "indices": [4, 0, 2, 2],
        }))
        .unwrap();
        assert_eq!(
            ids_of(&r),
            vec![50, 10, 30, 30],
            "rows follow the index list"
        );
        // Out-of-range index dies.
        let err = op_gather(json!({
            "path": path.to_str().unwrap(),
            "indices": [0, 5],
        }));
        assert!(err.is_err(), "index 5 is out of range for 5 rows");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn op_value_counts_tallies_a_column_sorted_by_frequency() {
        // color: red×3, blue×2, green×1.
        let path = unique_tmp_path("vcounts.parquet");
        let schema = ArcAlias::new(Schema::new(vec![Field::new(
            "color",
            DataType::Utf8,
            false,
        )]));
        let colors =
            arrow::array::StringArray::from(vec!["red", "blue", "red", "green", "blue", "red"]);
        let batch =
            RecordBatch::try_new(ArcAlias::clone(&schema), vec![ArcAlias::new(colors)]).unwrap();
        {
            let file = File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let r = op_value_counts(json!({
            "path": path.to_str().unwrap(),
            "column": "color",
        }))
        .unwrap();
        assert_eq!(r["distinct"].as_u64().unwrap(), 3);
        let rows = r["rows"].as_array().unwrap();
        let pairs: Vec<(&str, u64)> = rows
            .iter()
            .map(|row| {
                (
                    row["value"].as_str().unwrap(),
                    row["count"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            pairs,
            vec![("red", 3), ("blue", 2), ("green", 1)],
            "sorted by count descending"
        );
        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(test)]
mod tests_audit {
    //! Audit-pass edge-case tests targeting two distinct correctness gaps not
    //! covered by the existing `tests` module:
    //!
    //!   1. `parse_columns` collapses an empty JSON array `[]` to
    //!      `Some(vec![])` rather than `None`. That propagates to
    //!      `open_parquet_reader_with_columns`, which then builds an empty
    //!      `ProjectionMask` — silently projecting NO columns. A natural
    //!      caller default of `columns: []` therefore yields rows with zero
    //!      fields instead of the full schema.
    //!
    //!   2. `stat_minmax` only handles five physical types (Boolean, Int32,
    //!      Int64, Float, Double). ByteArray / FixedLenByteArray fall to the
    //!      `_ =>` arm and return `(Null, Null)`. Parquet writes statistics
    //!      for Utf8 columns by default, so `op_stats` silently drops every
    //!      string column's min/max.
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use serde_json::json;
    use std::fs::File;
    use std::sync::Arc;

    fn unique_audit_path(name: &str) -> std::path::PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut p = std::env::temp_dir();
        p.push(format!("stryke-parquet-audit-{pid}-{nanos}-{name}"));
        p
    }

    /// Bug class: `parse_columns(&json!([]))` returns `Some(vec![])`. That
    /// is structurally distinct from `None` and downstream
    /// `ProjectionMask::columns(... empty iter ...)` yields an all-false
    /// mask — i.e. project zero columns. A caller that passes `[]` as a
    /// default "no filter" value gets rows stripped of every field.
    ///
    /// Not a boilerplate check: this is NOT verifying that
    /// `parse_columns` returns `Some(vec![])` (a mirror of the impl). It is
    /// verifying the END-TO-END caller-visible consequence — that an empty
    /// columns array makes `op_head` return rows whose JSON objects have
    /// zero keys — which is the actual surface bug a stryke caller hits.
    #[test]
    fn op_head_with_empty_columns_array_returns_empty_field_rows() {
        let path = unique_audit_path("empty-cols.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Int64Array = vec![1_i64, 2, 3].into();
        let names = StringArray::from(vec!["a", "b", "c"]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(names)])
            .unwrap();
        {
            let file = File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let val = op_head(json!({
            "path": path.to_str().unwrap(),
            "n": 2,
            "columns": []
        }))
        .unwrap();
        let _ = std::fs::remove_file(&path);

        let rows = val["rows"].as_array().expect("rows must be an array");
        assert_eq!(rows.len(), 2, "head(n=2) must return two rows");
        // A correct implementation would treat `columns: []` as "no
        // projection" and emit rows with the full schema. The current code
        // applies an empty projection and emits rows with zero fields.
        let first = rows[0].as_object().expect("row must be an object");
        assert!(
            first.contains_key("id") && first.contains_key("name"),
            "empty `columns: []` must not drop schema fields; got row keys {:?}",
            first.keys().collect::<Vec<_>>()
        );
    }

    /// Bug class: `stat_minmax` returns `(Null, Null)` for ByteArray-backed
    /// columns (Utf8 strings). Parquet writes ByteArray min/max statistics
    /// by default, so `op_stats` silently drops min/max for every string
    /// column. A stryke caller asking "what's the alphabetical range of
    /// `name`?" gets `null`, indistinguishable from "no stats present".
    ///
    /// Not a boilerplate check: this is not asserting that `op_stats`
    /// returns SOME shape — it asserts the SEMANTICS that for a Utf8
    /// column with known-distinct values "a","b","c", min must equal "a"
    /// and max must equal "c". A passing test means `stat_minmax`'s
    /// `Statistics::ByteArray` arm has been implemented.
    #[test]
    fn op_stats_returns_min_max_for_utf8_string_column() {
        let path = unique_audit_path("utf8-stats.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let names = StringArray::from(vec!["a", "b", "c"]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(names)]).unwrap();
        {
            // Explicitly enable statistics at chunk level so this test
            // catches the `stat_minmax` gap, not a missing-stats false
            // negative.
            let props = WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Chunk)
                .build();
            let file = File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let val = op_stats(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);

        let cols = val["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 1, "one column expected");
        let min = &cols[0]["min"];
        let max = &cols[0]["max"];
        // A correct implementation extracts ByteArray min/max bytes and
        // converts to a JSON string. Current code falls through the match
        // and returns Null, so this assertion fails until `stat_minmax`
        // handles `Statistics::ByteArray`.
        assert!(
            !min.is_null() && !max.is_null(),
            "Utf8 column stats must produce non-null min/max; got min={min} max={max}"
        );
    }

    // ── cmp_lt (op_stats min-fold) ──
    //
    // `cmp_lt` drives the min-fold at line 282: `if cmp_lt(&mn, &min)`.
    // `cmp_max` is tested above but `cmp_lt` is exercised only indirectly
    // through op_stats. These pin its contract directly so a refactor that
    // flips the comparison, drops the `as_f64()` short-circuit, or promotes
    // on equality can't slip through.

    /// Strict less-than: `cmp_lt(a, b)` is true iff `a < b`. The min-fold
    /// only replaces the running min when the new value is *strictly*
    /// smaller; equal mins across row groups must NOT trigger a replace
    /// (off-by-one at the boundary — a `<=` would needlessly churn `min`
    /// and, combined with a non-numeric value, could promote Null).
    #[test]
    fn cmp_lt_strict_and_directional() {
        assert!(cmp_lt(&json!(1.0), &json!(2.0)), "1 < 2 must be true");
        assert!(cmp_lt(&json!(1), &json!(2)), "integer JSON path via as_f64");
        assert!(!cmp_lt(&json!(2.0), &json!(1.0)), "2 < 1 must be false");
        // Boundary: equal values are not strictly less-than. If this became
        // true, op_stats would replace `min` with an equal `mn` on every row
        // group — harmless for numbers but a `<=` plus the Null short-circuit
        // below is what guards the fold.
        assert!(!cmp_lt(&json!(5), &json!(5)), "equal is not strictly-less");
        assert!(!cmp_lt(&json!(-3), &json!(-3)));
    }

    /// Non-numeric operands must short-circuit to `false`. In op_stats the
    /// min-fold sees `cmp_lt(&mn, &min)`; if `mn` is a string min (ByteArray
    /// column) or a Null, `as_f64()` yields None and the fold must keep the
    /// existing `min` rather than swap in an unordered value. A regression to
    /// e.g. lexicographic string compare would silently corrupt min for
    /// string columns folded across row groups.
    #[test]
    fn cmp_lt_non_numeric_is_false() {
        assert!(!cmp_lt(&json!("a"), &json!("b")), "string operands → false");
        assert!(!cmp_lt(&Value::Null, &json!(5)), "Null lhs → false");
        assert!(!cmp_lt(&json!(5), &Value::Null), "Null rhs → false");
        assert!(!cmp_lt(&json!([1]), &json!([2])), "array operands → false");
        assert!(
            !cmp_lt(&json!(true), &json!(false)),
            "bool operands → false"
        );
    }

    // ── write paths: from_csv / from_json / merge ──
    //
    // These round-trip real data through the new write ops in-process (no FFI,
    // no install, no release build) so the write logic is exercised under
    // `cargo test`. They pin the contract callers depend on: row count
    // preserved, schema inferred, and merge = sum of inputs.

    fn count_parquet_rows(path: &std::path::Path) -> usize {
        let r = open_parquet_reader(path, 8192).unwrap();
        r.map(|b| b.unwrap().num_rows()).sum()
    }

    #[test]
    fn op_from_csv_infers_schema_and_preserves_rows() {
        let csv = unique_audit_path("from_csv.csv");
        std::fs::write(
            &csv,
            "id,name,score\n1,alice,1.5\n2,bob,2.0\n3,carol,3.25\n",
        )
        .unwrap();
        let out = unique_audit_path("from_csv.parquet");
        let r = op_from_csv(json!({
            "src": csv.to_str().unwrap(),
            "dst": out.to_str().unwrap(),
        }))
        .unwrap();
        assert_eq!(r["rows"], json!(3), "row count must round-trip; got {r}");
        assert_eq!(count_parquet_rows(&out), 3);
        // header row inferred 3 columns, not folded into the data.
        let reader = open_parquet_reader(&out, 8192).unwrap();
        assert_eq!(reader.schema().fields().len(), 3, "id/name/score columns");
        let _ = std::fs::remove_file(&csv);
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn op_from_json_reads_ndjson_one_object_per_line() {
        let nd = unique_audit_path("from_json.ndjson");
        std::fs::write(
            &nd,
            "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n",
        )
        .unwrap();
        let out = unique_audit_path("from_json.parquet");
        let r = op_from_json(json!({
            "src": nd.to_str().unwrap(),
            "dst": out.to_str().unwrap(),
        }))
        .unwrap();
        assert_eq!(r["rows"], json!(2), "two NDJSON lines → two rows; got {r}");
        assert_eq!(count_parquet_rows(&out), 2);
        let _ = std::fs::remove_file(&nd);
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn op_write_builds_parquet_from_in_memory_rows() {
        let out = unique_audit_path("write_rows.parquet");
        let r = op_write(json!({
            "dst": out.to_str().unwrap(),
            "rows": [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
                {"id": 3, "name": "carol"},
            ],
        }))
        .unwrap();
        assert_eq!(r["rows"], json!(3), "all in-memory rows written; got {r}");
        assert_eq!(count_parquet_rows(&out), 3);
        let reader = open_parquet_reader(&out, 8192).unwrap();
        assert_eq!(reader.schema().fields().len(), 2, "id + name inferred");
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn op_write_partitioned_splits_by_column_into_hive_dirs() {
        let base = unique_audit_path("partds");
        let r = op_write_partitioned(json!({
            "dst": base.to_str().unwrap(),
            "partition_by": "region",
            "rows": [
                {"region": "us", "id": 1},
                {"region": "eu", "id": 2},
                {"region": "us", "id": 3},
            ],
        }))
        .unwrap();
        assert_eq!(r["total_rows"], json!(3));
        let parts = r["partitions"].as_array().unwrap();
        assert_eq!(parts.len(), 2, "two distinct regions → two partitions");
        // BTreeMap order: eu (1 row), us (2 rows).
        assert_eq!(parts[0]["value"], json!("eu"));
        assert_eq!(parts[0]["rows"], json!(1));
        assert_eq!(parts[1]["value"], json!("us"));
        assert_eq!(parts[1]["rows"], json!(2));
        // Hive `region=us/part-0.parquet` exists and holds 2 rows.
        let us = base.join("region=us").join("part-0.parquet");
        assert!(us.exists(), "expected {us:?}");
        assert_eq!(count_parquet_rows(&us), 2);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn op_metadata_reads_writer_kv_and_created_by() {
        // mkdemo writes a parquet; ArrowWriter stamps `created_by` + an
        // ARROW:schema key-value entry. metadata must surface both.
        let out = unique_audit_path("meta.parquet");
        op_mkdemo(json!({"path": out.to_str().unwrap()})).unwrap();
        let m = op_metadata(json!({"path": out.to_str().unwrap()})).unwrap();
        assert!(
            m["created_by"]
                .as_str()
                .is_some_and(|s| s.contains("parquet")),
            "created_by should name the writer; got {m}"
        );
        // The arrow writer always embeds an ARROW:schema kv entry.
        assert!(
            m["metadata"].get("ARROW:schema").is_some(),
            "arrow writer kv metadata must be surfaced; got {m}"
        );
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn op_merge_sums_rows_of_same_schema_inputs() {
        let csv = unique_audit_path("merge.csv");
        std::fs::write(&csv, "id,name\n1,a\n2,b\n3,c\n").unwrap();
        let part = unique_audit_path("merge_part.parquet");
        op_from_csv(json!({
            "src": csv.to_str().unwrap(),
            "dst": part.to_str().unwrap(),
        }))
        .unwrap();
        let out = unique_audit_path("merged.parquet");
        let r = op_merge(json!({
            "srcs": [part.to_str().unwrap(), part.to_str().unwrap()],
            "dst": out.to_str().unwrap(),
        }))
        .unwrap();
        assert_eq!(r["files"], json!(2));
        assert_eq!(
            count_parquet_rows(&out),
            6,
            "merge of two 3-row files → 6 rows"
        );
        let _ = std::fs::remove_file(&csv);
        let _ = std::fs::remove_file(&part);
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn op_hstack_appends_columns_at_matching_row_count() {
        let lcsv = unique_audit_path("hsl.csv");
        std::fs::write(&lcsv, "id\n1\n2\n3\n").unwrap();
        let rcsv = unique_audit_path("hsr.csv");
        std::fs::write(&rcsv, "name\na\nb\nc\n").unwrap();
        let left = unique_audit_path("hsl.parquet");
        let right = unique_audit_path("hsr.parquet");
        op_from_csv(json!({"src": lcsv.to_str().unwrap(), "dst": left.to_str().unwrap()})).unwrap();
        op_from_csv(json!({"src": rcsv.to_str().unwrap(), "dst": right.to_str().unwrap()}))
            .unwrap();
        let out = unique_audit_path("hstack.parquet");
        let r = op_hstack(json!({
            "path": left.to_str().unwrap(),
            "other": right.to_str().unwrap(),
            "dst": out.to_str().unwrap(),
        }))
        .unwrap();
        assert_eq!(r["rows"], json!(3));
        assert_eq!(
            r["columns"],
            json!(["id", "name"]),
            "src columns then other columns"
        );
        assert_eq!(count_parquet_rows(&out), 3);
        let sch = op_schema(json!({"path": out.to_str().unwrap()})).unwrap();
        let names: Vec<&str> = sch["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["id", "name"]);
        // A mismatched row count, a duplicate column name, and a missing `other`
        // all fail loud rather than producing a misaligned or ambiguous file.
        let scsv = unique_audit_path("hss.csv");
        std::fs::write(&scsv, "name\nx\n").unwrap();
        let short = unique_audit_path("hss.parquet");
        op_from_csv(json!({"src": scsv.to_str().unwrap(), "dst": short.to_str().unwrap()}))
            .unwrap();
        assert!(op_hstack(json!({"path": left.to_str().unwrap(), "other": short.to_str().unwrap(), "dst": out.to_str().unwrap()})).is_err());
        assert!(op_hstack(json!({"path": left.to_str().unwrap(), "other": left.to_str().unwrap(), "dst": out.to_str().unwrap()})).is_err());
        assert!(
            op_hstack(json!({"path": left.to_str().unwrap(), "dst": out.to_str().unwrap()}))
                .is_err()
        );
        for p in [lcsv, rcsv, left, right, scsv, short, out] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn op_select_projects_a_column_subset_to_a_new_file() {
        let csv = unique_audit_path("select.csv");
        std::fs::write(&csv, "id,name,score\n1,a,10\n2,b,20\n3,c,30\n").unwrap();
        let src = unique_audit_path("select_src.parquet");
        op_from_csv(json!({
            "src": csv.to_str().unwrap(),
            "dst": src.to_str().unwrap(),
        }))
        .unwrap();
        let out = unique_audit_path("select_out.parquet");
        let r = op_select(json!({
            "path": src.to_str().unwrap(),
            "dst": out.to_str().unwrap(),
            "columns": ["id", "score"],
        }))
        .unwrap();
        // Row count preserved; only the two requested columns written (file order).
        assert_eq!(r["rows"], json!(3), "all rows preserved");
        assert_eq!(r["columns"], json!(["id", "score"]), "only id+score kept");
        assert_eq!(count_parquet_rows(&out), 3);
        // The output schema really has 2 fields — `name` was pruned.
        let sch = op_schema(json!({"path": out.to_str().unwrap()})).unwrap();
        assert_eq!(
            sch["num_fields"],
            json!(2),
            "name column pruned from the file"
        );
        let names: Vec<&str> = sch["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["id", "score"]);
        // An unknown column errors rather than silently writing fewer columns.
        assert!(op_select(json!({
            "path": src.to_str().unwrap(), "dst": out.to_str().unwrap(),
            "columns": ["id", "nope"],
        }))
        .is_err());
        // Missing/empty columns errors.
        assert!(op_select(json!({
            "path": src.to_str().unwrap(), "dst": out.to_str().unwrap(),
        }))
        .is_err());
        let _ = std::fs::remove_file(&csv);
        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn op_rename_relabels_columns_preserving_order_and_rows() {
        let csv = unique_audit_path("rename.csv");
        std::fs::write(&csv, "id,name,score\n1,a,10\n2,b,20\n3,c,30\n").unwrap();
        let src = unique_audit_path("rename_src.parquet");
        op_from_csv(json!({"src": csv.to_str().unwrap(), "dst": src.to_str().unwrap()})).unwrap();
        let out = unique_audit_path("rename_out.parquet");
        let r = op_rename(json!({
            "path": src.to_str().unwrap(),
            "dst": out.to_str().unwrap(),
            "rename": {"name": "label", "score": "points"},
        }))
        .unwrap();
        // Only the mapped names change; file order and row count are preserved.
        assert_eq!(r["columns"], json!(["id", "label", "points"]));
        assert_eq!(r["rows"], json!(3));
        assert_eq!(count_parquet_rows(&out), 3);
        let sch = op_schema(json!({"path": out.to_str().unwrap()})).unwrap();
        let names: Vec<&str> = sch["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["id", "label", "points"]);
        // Unknown old column, a collision with an existing column, and an empty
        // map all fail loud rather than producing a surprising file.
        assert!(op_rename(json!({"path": src.to_str().unwrap(), "dst": out.to_str().unwrap(), "rename": {"nope": "x"}})).is_err());
        assert!(op_rename(json!({"path": src.to_str().unwrap(), "dst": out.to_str().unwrap(), "rename": {"id": "name"}})).is_err());
        assert!(op_rename(
            json!({"path": src.to_str().unwrap(), "dst": out.to_str().unwrap(), "rename": {}})
        )
        .is_err());
        let _ = std::fs::remove_file(&csv);
        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn op_drop_keeps_every_column_except_the_named_ones() {
        let csv = unique_audit_path("drop.csv");
        std::fs::write(&csv, "id,name,score\n1,a,10\n2,b,20\n3,c,30\n").unwrap();
        let src = unique_audit_path("drop_src.parquet");
        op_from_csv(json!({
            "src": csv.to_str().unwrap(),
            "dst": src.to_str().unwrap(),
        }))
        .unwrap();
        let out = unique_audit_path("drop_out.parquet");
        let r = op_drop(json!({
            "path": src.to_str().unwrap(),
            "dst": out.to_str().unwrap(),
            "columns": ["name"],
        }))
        .unwrap();
        // Complement of select: every row, every column but `name`, file order.
        assert_eq!(r["rows"], json!(3), "all rows preserved");
        assert_eq!(
            r["columns"],
            json!(["id", "score"]),
            "name dropped, rest in file order"
        );
        let sch = op_schema(json!({"path": out.to_str().unwrap()})).unwrap();
        assert_eq!(sch["num_fields"], json!(2), "one column removed");
        // Dropping an unknown column errors rather than silently no-op.
        assert!(op_drop(json!({
            "path": src.to_str().unwrap(), "dst": out.to_str().unwrap(),
            "columns": ["nope"],
        }))
        .is_err());
        // Dropping every column is refused — an empty-schema parquet is useless.
        assert!(op_drop(json!({
            "path": src.to_str().unwrap(), "dst": out.to_str().unwrap(),
            "columns": ["id", "name", "score"],
        }))
        .is_err());
        let _ = std::fs::remove_file(&csv);
        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&out);
    }

    // ── validate / column_chunk_stats / sample / features ────────────────────

    /// Write `rows` int64 rows into a parquet with row groups capped at
    /// `rg_size`, chunk statistics on — exercises the multi-row-group paths.
    fn write_rg_parquet(path: &Path, rows: usize, rg_size: usize) {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::properties::EnabledStatistics;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rg_size))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let file = File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    #[test]
    fn op_validate_reports_ok_with_row_and_group_counts() {
        let path = unique_audit_path("validate.parquet");
        write_rg_parquet(&path, 5, 2); // 2 + 2 + 1 → 3 row groups
        let v = op_validate(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(v["ok"], json!(true), "clean file must validate ok");
        assert_eq!(v["rows"].as_u64().unwrap(), 5, "validate counts every row");
        assert_eq!(
            v["row_groups"].as_u64().unwrap(),
            3,
            "rg_size=2 over 5 rows → 3 groups"
        );
    }

    #[test]
    fn op_validate_reports_footer_failure_on_non_parquet() {
        // A truncated / non-parquet file must fail at the footer stage rather
        // than panic — op_validate catches the open error.
        let path = unique_audit_path("notparquet.parquet");
        std::fs::write(&path, b"this is not a parquet file").unwrap();
        let v = op_validate(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(v["ok"], json!(false), "garbage file must not validate");
        assert_eq!(
            v["stage"],
            json!("footer"),
            "failure surfaces at footer read"
        );
    }

    #[test]
    fn op_sample_offset_window_skips_and_caps() {
        // 5 rows id 0..4, sample offset=1 n=2 → ids 1,2 (skips row 0, stops
        // after 2). Boundary that head/tail can't express.
        let path = unique_audit_path("sample.parquet");
        write_rg_parquet(&path, 5, 2);
        let v = op_sample(json!({ "path": path.to_str().unwrap(), "offset": 1, "n": 2 })).unwrap();
        let _ = std::fs::remove_file(&path);
        let rows = v["rows"].as_array().unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![1, 2], "sample(offset=1,n=2) must return ids 1,2");
    }

    #[test]
    fn op_sample_offset_past_end_is_empty() {
        let path = unique_audit_path("sample-end.parquet");
        write_rg_parquet(&path, 3, 2);
        let v = op_sample(json!({ "path": path.to_str().unwrap(), "offset": 10, "n": 5 })).unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(
            v["rows"].as_array().unwrap().is_empty(),
            "offset beyond the file must yield no rows, not wrap"
        );
    }

    #[test]
    fn op_column_chunk_stats_exposes_per_group_chunk_detail() {
        let path = unique_audit_path("chunkstats.parquet");
        write_rg_parquet(&path, 5, 2); // 3 row groups
        let v = op_column_chunk_stats(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        let groups = v["row_groups"].as_array().unwrap();
        assert_eq!(groups.len(), 3, "one entry per row group");
        let c0 = &groups[0]["columns"][0];
        assert_eq!(c0["column"], json!("id"), "column name surfaced");
        assert!(
            c0["compressed_size"].as_i64().unwrap() > 0,
            "compressed_size must be a real footer value"
        );
        assert!(
            c0["encodings"]
                .as_array()
                .map(|e| !e.is_empty())
                .unwrap_or(false),
            "encodings list must be populated from the footer"
        );
        // First row group holds ids 0,1 → min 0, max 1 from chunk stats.
        assert_eq!(c0["min"].as_i64().unwrap(), 0, "first chunk min from stats");
        assert_eq!(c0["max"].as_i64().unwrap(), 1, "first chunk max from stats");
    }

    #[test]
    fn op_size_report_aggregates_footer_bytes_across_groups() {
        let path = unique_audit_path("sizereport.parquet");
        write_rg_parquet(&path, 5, 2); // single "id" column, 3 row groups
        let v = op_size_report(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            v["num_rows"].as_i64().unwrap(),
            5,
            "rows summed across groups"
        );
        let total = v["total_compressed_size"].as_i64().unwrap();
        assert!(total > 0, "compressed total is a real footer value");
        assert!(
            v["total_uncompressed_size"].as_i64().unwrap() > 0,
            "uncompressed total populated"
        );
        // Single column → its rollup equals the file totals.
        let cols = v["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 1, "one rolled-up column");
        assert_eq!(cols[0]["column"], json!("id"), "column name preserved");
        assert_eq!(
            cols[0]["compressed_size"].as_i64().unwrap(),
            total,
            "lone column's compressed bytes equal the file total"
        );
        // bytes_per_row = total_compressed / num_rows.
        let bpr = v["bytes_per_row"].as_f64().unwrap();
        assert!(
            (bpr - total as f64 / 5.0).abs() < 1e-9,
            "bytes_per_row derives from the compressed total"
        );
        assert!(
            v["compression_ratio"].as_f64().unwrap() > 0.0,
            "ratio computed from non-zero compressed total"
        );
    }

    #[test]
    fn op_null_summary_rolls_up_null_counts_from_the_footer() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::properties::EnabledStatistics;
        // `id` has no nulls; `label` has 2 of 5.
        let path = unique_audit_path("nullsummary.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let labels = StringArray::from(vec![Some("a"), None, Some("c"), None, Some("e")]);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(labels)])
                .unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let file = File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let v = op_null_summary(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(v["num_rows"].as_i64().unwrap(), 5);
        assert_eq!(
            v["total_nulls"].as_i64().unwrap(),
            2,
            "2 nulls across the file"
        );
        let cols = v["columns"].as_array().unwrap();
        let id = cols.iter().find(|c| c["column"] == "id").unwrap();
        let label = cols.iter().find(|c| c["column"] == "label").unwrap();
        assert_eq!(id["null_count"].as_i64().unwrap(), 0, "id has no nulls");
        assert_eq!(id["null_fraction"].as_f64().unwrap(), 0.0);
        assert_eq!(
            label["null_count"].as_i64().unwrap(),
            2,
            "label has 2 nulls"
        );
        assert!(
            (label["null_fraction"].as_f64().unwrap() - 0.4).abs() < 1e-9,
            "null_fraction = 2/5"
        );
    }

    #[test]
    fn op_encoding_summary_rolls_up_encodings_and_codec_per_column() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::basic::Compression;
        // 5 rows at row-group size 2 → 3 row groups; SNAPPY codec on every chunk.
        let path = unique_audit_path("encsummary.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let labels = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(labels)]).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let v = op_encoding_summary(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        let cols = v["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 2, "two columns");
        for c in cols {
            let encs = c["encodings"].as_array().unwrap();
            assert!(
                !encs.is_empty(),
                "every column reports at least one encoding"
            );
            // The codec is rolled up across all 3 row groups and deduped to one.
            assert_eq!(
                c["compression"],
                json!(["SNAPPY"]),
                "single deduped codec across row groups"
            );
            // Encodings come from a BTreeSet → sorted and free of duplicates.
            let names: Vec<&str> = encs.iter().map(|e| e.as_str().unwrap()).collect();
            let mut sorted = names.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(names, sorted, "encodings are sorted and deduped");
        }
    }

    #[test]
    fn op_bloom_filter_summary_detects_enabled_and_absent_filters() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let make_batch = || {
            let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
            let labels =
                StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]);
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(labels)])
                .unwrap()
        };

        // Bloom filters enabled on every column.
        let with_path = unique_audit_path("bloom_on.parquet");
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let file = File::create(&with_path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        w.write(&make_batch()).unwrap();
        w.close().unwrap();
        let v = op_bloom_filter_summary(json!({ "path": with_path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&with_path);
        assert_eq!(v["columns_total"].as_i64().unwrap(), 2);
        assert_eq!(
            v["columns_with_bloom_filter"].as_i64().unwrap(),
            2,
            "both columns carry a bloom filter"
        );
        for c in v["columns"].as_array().unwrap() {
            assert_eq!(c["has_bloom_filter"], json!(true));
            assert!(c["chunks_with_filter"].as_i64().unwrap() >= 1);
        }

        // Default writer: no bloom filters.
        let off_path = unique_audit_path("bloom_off.parquet");
        let file = File::create(&off_path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        w.write(&make_batch()).unwrap();
        w.close().unwrap();
        let v = op_bloom_filter_summary(json!({ "path": off_path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&off_path);
        assert_eq!(
            v["columns_with_bloom_filter"].as_i64().unwrap(),
            0,
            "none by default"
        );
        for c in v["columns"].as_array().unwrap() {
            assert_eq!(c["has_bloom_filter"], json!(false));
            assert_eq!(c["chunks_with_filter"].as_i64().unwrap(), 0);
        }
        // Missing path errors.
        assert!(op_bloom_filter_summary(json!({})).is_err());
    }

    #[test]
    fn op_sorting_columns_summary_reads_declared_sort_order() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::metadata::SortingColumn;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let make_batch = || {
            let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
            let labels =
                StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]);
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(labels)])
                .unwrap()
        };

        // Writer declares the data sorted by column 0 descending, nulls first.
        let on_path = unique_audit_path("sort_on.parquet");
        let props = WriterProperties::builder()
            .set_sorting_columns(Some(vec![SortingColumn {
                column_idx: 0,
                descending: true,
                nulls_first: true,
            }]))
            .build();
        let file = File::create(&on_path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        w.write(&make_batch()).unwrap();
        w.close().unwrap();
        let v = op_sorting_columns_summary(json!({ "path": on_path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&on_path);
        assert_eq!(v["has_sorting_columns"], json!(true));
        let sc = &v["row_groups"][0]["sorting_columns"][0];
        assert_eq!(sc["column"], json!("id"));
        assert_eq!(sc["column_idx"], json!(0));
        assert_eq!(sc["descending"], json!(true));
        assert_eq!(sc["nulls_first"], json!(true));

        // Default writer declares no sort order.
        let off_path = unique_audit_path("sort_off.parquet");
        let file = File::create(&off_path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        w.write(&make_batch()).unwrap();
        w.close().unwrap();
        let v = op_sorting_columns_summary(json!({ "path": off_path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&off_path);
        assert_eq!(v["has_sorting_columns"], json!(false));
        assert_eq!(
            v["row_groups"][0]["sorting_columns"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert!(op_sorting_columns_summary(json!({})).is_err());
    }

    #[test]
    fn op_row_group_summary_rolls_up_sizing_from_the_footer() {
        // 6 rows at row-group size 2 → 3 row groups of 2.
        let path = unique_audit_path("rgsummary.parquet");
        write_rg_parquet(&path, 6, 2);
        let v = op_row_group_summary(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(v["num_row_groups"], json!(3));
        assert_eq!(v["total_rows"], json!(6));
        // Evenly split → 2 rows per group, min == max == mean.
        assert_eq!(v["rows_per_group"]["min"], json!(2));
        assert_eq!(v["rows_per_group"]["max"], json!(2));
        assert_eq!(v["rows_per_group"]["mean"], json!(2.0));
        // Compressed-byte stats are present and consistent (min <= max, total > 0).
        let cb = &v["compressed_bytes_per_group"];
        assert!(cb["min"].as_i64().unwrap() <= cb["max"].as_i64().unwrap());
        assert!(v["total_compressed_size"].as_i64().unwrap() > 0);
        // An empty (0-row-group) file reports nulls, not a panic.
        let empty = unique_audit_path("rgempty.parquet");
        write_rg_parquet(&empty, 0, 1);
        let ev = op_row_group_summary(json!({ "path": empty.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&empty);
        assert_eq!(ev["num_row_groups"], json!(0));
        assert_eq!(ev["rows_per_group"], Value::Null);
    }

    #[test]
    fn op_features_reports_absent_indexes_for_plain_file() {
        // A vanilla ArrowWriter file has no bloom filter; presence flags must
        // be reported (here, false) rather than crash on the missing offsets.
        let path = unique_audit_path("features.parquet");
        write_rg_parquet(&path, 4, 2);
        let v = op_features(json!({ "path": path.to_str().unwrap() })).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            v["has_bloom_filter"],
            json!(false),
            "default writer emits no bloom filter"
        );
        assert!(
            v["columns"].as_array().unwrap().len() == 1,
            "one column reported"
        );
        let c = &v["columns"][0];
        assert_eq!(
            c["bloom_filter"],
            json!(false),
            "per-column bloom flag present"
        );
    }
}
