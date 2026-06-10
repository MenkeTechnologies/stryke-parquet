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
    v.as_array().map(|a| {
        a.iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect()
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
                if min == Value::Null {
                    min = mn;
                }
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
    // Read only the last row group for efficiency.
    let file = File::open(path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(8192);
    let num_groups = builder.metadata().num_row_groups();
    if num_groups > 0 {
        builder = builder.with_row_groups(vec![num_groups - 1]);
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
pub extern "C" fn parquet__mkdemo(args: *const c_char) -> *const c_char {
    ffi_call(args, op_mkdemo)
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

    #[test]
    fn parse_columns_empty_array_is_some_empty_not_none() {
        assert_eq!(parse_columns(&json!([])), Some(vec![]));
    }
}
