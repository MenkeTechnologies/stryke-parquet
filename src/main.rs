//! `stryke-parquet-helper` — Parquet file inspector.
//!
//! Sibling to stryke-arrow but with a diagnostic surface — read footers,
//! dump schemas, peek at rows, inspect per-row-group statistics. Output
//! is NDJSON (per-row / per-row-group commands) or a single JSON object
//! (inspect / schema / count / stats).

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchReader;
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_json::writer::{LineDelimited, WriterBuilder as JsonWriterBuilder};
use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Parser, Debug)]
#[command(
    name = "stryke-parquet-helper",
    version,
    about = "Parquet file inspector for the stryke `parquet` package"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// One-line summary: rows, row groups, columns, total compressed size,
    /// dominant compression, writer version.
    Inspect { path: PathBuf },

    /// Full schema as JSON (parquet types, repetition, logical type).
    Schema { path: PathBuf },

    /// Stream the first N rows as NDJSON.
    Head {
        path: PathBuf,
        #[arg(long, default_value_t = 10)]
        n: usize,
        /// Comma-separated column projection.
        #[arg(long)]
        columns: Option<String>,
    },

    /// Stream the last N rows as NDJSON (scans only the trailing row group).
    Tail {
        path: PathBuf,
        #[arg(long, default_value_t = 10)]
        n: usize,
        #[arg(long)]
        columns: Option<String>,
    },

    /// Per-row-group breakdown: rows, size, compressions, column chunks.
    Rowgroups { path: PathBuf },

    /// Per-column stats aggregated across row groups (footer-only, fast).
    Stats {
        path: PathBuf,
        /// Single column to focus on (default: all columns).
        #[arg(long)]
        column: Option<String>,
    },

    /// Just print the row count (from footer, no data scan).
    Count { path: PathBuf },

    /// Stream every row as NDJSON.
    ToJson {
        path: PathBuf,
        #[arg(long)]
        columns: Option<String>,
    },

    /// Stream every row as CSV.
    ToCsv {
        path: PathBuf,
        /// `-` for stdout (default), or a file path.
        #[arg(long, default_value = "-")]
        output: String,
        #[arg(long)]
        columns: Option<String>,
    },

    /// Read a parquet and rewrite it with a different compression / row-group size.
    Compress {
        src: PathBuf,
        dst: PathBuf,
        #[arg(long, default_value = "zstd")]
        codec: String,
        #[arg(long)]
        row_group: Option<usize>,
    },

    /// Write a small demo parquet file at PATH. Used by tests and `--help`
    /// examples; not part of the production API.
    Mkdemo { path: PathBuf },
}

fn main() {
    if let Err(e) = run() {
        eprintln!("stryke-parquet-helper: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Inspect { path } => cmd_inspect(&path),
        Cmd::Schema { path } => cmd_schema(&path),
        Cmd::Head { path, n, columns } => cmd_head(&path, n, columns.as_deref()),
        Cmd::Tail { path, n, columns } => cmd_tail(&path, n, columns.as_deref()),
        Cmd::Rowgroups { path } => cmd_rowgroups(&path),
        Cmd::Stats { path, column } => cmd_stats(&path, column.as_deref()),
        Cmd::Count { path } => cmd_count(&path),
        Cmd::ToJson { path, columns } => cmd_head(&path, usize::MAX, columns.as_deref()),
        Cmd::ToCsv { path, output, columns } => {
            cmd_to_csv(&path, &output, columns.as_deref())
        }
        Cmd::Compress { src, dst, codec, row_group } => {
            cmd_compress(&src, &dst, &codec, row_group)
        }
        Cmd::Mkdemo { path } => cmd_mkdemo(&path),
    }
}

fn cmd_mkdemo(path: &Path) -> Result<()> {
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let names = Arc::new(StringArray::from(vec![
        "alice", "bob", "charlie", "dana", "eve",
    ]));
    let scores = Arc::new(Float64Array::from(vec![
        Some(1.5),
        Some(2.0),
        Some(3.25),
        None,
        Some(5.5),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![ids, names, scores],
    )?;
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    emit_json(&json!({
        "path": path.display().to_string(),
        "num_rows": 5,
        "num_columns": 3,
    }))
}

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn emit_ndjson<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/* ------------------------------------------------------------------------- */
/* inspect / count / schema / rowgroups / stats — all footer-only            */
/* ------------------------------------------------------------------------- */

fn open_reader(path: &Path) -> Result<SerializedFileReader<File>> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    SerializedFileReader::new(f).context("reading parquet footer")
}

fn cmd_count(path: &Path) -> Result<()> {
    let r = open_reader(path)?;
    let n = r.metadata().file_metadata().num_rows();
    emit_json(&json!({ "path": path.display().to_string(), "num_rows": n }))
}

fn cmd_inspect(path: &Path) -> Result<()> {
    let r = open_reader(path)?;
    let meta = r.metadata();
    let file_meta = meta.file_metadata();
    let num_rows = file_meta.num_rows();
    let num_row_groups = meta.num_row_groups();
    let schema_descr = file_meta.schema_descr();
    let num_columns = schema_descr.num_columns();
    let total_compressed_size: i64 = (0..num_row_groups)
        .map(|i| meta.row_group(i).compressed_size())
        .sum();
    let total_uncompressed_size: i64 = (0..num_row_groups)
        .map(|i| meta.row_group(i).total_byte_size())
        .sum();
    let mut compression_counts: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    for rg in 0..num_row_groups {
        let rgm = meta.row_group(rg);
        for c in 0..rgm.num_columns() {
            let code = format!("{:?}", rgm.column(c).compression()).to_lowercase();
            *compression_counts.entry(code).or_insert(0) += 1;
        }
    }
    emit_json(&json!({
        "path": path.display().to_string(),
        "file_size": file_size(path),
        "num_rows": num_rows,
        "num_row_groups": num_row_groups,
        "num_columns": num_columns,
        "total_compressed_size": total_compressed_size,
        "total_uncompressed_size": total_uncompressed_size,
        "compression_ratio": if total_uncompressed_size > 0 {
            Some((total_compressed_size as f64) / (total_uncompressed_size as f64))
        } else { None },
        "compressions": compression_counts,
        "created_by": file_meta.created_by(),
        "version": file_meta.version(),
    }))
}

fn cmd_schema(path: &Path) -> Result<()> {
    let r = open_reader(path)?;
    let file_meta = r.metadata().file_metadata();
    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        file_meta.schema_descr(),
        file_meta.key_value_metadata(),
    )?;
    emit_json(&schema_to_json(&arrow_schema))
}

fn cmd_rowgroups(path: &Path) -> Result<()> {
    let r = open_reader(path)?;
    let meta = r.metadata();
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for i in 0..meta.num_row_groups() {
        let rg = meta.row_group(i);
        let cols: Vec<Value> = (0..rg.num_columns())
            .map(|c| {
                let col = rg.column(c);
                json!({
                    "column": col.column_path().string(),
                    "type": format!("{:?}", col.column_type()).to_lowercase(),
                    "compression": format!("{:?}", col.compression()).to_lowercase(),
                    "encodings": col.encodings().map(|e| format!("{e:?}").to_lowercase()).collect::<Vec<_>>(),
                    "num_values": col.num_values(),
                    "compressed_size": col.compressed_size(),
                    "uncompressed_size": col.uncompressed_size(),
                    "data_page_offset": col.data_page_offset(),
                    "dictionary_page_offset": col.dictionary_page_offset(),
                    "has_index_page": col.column_index_offset().is_some(),
                })
            })
            .collect();
        emit_ndjson(
            &mut out,
            &json!({
                "ordinal": i,
                "num_rows": rg.num_rows(),
                "total_byte_size": rg.total_byte_size(),
                "total_compressed_size": rg.compressed_size(),
                "columns": cols,
            }),
        )?;
    }
    Ok(())
}

fn cmd_stats(path: &Path, column_filter: Option<&str>) -> Result<()> {
    let r = open_reader(path)?;
    let meta = r.metadata();
    let file_meta = meta.file_metadata();
    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        file_meta.schema_descr(),
        file_meta.key_value_metadata(),
    )?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
        if let Some(want) = column_filter {
            if field.name() != want {
                continue;
            }
        }
        let mut null_count: i64 = 0;
        let mut distinct_count: Option<i64> = None;
        let mut overall_min: Option<Value> = None;
        let mut overall_max: Option<Value> = None;
        for rg in 0..meta.num_row_groups() {
            let rgm = meta.row_group(rg);
            if col_idx >= rgm.num_columns() {
                continue;
            }
            let col = rgm.column(col_idx);
            if let Some(s) = col.statistics() {
                null_count += stats_null_count(s) as i64;
                if let Some(d) = s.distinct_count_opt() {
                    distinct_count = Some(distinct_count.unwrap_or(0) + d as i64);
                }
                if let Some(min) = stats_min_value(s) {
                    overall_min = Some(merge_min(overall_min.take(), min));
                }
                if let Some(max) = stats_max_value(s) {
                    overall_max = Some(merge_max(overall_max.take(), max));
                }
            }
        }
        emit_ndjson(
            &mut out,
            &json!({
                "name": field.name(),
                "type": data_type_label(field.data_type()),
                "nullable": field.is_nullable(),
                "null_count": null_count,
                "distinct_count": distinct_count,
                "min": overall_min,
                "max": overall_max,
            }),
        )?;
    }
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* head / tail / to-json / to-csv — needs arrow                              */
/* ------------------------------------------------------------------------- */

fn parse_columns(s: Option<&str>) -> Option<Vec<String>> {
    s.map(|spec| {
        spec.split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect()
    })
}

fn open_batches(
    path: &Path,
    columns: Option<&[String]>,
    batch_size: usize,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    let file = File::open(path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(batch_size);
    if let Some(cols) = columns {
        let schema = builder.parquet_schema();
        let mask =
            parquet::arrow::ProjectionMask::columns(schema, cols.iter().map(|s| s.as_str()));
        builder = builder.with_projection(mask);
    }
    Ok(Box::new(builder.build()?))
}

fn cmd_head(path: &Path, n: usize, columns: Option<&str>) -> Result<()> {
    let cols = parse_columns(columns);
    let mut reader = open_batches(path, cols.as_deref(), 8192)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut writer = JsonWriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(&mut out);
    let mut emitted: usize = 0;
    while let Some(batch) = reader.next() {
        let mut batch = batch?;
        let remaining = n.saturating_sub(emitted);
        if remaining == 0 {
            break;
        }
        if batch.num_rows() > remaining {
            batch = batch.slice(0, remaining);
        }
        writer.write(&batch)?;
        emitted += batch.num_rows();
        if emitted >= n {
            break;
        }
    }
    writer.finish()?;
    Ok(())
}

fn cmd_tail(path: &Path, n: usize, columns: Option<&str>) -> Result<()> {
    let r = open_reader(path)?;
    let meta = r.metadata();
    let total = meta.file_metadata().num_rows() as usize;
    if total == 0 {
        return Ok(());
    }
    let skip = total.saturating_sub(n);

    let cols = parse_columns(columns);
    let mut reader = open_batches(path, cols.as_deref(), 8192)?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut writer = JsonWriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(&mut out);
    let mut seen: usize = 0;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        let batch_rows = batch.num_rows();
        let batch_start = seen;
        let batch_end = seen + batch_rows;
        seen = batch_end;
        if batch_end <= skip {
            continue;
        }
        let local_start = if skip > batch_start {
            skip - batch_start
        } else {
            0
        };
        let slice = batch.slice(local_start, batch_rows - local_start);
        writer.write(&slice)?;
    }
    writer.finish()?;
    Ok(())
}

fn cmd_to_csv(path: &Path, output: &str, columns: Option<&str>) -> Result<()> {
    let cols = parse_columns(columns);
    let mut reader = open_batches(path, cols.as_deref(), 8192)?;
    let writer: Box<dyn Write> = if output == "-" {
        let stdout = io::stdout();
        Box::new(BufWriter::new(stdout.lock()))
    } else {
        Box::new(BufWriter::new(File::create(output)?))
    };
    let mut csv_writer = CsvWriterBuilder::new().with_header(true).build(writer);
    while let Some(batch) = reader.next() {
        let batch = batch?;
        csv_writer.write(&batch)?;
    }
    Ok(())
}

fn cmd_compress(
    src: &Path,
    dst: &Path,
    codec: &str,
    row_group: Option<usize>,
) -> Result<()> {
    let mut reader = open_batches(src, None, 8192)?;
    let schema = reader.schema();
    let compression = parse_compression(codec)?;
    let mut props = WriterProperties::builder().set_compression(compression);
    if let Some(rg) = row_group {
        props = props.set_max_row_group_row_count(Some(rg));
    }
    let file = File::create(dst)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props.build()))?;
    let mut rows: i64 = 0;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        rows += batch.num_rows() as i64;
        writer.write(&batch)?;
    }
    writer.close()?;
    emit_json(&json!({
        "src": src.display().to_string(),
        "dst": dst.display().to_string(),
        "codec": codec,
        "num_rows": rows,
        "src_size": file_size(src),
        "dst_size": file_size(dst),
    }))
}

fn parse_compression(name: &str) -> Result<Compression> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "gzip" | "gz" => Compression::GZIP(Default::default()),
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "lz4" | "lz4_raw" => Compression::LZ4_RAW,
        "brotli" => Compression::BROTLI(Default::default()),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => anyhow::bail!("unknown codec `{other}`"),
    })
}

/* ------------------------------------------------------------------------- */
/* schema → JSON                                                             */
/* ------------------------------------------------------------------------- */

#[derive(Serialize)]
struct FieldJson {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    nullable: bool,
}

fn schema_to_json(schema: &Schema) -> Value {
    let fields: Vec<FieldJson> = schema
        .fields()
        .iter()
        .map(|f| FieldJson {
            name: f.name().clone(),
            ty: data_type_label(f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();
    let _ = Field::new("x", DataType::Int32, true); // anchor unused import
    let _ = RecordBatch::try_new_with_options;
    let _: SchemaRef = std::sync::Arc::new(Schema::empty());
    json!({
        "fields": fields,
        "num_fields": schema.fields().len(),
    })
}

fn data_type_label(t: &DataType) -> String {
    match t {
        DataType::Null => "null".into(),
        DataType::Boolean => "bool".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float16 => "float16".into(),
        DataType::Float32 => "float32".into(),
        DataType::Float64 => "float64".into(),
        DataType::Utf8 => "string".into(),
        DataType::LargeUtf8 => "large_string".into(),
        DataType::Binary => "binary".into(),
        DataType::LargeBinary => "large_binary".into(),
        DataType::Date32 => "date32".into(),
        DataType::Date64 => "date64".into(),
        DataType::Timestamp(u, tz) => match tz {
            Some(tz) => format!("timestamp({u:?},{tz})").to_lowercase(),
            None => format!("timestamp({u:?})").to_lowercase(),
        },
        DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
        DataType::Decimal256(p, s) => format!("decimal256({p},{s})"),
        DataType::List(f) => format!("list<{}>", data_type_label(f.data_type())),
        DataType::Struct(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), data_type_label(f.data_type())))
                .collect();
            format!("struct<{}>", inner.join(","))
        }
        other => format!("{other:?}").to_lowercase(),
    }
}

/* ------------------------------------------------------------------------- */
/* stats helpers                                                             */
/* ------------------------------------------------------------------------- */

fn stats_null_count(s: &Statistics) -> u64 {
    s.null_count_opt().unwrap_or(0)
}

fn stats_min_value(s: &Statistics) -> Option<Value> {
    match s {
        Statistics::Boolean(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Int32(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Int64(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Float(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Double(v) => v.min_opt().map(|x| json!(x)),
        Statistics::ByteArray(v) => v.min_opt().map(|x| json!(String::from_utf8_lossy(x.data()))),
        Statistics::FixedLenByteArray(v) => {
            v.min_opt().map(|x| json!(String::from_utf8_lossy(x.data())))
        }
        Statistics::Int96(_) => None,
    }
}

fn stats_max_value(s: &Statistics) -> Option<Value> {
    match s {
        Statistics::Boolean(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Int32(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Int64(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Float(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Double(v) => v.max_opt().map(|x| json!(x)),
        Statistics::ByteArray(v) => v.max_opt().map(|x| json!(String::from_utf8_lossy(x.data()))),
        Statistics::FixedLenByteArray(v) => {
            v.max_opt().map(|x| json!(String::from_utf8_lossy(x.data())))
        }
        Statistics::Int96(_) => None,
    }
}

fn merge_min(prev: Option<Value>, cand: Value) -> Value {
    match prev {
        None => cand,
        Some(p) => {
            if compare_values(&cand, &p) == std::cmp::Ordering::Less {
                cand
            } else {
                p
            }
        }
    }
}

fn merge_max(prev: Option<Value>, cand: Value) -> Value {
    match prev {
        None => cand,
        Some(p) => {
            if compare_values(&cand, &p) == std::cmp::Ordering::Greater {
                cand
            } else {
                p
            }
        }
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => xf.partial_cmp(&yf).unwrap_or(Equal),
            _ => Equal,
        },
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => Equal,
    }
}

#[allow(dead_code)]
fn _force_anyhow_link() -> anyhow::Error {
    anyhow!("unused")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // ─── parse_columns ───────────────────────────────────────────────

    #[test]
    fn parse_columns_none_passthrough() {
        assert_eq!(parse_columns(None), None);
    }

    #[test]
    fn parse_columns_splits_and_trims() {
        assert_eq!(
            parse_columns(Some(" a , b , c ")).unwrap(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn parse_columns_filters_empty_segments() {
        assert_eq!(
            parse_columns(Some("a,,b,")).unwrap(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn parse_columns_empty_string_returns_empty_vec() {
        assert_eq!(parse_columns(Some("")), Some(Vec::<String>::new()));
    }

    // ─── parse_compression ───────────────────────────────────────────

    #[test]
    fn parse_compression_known_codecs() {
        assert_eq!(parse_compression("snappy").unwrap(), Compression::SNAPPY);
        assert_eq!(parse_compression("none").unwrap(), Compression::UNCOMPRESSED);
        assert_eq!(parse_compression("uncompressed").unwrap(), Compression::UNCOMPRESSED);
        assert_eq!(parse_compression("lz4").unwrap(), Compression::LZ4_RAW);
        assert_eq!(parse_compression("lz4_raw").unwrap(), Compression::LZ4_RAW);
        // gz alias for gzip.
        assert!(matches!(parse_compression("gz").unwrap(), Compression::GZIP(_)));
        assert!(matches!(parse_compression("zstd").unwrap(), Compression::ZSTD(_)));
        assert!(matches!(parse_compression("brotli").unwrap(), Compression::BROTLI(_)));
    }

    #[test]
    fn parse_compression_case_insensitive() {
        assert_eq!(parse_compression("SNAPPY").unwrap(), Compression::SNAPPY);
        assert_eq!(parse_compression("Zstd").unwrap_or(Compression::SNAPPY), parse_compression("zstd").unwrap_or(Compression::SNAPPY));
    }

    #[test]
    fn parse_compression_unknown_errors() {
        let err = parse_compression("avro").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown codec"));
        assert!(msg.contains("avro"));
    }

    // ─── data_type_label ─────────────────────────────────────────────

    #[test]
    fn data_type_label_scalars() {
        assert_eq!(data_type_label(&DataType::Boolean), "bool");
        assert_eq!(data_type_label(&DataType::Int32), "int32");
        assert_eq!(data_type_label(&DataType::Int64), "int64");
        assert_eq!(data_type_label(&DataType::UInt8), "uint8");
        assert_eq!(data_type_label(&DataType::Float64), "float64");
        assert_eq!(data_type_label(&DataType::Utf8), "string");
        assert_eq!(data_type_label(&DataType::LargeUtf8), "large_string");
        assert_eq!(data_type_label(&DataType::Null), "null");
        assert_eq!(data_type_label(&DataType::Date32), "date32");
        assert_eq!(data_type_label(&DataType::Binary), "binary");
        assert_eq!(data_type_label(&DataType::LargeBinary), "large_binary");
    }

    #[test]
    fn data_type_label_decimals() {
        assert_eq!(data_type_label(&DataType::Decimal128(18, 6)), "decimal128(18,6)");
        assert_eq!(data_type_label(&DataType::Decimal256(38, 10)), "decimal256(38,10)");
    }

    #[test]
    fn data_type_label_list_nests() {
        let inner = Field::new("item", DataType::Int32, true);
        assert_eq!(data_type_label(&DataType::List(Arc::new(inner))), "list<int32>");
    }

    #[test]
    fn data_type_label_struct_concatenates() {
        let dt = DataType::Struct(
            vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(data_type_label(&dt), "struct<x:int32,y:string>");
    }

    // ─── compare_values / merge_min / merge_max ──────────────────────

    #[test]
    fn compare_values_numbers_via_f64() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(1), &json!(2)), Less);
        assert_eq!(compare_values(&json!(2.5), &json!(2.5)), Equal);
        assert_eq!(compare_values(&json!(10), &json!(2)), Greater);
    }

    #[test]
    fn compare_values_mixed_types_return_equal() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(1), &json!("a")), Equal);
        assert_eq!(compare_values(&Value::Null, &json!(1)), Equal);
    }

    #[test]
    fn merge_min_picks_smaller_or_base_when_none() {
        assert_eq!(merge_min(None, json!(5)), json!(5));
        assert_eq!(merge_min(Some(json!(10)), json!(5)), json!(5));
        assert_eq!(merge_min(Some(json!(2)), json!(7)), json!(2));
    }

    #[test]
    fn merge_max_picks_larger_or_base_when_none() {
        assert_eq!(merge_max(None, json!(5)), json!(5));
        assert_eq!(merge_max(Some(json!(10)), json!(5)), json!(10));
        assert_eq!(merge_max(Some(json!(2)), json!(7)), json!(7));
    }

    #[test]
    fn merge_min_max_strings_lex_order() {
        assert_eq!(merge_min(Some(json!("zebra")), json!("apple")), json!("apple"));
        assert_eq!(merge_max(Some(json!("apple")), json!("zebra")), json!("zebra"));
    }

    // ─── emit_ndjson ─────────────────────────────────────────────────

    #[test]
    fn emit_ndjson_appends_newline() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({"k": 1})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"k\":1}\n");
    }

    #[test]
    fn emit_ndjson_multi_call_line_count() {
        let mut buf = Vec::new();
        for i in 0..4 {
            emit_ndjson(&mut buf, &json!({"i": i})).unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap().lines().count(), 4);
    }

    // ─── file_size ───────────────────────────────────────────────────

    #[test]
    fn file_size_returns_metadata_len() {
        let tmp = std::env::temp_dir()
            .join(format!("stryke-parquet-test-{}.bin", std::process::id()));
        std::fs::write(&tmp, b"abcdefghij").unwrap();
        let got = file_size(&tmp);
        let _ = std::fs::remove_file(&tmp);
        assert_eq!(got, 10);
    }

    #[test]
    fn file_size_missing_path_returns_zero() {
        // Defensive: missing file → 0 (not a panic, not an error).
        let nope = Path::new("/definitely/not/a/real/path/abc.parquet");
        assert_eq!(file_size(nope), 0);
    }

    // ─── schema_to_json (smoke) ──────────────────────────────────────

    #[test]
    fn schema_to_json_emits_fields_with_num_fields() {
        let s = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let j = schema_to_json(&s);
        assert!(j["fields"].is_array());
        assert_eq!(j["fields"].as_array().unwrap().len(), 2);
        assert_eq!(j["num_fields"], json!(2));
        assert_eq!(j["fields"][0]["name"], json!("a"));
        assert_eq!(j["fields"][0]["type"], json!("int32"));
        assert_eq!(j["fields"][0]["nullable"], json!(false));
        assert_eq!(j["fields"][1]["nullable"], json!(true));
    }

    #[test]
    fn compare_values_string_lex_order() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!("a"), &json!("b")), Less);
        assert_eq!(compare_values(&json!("z"), &json!("a")), Greater);
    }

    #[test]
    fn compare_values_bool_ordering() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(false), &json!(true)), Less);
    }

    #[test]
    fn parse_compression_bzip2_unsupported() {
        let err = parse_compression("bzip2").unwrap_err();
        assert!(format!("{err}").contains("unknown codec"));
    }

    #[test]
    fn data_type_label_fixed_size_list_falls_back_to_debug() {
        let inner = Field::new("item", DataType::Int32, true);
        let dt = DataType::FixedSizeList(Arc::new(inner), 4);
        let label = data_type_label(&dt);
        assert!(label.contains("fixedsizelist"), "label = {label}");
        assert!(label.contains("4"));
    }

    #[test]
    fn parse_columns_single_column_no_commas() {
        assert_eq!(parse_columns(Some("only")).unwrap(), vec!["only"]);
    }

    #[test]
    fn merge_min_max_equal_candidates_keep_first() {
        assert_eq!(merge_min(Some(json!(5)), json!(5)), json!(5));
        assert_eq!(merge_max(Some(json!(5)), json!(5)), json!(5));
    }

    #[test]
    fn emit_ndjson_empty_object() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{}\n");
    }

    #[test]
    fn data_type_label_timestamp_with_tz() {
        use arrow::datatypes::TimeUnit;
        let dt = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
        let label = data_type_label(&dt);
        assert!(label.contains("timestamp"));
        assert!(label.contains("utc"));
    }

    #[test]
    fn parse_compression_gzip_alias() {
        assert!(matches!(parse_compression("gzip").unwrap(), Compression::GZIP(_)));
        assert!(matches!(parse_compression("gz").unwrap(), Compression::GZIP(_)));
    }

    #[test]
    fn compare_values_null_equal() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&Value::Null, &Value::Null), Equal);
    }

    #[test]
    fn schema_to_json_single_field() {
        let s = Schema::new(vec![Field::new("only", DataType::Int32, true)]);
        let j = schema_to_json(&s);
        assert_eq!(j["num_fields"], 1);
        assert_eq!(j["fields"][0]["name"], json!("only"));
    }

    #[test]
    fn parse_columns_only_commas_returns_empty() {
        assert_eq!(parse_columns(Some(",,,")).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn merge_min_none_takes_candidate() {
        assert_eq!(merge_min(None, json!("z")), json!("z"));
    }

    #[test]
    fn emit_ndjson_array_top_level() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!([1, 2])).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "[1,2]\n");
    }

    #[test]
    fn data_type_label_uint32() {
        assert_eq!(data_type_label(&DataType::UInt32), "uint32");
    }

    #[test]
    fn parse_compression_snappy_case_insensitive() {
        assert_eq!(parse_compression("Snappy").unwrap(), Compression::SNAPPY);
    }

    #[test]
    fn merge_min_string_when_prev_none() {
        assert_eq!(merge_min(None, json!("first")), json!("first"));
    }

    #[test]
    fn compare_values_number_u64_in_json() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(100u64), &json!(200u64)), Less);
    }

    #[test]
    fn schema_to_json_empty_schema() {
        let s = Schema::empty();
        let j = schema_to_json(&s);
        assert_eq!(j["num_fields"], 0);
        assert!(j["fields"].as_array().unwrap().is_empty());
    }

    #[test]
    fn parse_columns_leading_trailing_commas() {
        assert_eq!(parse_columns(Some(",a,b,")).unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn emit_ndjson_number() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!(99)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "99\n");
    }

    #[test]
    fn data_type_label_int8() {
        assert_eq!(data_type_label(&DataType::Int8), "int8");
    }

    #[test]
    fn file_size_zero_for_empty_file() {
        let tmp = std::env::temp_dir().join(format!("stryke-parquet-empty-{}", std::process::id()));
        std::fs::write(&tmp, b"").unwrap();
        assert_eq!(file_size(&tmp), 0);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn data_type_label_float32() {
        assert_eq!(data_type_label(&DataType::Float32), "float32");
    }

    #[test]
    fn parse_compression_lz4_alias() {
        assert_eq!(parse_compression("lz4").unwrap(), Compression::LZ4_RAW);
    }

    #[test]
    fn merge_max_string_when_prev_none() {
        assert_eq!(merge_max(None, json!("z")), json!("z"));
    }

    #[test]
    fn merge_max_equal_candidates_keeps_prev() {
        assert_eq!(merge_max(Some(json!(5)), json!(5)), json!(5));
    }

    #[test]
    fn parse_columns_single_name() {
        assert_eq!(parse_columns(Some("only")).unwrap(), vec!["only"]);
    }

    #[test]
    fn emit_ndjson_bool_false() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!(false)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "false\n");
    }

    #[test]
    fn data_type_label_large_binary() {
        assert_eq!(data_type_label(&DataType::LargeBinary), "large_binary");
    }

    #[test]
    fn compare_values_equal_strings() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!("x"), &json!("x")), Equal);
    }

    #[test]
    fn data_type_label_uint64() {
        assert_eq!(data_type_label(&DataType::UInt64), "uint64");
    }

    #[test]
    fn parse_compression_uncompressed_aliases() {
        assert_eq!(parse_compression("none").unwrap(), Compression::UNCOMPRESSED);
        assert_eq!(parse_compression("uncompressed").unwrap(), Compression::UNCOMPRESSED);
    }

    #[test]
    fn merge_min_string_lex() {
        assert_eq!(merge_min(Some(json!("z")), json!("a")), json!("a"));
    }

    #[test]
    fn parse_columns_none_returns_none() {
        assert!(parse_columns(None).is_none());
    }

    #[test]
    fn emit_ndjson_string() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!("x")).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "\"x\"\n");
    }

    #[test]
    fn data_type_label_date32() {
        assert_eq!(data_type_label(&DataType::Date32), "date32");
    }

    #[test]
    fn compare_values_object_vs_object_equal() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!({"a": 1}), &json!({"a": 1})), Equal);
    }

    #[test]
    fn merge_max_number_candidates() {
        assert_eq!(merge_max(Some(json!(1)), json!(9)), json!(9));
    }

    #[test]
    fn data_type_label_int64() {
        assert_eq!(data_type_label(&DataType::Int64), "int64");
    }

    #[test]
    fn parse_compression_zstd_alias() {
        assert!(matches!(parse_compression("zstd").unwrap(), Compression::ZSTD(_)));
    }

    #[test]
    fn merge_min_number_candidates() {
        assert_eq!(merge_min(Some(json!(10)), json!(3)), json!(3));
    }

    #[test]
    fn parse_columns_two_names() {
        assert_eq!(parse_columns(Some("a,b")).unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn emit_ndjson_null() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &Value::Null).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "null\n");
    }

    #[test]
    fn data_type_label_float64() {
        assert_eq!(data_type_label(&DataType::Float64), "float64");
    }

    #[test]
    fn compare_values_string_greater() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!("z"), &json!("a")), Greater);
    }

    #[test]
    fn merge_max_none_takes_first() {
        assert_eq!(merge_max(None, json!(1)), json!(1));
    }

    #[test]
    fn data_type_label_int32_type() {
        assert_eq!(data_type_label(&DataType::Int32), "int32");
    }

    #[test]
    fn parse_compression_uncompressed() {
        assert!(matches!(
            parse_compression("uncompressed").unwrap(),
            Compression::UNCOMPRESSED,
        ));
    }

    #[test]
    fn merge_min_equal_keeps_first() {
        assert_eq!(merge_min(Some(json!(3)), json!(3)), json!(3));
    }

    #[test]
    fn parse_columns_trailing_comma_ignored() {
        let cols = parse_columns(Some("a,b,")).unwrap();
        assert_eq!(cols, vec!["a", "b"]);
    }

    #[test]
    fn emit_ndjson_bool_true() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!(true)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "true\n");
    }

    #[test]
    fn data_type_label_utf8() {
        assert_eq!(data_type_label(&DataType::Utf8), "string");
    }

    #[test]
    fn compare_values_bool_false_less_than_true() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(false), &json!(true)), Less);
    }

    #[test]
    fn merge_max_string_lex() {
        assert_eq!(merge_max(Some(json!("a")), json!("z")), json!("z"));
    }
}
