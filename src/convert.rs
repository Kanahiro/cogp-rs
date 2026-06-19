use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, LargeBinaryArray, RecordBatch, StringArray,
    StructArray, UInt32Array,
};
use arrow::compute::{cast, concat_batches, rank, take, SortOptions};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use clap::Args;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::thread;

use crate::meta::{
    BboxCovering, CogpMeta, Covering, GeoColumn, GeoMeta, Level, COGP_METADATA_KEY, COGP_VERSION,
    GEOPARQUET_VERSION, GEO_METADATA_KEY,
};
use crate::wkb_bbox::{bbox_from_wkb, kind_from_wkb, Bbox, GeomKind};

#[derive(Args)]
pub struct ConvertArgs {
    /// Input GeoParquet 1.x file
    pub input: PathBuf,
    /// Output COGP file
    pub output: PathBuf,
    /// Comma-separated GSD list, meters, coarse to fine (e.g. 1000,500,100,50).
    /// Projection-agnostic: each value is the ground sample distance in meters
    /// at which a level becomes meaningful. If omitted, GSDs are auto-derived
    /// from --webmerc-minzoom..=--webmerc-maxzoom assuming a Web Mercator tile
    /// pyramid (see --webmerc-minzoom/--webmerc-maxzoom/--webmerc-resolution).
    /// Pass --gsd directly if you target a non-Web-Mercator renderer.
    #[arg(long, value_delimiter = ',', num_args = 1.., conflicts_with_all = ["webmerc_minzoom", "webmerc_maxzoom"])]
    pub gsd: Vec<f64>,
    /// Coarsest Web Mercator zoom level for GSD auto-derivation. Used only
    /// when --gsd is omitted. Assumes the consumer renders on a Web Mercator
    /// (EPSG:3857) tile pyramid; for other projections, supply --gsd.
    #[arg(long, default_value_t = 0)]
    pub webmerc_minzoom: u32,
    /// Finest Web Mercator zoom level for GSD auto-derivation. Used only
    /// when --gsd is omitted. Same Web Mercator assumption as --webmerc-minzoom.
    #[arg(long, default_value_t = 16)]
    pub webmerc_maxzoom: u32,
    /// Parquet row group size in rows
    #[arg(long, default_value_t = 10000)]
    pub row_group_size: usize,
    /// Maximum estimated encoded Parquet row group size in bytes
    #[arg(long)]
    pub row_group_max_bytes: Option<usize>,
    /// Coordinate units in the input file. `auto` (default) inspects the GeoParquet
    /// `crs` PROJJSON: `ProjectedCRS` → meters, otherwise degrees. Override with
    /// `degrees` or `meters` if needed.
    #[arg(long, default_value = "auto")]
    pub input_units: InputUnits,
    /// Override auto-detected primary geometry column
    #[arg(long)]
    pub geometry_column: Option<String>,
    /// **Web Mercator only.** Base resolution per tile side (units) used to
    /// derive the level thinning grid when auto-deriving GSDs from
    /// --webmerc-minzoom/--webmerc-maxzoom. The level-i GSD is the ground
    /// distance covered by one base unit at zoom i, computed as
    /// `40_075_016 / (base · 2^i)` meters at the equator — i.e. it bakes in
    /// the Web Mercator equatorial circumference and the standard `2^z` tile
    /// pyramid. This controls *thinning* granularity, not output coordinate
    /// precision. The default of 1024 keeps the thinning grid at ~4× the
    /// typical 256-pixel tile resolution, so features collapsing within a
    /// few subpixels are dropped. Ignored when --gsd is given (in that case
    /// the GSDs are taken verbatim and no projection is assumed).
    #[arg(long, default_value_t = 1024)]
    pub webmerc_resolution: u32,
    /// Point-like features (WKB Point / MultiPoint) use a thinning grid this
    /// many times coarser than `prec` per axis, yielding ~factor² fewer
    /// points per level than polygons. Compensates for the fact that polygons
    /// span multiple cells visually while points occupy one, so equal grid
    /// density looks too dense for points. Set to `1` to disable.
    #[arg(long, default_value_t = 4)]
    pub point_thinning_factor: u32,
    /// LineString-like features (WKB LineString / MultiLineString) use a
    /// thinning grid this many times coarser than `prec` per axis. Lines
    /// are 1D so multiple parallel/near-parallel lines within `prec` overlap
    /// visually even when their bbox centers fall into distinct cells; this
    /// factor compensates. Smaller than `--point-thinning-factor` because
    /// lines still span many cells along their length. Set to `1` to disable.
    #[arg(long, default_value_t = 2)]
    pub line_thinning_factor: u32,
    /// Polygon-like features (WKB Polygon / MultiPolygon) use a thinning grid
    /// this many times coarser than `prec` per axis. Polygons span area so
    /// `1` (the default) already looks well-covered; raise to thin further.
    #[arg(long, default_value_t = 1)]
    pub polygon_thinning_factor: u32,
    /// Line visibility threshold multiplier applied to `prec` when deciding
    /// the coarsest level at which a LineString first becomes independently
    /// meaningful. A line is eligible from level `i` once its bbox diagonal
    /// reaches `factor · prec[i]`. Lines are 1D so a diagonal equal to `prec`
    /// is only a hairline; the default of `2` defers such short lines to a
    /// finer level. Distinct from `--line-thinning-factor` (which controls grid
    /// cell pitch, not eligibility). Set to `1` to disable.
    #[arg(long, default_value_t = 2)]
    pub line_visibility_factor: u32,
    /// Polygon visibility threshold multiplier applied to `prec` when deciding
    /// the coarsest level at which a Polygon first becomes independently
    /// meaningful. A polygon is eligible from level `i` once its bbox diagonal
    /// reaches `factor · prec[i]`. Default of `4` defers polygons whose
    /// diagonal is under ~4 grid cells to a finer level, so coarse levels aren't
    /// crowded by tiny polygons. Distinct from `--polygon-thinning-factor`.
    /// Set to `1` to disable.
    #[arg(long, default_value_t = 4)]
    pub polygon_visibility_factor: u32,
    /// Attribute column deciding which feature wins when several contend for the
    /// same thinning cell. When set it is the primary criterion: the
    /// higher-ranked feature survives to coarser levels, so the more important
    /// one is kept instead of an arbitrary one. Bbox size only breaks ties
    /// between equal-valued features (e.g. same road class → keep the longer
    /// one), then a deterministic row-index hash. Works for all geometry kinds,
    /// including points (whose bbox size is always zero). The column must be
    /// rank-able: numeric, boolean, or string. Rows whose value is null always
    /// lose the tie.
    #[arg(long)]
    pub sort_key: Option<String>,
    /// Direction for --sort-key: `desc` (default) keeps the feature with the
    /// largest value, `asc` keeps the smallest. Ignored when --sort-key is unset.
    #[arg(long, default_value = "desc")]
    pub sort_order: SortKeyOrder,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum SortKeyOrder {
    /// Largest --sort-key value wins the cell.
    Desc,
    /// Smallest --sort-key value wins the cell.
    Asc,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum InputUnits {
    /// Detect from the GeoParquet `crs` field (ProjectedCRS → meters, else degrees).
    Auto,
    Degrees,
    Meters,
}

/// Upper bound on slice size between byte-limit checks. A fixed cap alone
/// can't enforce `max_bytes` when per-row payload is large — see the probe
/// logic in `write_batch_with_row_group_limits`.
const ROW_GROUP_BYTE_CHECK_MAX_ROWS: usize = 1024;

fn flushed_row_group_end<W: Write + Send>(writer: &ArrowWriter<W>) -> Result<i64> {
    let count = writer.flushed_row_groups().len();
    if count == 0 {
        bail!("internal error: level ended before any row group was written");
    }
    Ok((count as i64) - 1)
}

fn write_batch_with_row_group_limits<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    batch: &RecordBatch,
    max_rows: usize,
    max_bytes: Option<usize>,
) -> Result<()> {
    let Some(max_bytes) = max_bytes else {
        writer.write(batch)?;
        return Ok(());
    };

    let mut offset = 0;
    while offset < batch.num_rows() {
        let buffered_rows = writer.in_progress_rows();
        let buffered_bytes = writer.in_progress_size();
        let rows_until_row_limit = max_rows.saturating_sub(buffered_rows).max(1);

        // Predict how many more rows fit in the remaining byte budget by
        // extrapolating buffered bytes/row. Without a sample (fresh row group)
        // probe a single row first, so a dataset where one row already exceeds
        // `max_bytes` (e.g. dense MultiPolygons) cannot inflate the row group
        // by ~1024× before the next size check.
        let rows_until_byte_limit = if buffered_rows == 0 || buffered_bytes >= max_bytes {
            1
        } else {
            let bytes_per_row = buffered_bytes.div_ceil(buffered_rows).max(1);
            ((max_bytes - buffered_bytes) / bytes_per_row).max(1)
        };

        let rows = (batch.num_rows() - offset)
            .min(rows_until_row_limit)
            .min(rows_until_byte_limit)
            .min(ROW_GROUP_BYTE_CHECK_MAX_ROWS);
        writer.write(&batch.slice(offset, rows))?;
        offset += rows;

        if writer.in_progress_rows() > 0 && writer.in_progress_size() >= max_bytes {
            writer.flush()?;
        }
    }
    Ok(())
}

/// Inspect the GeoParquet column `crs` PROJJSON value to guess coordinate units.
/// Absent / null `crs` defaults to OGC:CRS84 (degrees).
fn detect_input_units(input_geo: Option<&GeoMeta>, geom_col: &str) -> InputUnits {
    let Some(geo) = input_geo else {
        return InputUnits::Degrees;
    };
    let Some(col) = geo.columns.get(geom_col) else {
        return InputUnits::Degrees;
    };
    let Some(crs) = col.crs.as_ref() else {
        return InputUnits::Degrees;
    };
    if crs.is_null() {
        return InputUnits::Degrees;
    }
    fn classify(v: &serde_json::Value) -> Option<InputUnits> {
        let t = v.get("type")?.as_str()?;
        if t.contains("Projected") {
            return Some(InputUnits::Meters);
        }
        if t.contains("Geographic") {
            return Some(InputUnits::Degrees);
        }
        if t == "BoundCRS" {
            return v.get("source_crs").and_then(classify);
        }
        None
    }
    classify(crs).unwrap_or(InputUnits::Degrees)
}

/// Web Mercator equatorial circumference, used as `2π · 6_378_137 m`.
const WEB_MERCATOR_CIRCUMFERENCE_M: f64 = 40_075_016.685_578_49;

/// Ground distance per base unit at the equator at zoom 0, for a tile sliced
/// into `webmerc_resolution` units per side. The default of 1024 yields
/// ~39136 m per unit at zoom 0 — the smallest distance the thinning grid
/// distinguishes at the coarsest level.
fn base_unit_gsd_z0(webmerc_resolution: u32) -> f64 {
    WEB_MERCATOR_CIRCUMFERENCE_M / (webmerc_resolution as f64)
}

fn web_mercator_gsds(
    webmerc_minzoom: u32,
    webmerc_maxzoom: u32,
    webmerc_resolution: u32,
) -> Vec<f64> {
    let z0 = base_unit_gsd_z0(webmerc_resolution);
    (webmerc_minzoom..=webmerc_maxzoom)
        .map(|z| z0 / (1u64 << z) as f64)
        .collect()
}

pub fn run(args: ConvertArgs) -> Result<()> {
    let gsds: Vec<f64> = if !args.gsd.is_empty() {
        args.gsd.clone()
    } else {
        if args.webmerc_minzoom > args.webmerc_maxzoom {
            bail!(
                "--webmerc-minzoom ({}) must be <= --webmerc-maxzoom ({})",
                args.webmerc_minzoom,
                args.webmerc_maxzoom
            );
        }
        if args.webmerc_maxzoom > 30 {
            bail!(
                "--webmerc-maxzoom must be <= 30 (got {})",
                args.webmerc_maxzoom
            );
        }
        if args.webmerc_resolution == 0 {
            bail!(
                "--webmerc-resolution must be > 0 (got {})",
                args.webmerc_resolution
            );
        }
        let derived = web_mercator_gsds(
            args.webmerc_minzoom,
            args.webmerc_maxzoom,
            args.webmerc_resolution,
        );
        eprintln!(
            "      auto-derived {} level(s) from Web Mercator z{}..=z{} (resolution {})",
            derived.len(),
            args.webmerc_minzoom,
            args.webmerc_maxzoom,
            args.webmerc_resolution,
        );
        derived
    };
    // `partial_cmp` rather than `<=` / `<` so NaN values also fail the check
    // (NaN compares as `None`, which is not `Some(Greater)`).
    for w in gsds.windows(2) {
        if w[0].partial_cmp(&w[1]) != Some(std::cmp::Ordering::Greater) {
            bail!("GSD values must be strictly decreasing, got {:?}", gsds);
        }
    }
    for g in &gsds {
        if g.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            bail!("GSD values must be positive, got {:?}", gsds);
        }
    }
    if args.point_thinning_factor == 0 {
        bail!(
            "--point-thinning-factor must be >= 1 (got {})",
            args.point_thinning_factor
        );
    }
    if args.line_thinning_factor == 0 {
        bail!(
            "--line-thinning-factor must be >= 1 (got {})",
            args.line_thinning_factor
        );
    }
    if args.polygon_thinning_factor == 0 {
        bail!(
            "--polygon-thinning-factor must be >= 1 (got {})",
            args.polygon_thinning_factor
        );
    }
    if args.line_visibility_factor == 0 {
        bail!(
            "--line-visibility-factor must be >= 1 (got {})",
            args.line_visibility_factor
        );
    }
    if args.polygon_visibility_factor == 0 {
        bail!(
            "--polygon-visibility-factor must be >= 1 (got {})",
            args.polygon_visibility_factor
        );
    }
    if args.row_group_size == 0 {
        bail!("--row-group-size must be >= 1");
    }

    eprintln!("[1/4] Reading input: {}", args.input.display());
    let file =
        File::open(&args.input).with_context(|| format!("opening {}", args.input.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let input_schema = builder.schema().clone();
    let pq_meta = builder.metadata().clone();
    let input_kv = pq_meta
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    let input_geo: Option<GeoMeta> = input_kv
        .iter()
        .find(|kv| kv.key == GEO_METADATA_KEY)
        .and_then(|kv| kv.value.as_ref())
        .and_then(|v| serde_json::from_str(v).ok());

    let geom_col_name = if let Some(c) = &args.geometry_column {
        c.clone()
    } else if let Some(g) = &input_geo {
        g.primary_column.clone()
    } else {
        guess_geometry_column(&input_schema).ok_or_else(|| {
            anyhow!("could not auto-detect geometry column; pass --geometry-column")
        })?
    };
    let geom_col_idx = input_schema
        .index_of(&geom_col_name)
        .with_context(|| format!("geometry column `{geom_col_name}` not found"))?;
    eprintln!("      geometry column: {geom_col_name}");

    let input_units = match args.input_units {
        InputUnits::Auto => {
            let detected = detect_input_units(input_geo.as_ref(), &geom_col_name);
            eprintln!(
                "      input units (auto): {}",
                match detected {
                    InputUnits::Degrees => "degrees",
                    InputUnits::Meters => "meters",
                    InputUnits::Auto => unreachable!(),
                }
            );
            detected
        }
        explicit => explicit,
    };

    let reader = builder.build()?;
    let mut input_batches = Vec::new();
    for batch in reader {
        input_batches.push(batch?);
    }
    if input_batches.is_empty() {
        bail!("input file has no rows");
    }

    // arrow's GenericBytesBuilder<i32> panics with "byte array offset overflow"
    // once cumulative bytes exceed i32::MAX (~2 GiB). `concat_batches` walks
    // every column independently, so a panic can come from the geometry column
    // *or* any sufficiently large attribute column (e.g. addresses on a
    // country-scale building dataset). Upcast every Binary/Utf8 column that
    // crosses the threshold to its i64-offset counterpart.
    let (input_batches, input_schema) = upcast_large_offsets_if_needed(input_batches, input_schema)?;
    let table: RecordBatch = concat_batches(&input_schema, &input_batches)?;
    let n_rows = table.num_rows();
    eprintln!("      features: {n_rows}");

    let (bboxes, kinds, existing_bbox_col) =
        match read_existing_bboxes(&table, input_geo.as_ref(), &geom_col_name) {
            Some((name, bb)) => {
                eprintln!("[2/4] Reusing existing bbox column `{name}` from input");
                let kinds = compute_kinds(&table, geom_col_idx)?;
                (bb, kinds, Some(name))
            }
            None => {
                eprintln!("[2/4] Computing per-feature bbox from WKB");
                let pairs = compute_bboxes_and_kinds(&table, geom_col_idx)?;
                let (bb, kinds): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
                (bb, kinds, None)
            }
        };

    let sort_ranks = compute_sort_ranks(&table, args.sort_key.as_deref(), args.sort_order)?;
    if let Some(col) = &args.sort_key {
        eprintln!(
            "      tie-break sort key: {col} ({})",
            match args.sort_order {
                SortKeyOrder::Desc => "desc, largest wins",
                SortKeyOrder::Asc => "asc, smallest wins",
            }
        );
    }

    eprintln!("[3/4] Assigning features to {} level(s)", gsds.len());
    let assignment = assign_levels(
        &bboxes,
        &kinds,
        &gsds,
        input_units,
        ThinningFactors {
            point: args.point_thinning_factor,
            line: args.line_thinning_factor,
            polygon: args.polygon_thinning_factor,
        },
        VisibilityFactors {
            line: args.line_visibility_factor,
            polygon: args.polygon_visibility_factor,
        },
        &sort_ranks,
    )?;
    let mut per_level_full: Vec<Vec<u32>> = vec![Vec::new(); gsds.len()];
    for (idx, level_i) in assignment.iter().enumerate() {
        per_level_full[*level_i as usize].push(idx as u32);
    }
    // SPEC §5.3 requires each level entry to have a real row group end, so a
    // level with zero features cannot be represented. Drop those and keep the
    // GSDs that survive.
    let dropped = per_level_full.iter().filter(|r| r.is_empty()).count();
    let (mut per_level, gsds): (Vec<Vec<u32>>, Vec<f64>) = per_level_full
        .into_iter()
        .zip(gsds.iter().copied())
        .filter(|(rows, _)| !rows.is_empty())
        .unzip();
    if per_level.is_empty() {
        bail!("no levels received any features; check input data and GSD selection");
    }
    if dropped > 0 {
        eprintln!("      note: dropped {dropped} empty level(s)");
    }
    for (i, rows) in per_level.iter().enumerate() {
        eprintln!(
            "      level {i} (gsd={:>10.2} m): {:>9} features",
            gsds[i],
            rows.len()
        );
    }

    for (level_idx, rows) in per_level.iter_mut().enumerate() {
        str_pack(rows, &bboxes, args.row_group_size, level_idx);
    }

    eprintln!("[4/4] Writing COGP file: {}", args.output.display());
    // Replace any pre-existing `bbox` column (and the bbox covering column we
    // already consumed into `bboxes`) with the freshly-built struct.
    let drop_names: Vec<&str> = std::iter::once("bbox")
        .chain(existing_bbox_col.as_deref().filter(|n| *n != "bbox"))
        .collect();
    let mut output_fields: Vec<Arc<Field>> = Vec::new();
    let mut keep_col_indices: Vec<usize> = Vec::new();
    for (i, f) in input_schema.fields().iter().enumerate() {
        if drop_names.contains(&f.name().as_str()) {
            eprintln!(
                "      note: dropping input column `{}` (will be overwritten)",
                f.name()
            );
            continue;
        }
        output_fields.push(f.clone());
        keep_col_indices.push(i);
    }
    output_fields.push(Arc::new(bbox_struct_field()));
    let output_schema = Arc::new(Schema::new(output_fields));

    let bbox_struct = build_bbox_struct(&bboxes)?;

    let dataset_bbox = bboxes
        .par_iter()
        .fold(Bbox::empty, |mut acc, b| {
            acc.merge(b);
            acc
        })
        .reduce(Bbox::empty, |mut a, b| {
            a.merge(&b);
            a
        });

    // Disable dictionary encoding for the geometry column (WKB is high-cardinality, dict
    // is pure overhead) and for the bbox struct's float fields (each value is unique).
    let mut props_builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(args.row_group_size)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .set_column_dictionary_enabled(ColumnPath::from(geom_col_name.as_str()), false);
    for child in ["xmin", "ymin", "xmax", "ymax"] {
        props_builder = props_builder.set_column_dictionary_enabled(
            ColumnPath::from(vec!["bbox".to_string(), child.to_string()]),
            false,
        );
    }
    let props = props_builder.build();
    let out_file = File::create(&args.output)
        .with_context(|| format!("creating {}", args.output.display()))?;
    let mut writer = ArrowWriter::try_new(out_file, output_schema.clone(), Some(props))?;

    // Background thread builds RecordBatches (`take` per column is non-trivial
    // for wide tables) while the main thread flushes the previous batch through
    // the parquet writer. Capacity 2 overlaps I/O without unbounded buffering.
    let (tx, rx) = sync_channel::<(usize, RecordBatch)>(2);
    let producer_schema = output_schema.clone();
    let producer_table = Arc::new(table);
    let producer_bbox = Arc::new(bbox_struct);
    let producer_keep = keep_col_indices.clone();
    let producer_per_level = per_level.clone();
    let producer_row_group_size = args.row_group_size;
    let producer = thread::spawn(move || -> Result<()> {
        for (level_i, rows) in producer_per_level.iter().enumerate() {
            for chunk in rows.chunks(producer_row_group_size) {
                let indices = UInt32Array::from(chunk.to_vec());
                let mut cols: Vec<ArrayRef> = Vec::with_capacity(producer_schema.fields().len());
                for ki in &producer_keep {
                    cols.push(take(producer_table.column(*ki).as_ref(), &indices, None)?);
                }
                let bbox_arr: ArrayRef = Arc::new((*producer_bbox).clone());
                cols.push(take(bbox_arr.as_ref(), &indices, None)?);
                let batch = RecordBatch::try_new(producer_schema.clone(), cols)?;
                if tx.send((level_i, batch)).is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    });

    let mut last_level: Option<usize> = None;
    let mut levels_meta: Vec<Level> = Vec::with_capacity(gsds.len());
    let row_group_max_bytes = args.row_group_max_bytes;
    while let Ok((level_i, batch)) = rx.recv() {
        if let Some(prev) = last_level {
            if prev != level_i {
                writer.flush()?;
                levels_meta.push(Level {
                    row_group_end: flushed_row_group_end(&writer)?,
                    gsd: gsds[prev],
                });
            }
        }
        write_batch_with_row_group_limits(
            &mut writer,
            &batch,
            args.row_group_size,
            row_group_max_bytes,
        )?;
        last_level = Some(level_i);
    }
    if let Some(prev) = last_level {
        writer.flush()?;
        levels_meta.push(Level {
            row_group_end: flushed_row_group_end(&writer)?,
            gsd: gsds[prev],
        });
    }
    producer
        .join()
        .map_err(|e| anyhow!("batch producer panicked: {:?}", e))??;

    let mut columns: BTreeMap<String, GeoColumn> = BTreeMap::new();
    if let Some(g) = &input_geo {
        if let Some(orig) = g.columns.get(&geom_col_name) {
            let mut c = orig.clone();
            c.covering = Some(default_covering());
            c.bbox = Some(vec![
                dataset_bbox.xmin,
                dataset_bbox.ymin,
                dataset_bbox.xmax,
                dataset_bbox.ymax,
            ]);
            columns.insert(geom_col_name.clone(), c);
        }
    }
    columns
        .entry(geom_col_name.clone())
        .or_insert_with(|| GeoColumn {
            encoding: "WKB".to_string(),
            geometry_types: Vec::new(),
            covering: Some(default_covering()),
            bbox: Some(vec![
                dataset_bbox.xmin,
                dataset_bbox.ymin,
                dataset_bbox.xmax,
                dataset_bbox.ymax,
            ]),
            crs: None,
        });
    let geo_meta = GeoMeta {
        version: GEOPARQUET_VERSION.to_string(),
        primary_column: geom_col_name.clone(),
        columns,
    };
    let cogp_meta = CogpMeta {
        version: COGP_VERSION.to_string(),
        levels: levels_meta,
    };

    writer.append_key_value_metadata(KeyValue {
        key: GEO_METADATA_KEY.to_string(),
        value: Some(serde_json::to_string(&geo_meta)?),
    });
    writer.append_key_value_metadata(KeyValue {
        key: COGP_METADATA_KEY.to_string(),
        value: Some(serde_json::to_string(&cogp_meta)?),
    });
    let _ = writer.close()?;

    let row_group_count = cogp_meta
        .levels
        .last()
        .map(|level| level.row_group_end + 1)
        .unwrap_or(0);
    eprintln!(
        "      wrote {} row group(s) across {} level(s)",
        row_group_count,
        cogp_meta.levels.len()
    );
    Ok(())
}

fn default_covering() -> Covering {
    Covering {
        bbox: BboxCovering {
            xmin: vec!["bbox".into(), "xmin".into()],
            ymin: vec!["bbox".into(), "ymin".into()],
            xmax: vec!["bbox".into(), "xmax".into()],
            ymax: vec!["bbox".into(), "ymax".into()],
        },
    }
}

fn bbox_child_fields() -> Fields {
    Fields::from(vec![
        Field::new("xmin", DataType::Float64, false),
        Field::new("ymin", DataType::Float64, false),
        Field::new("xmax", DataType::Float64, false),
        Field::new("ymax", DataType::Float64, false),
    ])
}

fn bbox_struct_field() -> Field {
    Field::new("bbox", DataType::Struct(bbox_child_fields()), false)
}

fn build_bbox_struct(bboxes: &[Bbox]) -> Result<StructArray> {
    let xmin: ArrayRef = Arc::new(Float64Array::from(
        bboxes.par_iter().map(|b| b.xmin).collect::<Vec<_>>(),
    ));
    let ymin: ArrayRef = Arc::new(Float64Array::from(
        bboxes.par_iter().map(|b| b.ymin).collect::<Vec<_>>(),
    ));
    let xmax: ArrayRef = Arc::new(Float64Array::from(
        bboxes.par_iter().map(|b| b.xmax).collect::<Vec<_>>(),
    ));
    let ymax: ArrayRef = Arc::new(Float64Array::from(
        bboxes.par_iter().map(|b| b.ymax).collect::<Vec<_>>(),
    ));
    Ok(StructArray::try_new(
        bbox_child_fields(),
        vec![xmin, ymin, xmax, ymax],
        None,
    )?)
}

/// Promote every top-level `Binary`/`Utf8` column whose accumulated bytes
/// across batches would overflow i32 offsets during `concat_batches` to its
/// `Large` counterpart (i64 offsets). Each batch is cast independently — the
/// per-batch arrays were just produced by the parquet reader, so they are
/// already within the i32 budget; the overflow only manifests when arrow tries
/// to combine them. A 1 GiB per-column threshold leaves headroom for arrow's
/// internal rounding and keeps small datasets on the cheaper i32 path.
fn upcast_large_offsets_if_needed(
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> Result<(Vec<RecordBatch>, Arc<Schema>)> {
    const PROMOTE_THRESHOLD: usize = 1 << 30;

    let mut promote: Vec<(usize, DataType)> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let large = match field.data_type() {
            DataType::Binary => DataType::LargeBinary,
            DataType::Utf8 => DataType::LargeUtf8,
            _ => continue,
        };
        let total: usize = batches
            .iter()
            .map(|b| {
                let arr = b.column(i);
                match arr.data_type() {
                    DataType::Binary => arr
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|a| a.value_data().len())
                        .unwrap_or(0),
                    DataType::Utf8 => arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| a.value_data().len())
                        .unwrap_or(0),
                    _ => 0,
                }
            })
            .sum();
        if total >= PROMOTE_THRESHOLD {
            eprintln!(
                "      column `{}` is {} bytes — upcasting {:?} → {:?} to avoid i32 offset overflow",
                field.name(),
                total,
                field.data_type(),
                large,
            );
            promote.push((i, large));
        }
    }
    if promote.is_empty() {
        return Ok((batches, schema));
    }
    let mut new_fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
    for (idx, large) in &promote {
        let old = &new_fields[*idx];
        new_fields[*idx] = Field::new(old.name(), large.clone(), old.is_nullable())
            .with_metadata(old.metadata().clone());
    }
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    let new_batches = batches
        .into_iter()
        .map(|b| -> Result<RecordBatch> {
            let mut cols: Vec<ArrayRef> = b.columns().to_vec();
            for (idx, large) in &promote {
                cols[*idx] = cast(cols[*idx].as_ref(), large)?;
            }
            Ok(RecordBatch::try_new(new_schema.clone(), cols)?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((new_batches, new_schema))
}

fn guess_geometry_column(schema: &Schema) -> Option<String> {
    for f in schema.fields() {
        let n = f.name();
        if matches!(f.data_type(), DataType::Binary | DataType::LargeBinary)
            && (n == "geometry" || n == "geom" || n == "wkb")
        {
            return Some(n.clone());
        }
    }
    for f in schema.fields() {
        if matches!(f.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Some(f.name().clone());
        }
    }
    None
}

/// If the input declares a bbox covering column (GeoParquet 1.1 `covering.bbox`),
/// read the per-row bboxes directly from it instead of recomputing from WKB.
/// Returns `None` if the metadata is missing, the referenced column is not a
/// top-level Float64 struct with the expected children, or any value is null.
fn read_existing_bboxes(
    table: &RecordBatch,
    input_geo: Option<&GeoMeta>,
    geom_col: &str,
) -> Option<(String, Vec<Bbox>)> {
    let covering = input_geo?.columns.get(geom_col)?.covering.as_ref()?;
    let b = &covering.bbox;
    if b.xmin.len() != 2 || b.ymin.len() != 2 || b.xmax.len() != 2 || b.ymax.len() != 2 {
        return None;
    }
    let col_name = &b.xmin[0];
    if &b.ymin[0] != col_name || &b.xmax[0] != col_name || &b.ymax[0] != col_name {
        return None;
    }
    let col_idx = table.schema().index_of(col_name).ok()?;
    let struct_arr = table
        .column(col_idx)
        .as_any()
        .downcast_ref::<StructArray>()?;
    let get = |name: &str| -> Option<&Float64Array> {
        struct_arr
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Float64Array>()
    };
    let xmin = get(&b.xmin[1])?;
    let ymin = get(&b.ymin[1])?;
    let xmax = get(&b.xmax[1])?;
    let ymax = get(&b.ymax[1])?;
    let n = table.num_rows();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if xmin.is_null(i) || ymin.is_null(i) || xmax.is_null(i) || ymax.is_null(i) {
            return None;
        }
        out.push(Bbox {
            xmin: xmin.value(i),
            ymin: ymin.value(i),
            xmax: xmax.value(i),
            ymax: ymax.value(i),
        });
    }
    Some((col_name.clone(), out))
}

fn compute_bboxes_and_kinds(
    table: &RecordBatch,
    geom_col_idx: usize,
) -> Result<Vec<(Bbox, GeomKind)>> {
    let col = table.column(geom_col_idx);
    let n = col.len();
    if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
        (0..n)
            .into_par_iter()
            .map(|i| {
                if arr.is_null(i) {
                    bail!("null geometry at row {i}");
                }
                bbox_from_wkb(arr.value(i))
            })
            .collect()
    } else if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        (0..n)
            .into_par_iter()
            .map(|i| {
                if arr.is_null(i) {
                    bail!("null geometry at row {i}");
                }
                bbox_from_wkb(arr.value(i))
            })
            .collect()
    } else {
        bail!(
            "geometry column has unsupported type `{:?}`; only WKB Binary/LargeBinary is supported",
            col.data_type()
        );
    }
}

fn compute_kinds(table: &RecordBatch, geom_col_idx: usize) -> Result<Vec<GeomKind>> {
    let col = table.column(geom_col_idx);
    let n = col.len();
    if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
        (0..n)
            .into_par_iter()
            .map(|i| {
                if arr.is_null(i) {
                    bail!("null geometry at row {i}");
                }
                kind_from_wkb(arr.value(i))
            })
            .collect()
    } else if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        (0..n)
            .into_par_iter()
            .map(|i| {
                if arr.is_null(i) {
                    bail!("null geometry at row {i}");
                }
                kind_from_wkb(arr.value(i))
            })
            .collect()
    } else {
        bail!(
            "geometry column has unsupported type `{:?}`; only WKB Binary/LargeBinary is supported",
            col.data_type()
        );
    }
}

/// Per-kind multipliers on `prec` for the level thinning grid pitch.
#[derive(Clone, Copy)]
struct ThinningFactors {
    point: u32,
    line: u32,
    polygon: u32,
}

/// Per-kind multipliers on `prec` for the visibility (eligibility) threshold.
/// Points are excluded — they have no extent, so they are always eligible
/// from level 0 regardless of any factor.
#[derive(Clone, Copy)]
struct VisibilityFactors {
    line: u32,
    polygon: u32,
}

/// Per-row tie-break ranks derived from the `--sort-key` attribute column: a
/// larger rank means higher priority within a thinning cell (see `priority`).
/// Equal column values share a rank, so ties still fall through to the hashed
/// row index; null values rank below every non-null and so always lose.
/// Returns all-zero ranks when no key is given, leaving `priority` unchanged.
fn compute_sort_ranks(
    table: &RecordBatch,
    sort_key: Option<&str>,
    order: SortKeyOrder,
) -> Result<Vec<u64>> {
    let Some(col_name) = sort_key else {
        return Ok(vec![0u64; table.num_rows()]);
    };
    let idx = table
        .schema()
        .index_of(col_name)
        .with_context(|| format!("--sort-key column `{col_name}` not found"))?;
    // nulls_first keeps nulls at the lowest rank in both directions; `descending`
    // only flips which end of the value range earns the highest (winning) rank.
    let opts = SortOptions {
        descending: matches!(order, SortKeyOrder::Asc),
        nulls_first: true,
    };
    let ranks = rank(table.column(idx).as_ref(), Some(opts))
        .with_context(|| format!("ranking --sort-key column `{col_name}`"))?;
    Ok(ranks.into_iter().map(u64::from).collect())
}

/// Grid-based density thinning. Returns an assignment of each row to a level index.
///
/// For each level (coarse → fine), bucket remaining features into grid cells of side
/// `prec` (the level's GSD in input CRS units). Within each cell, pick the highest-
/// priority feature to assign to this level; the rest fall through to the next level.
///
/// A feature is *eligible* at a level only once its bbox diagonal reaches
/// `vis_factor · prec` for its kind (see `min_visible`); below that it is excluded and
/// deferred to a finer level where it becomes independently meaningful. This is a hard
/// gate, not a tie-break: it keeps a level's feature count bounded by screen density, so
/// the reader never fetches sub-resolution features at a coarse zoom (the read cost of a
/// progressive `up_to_gsd` read stays independent of total dataset size). The trade-off
/// is that a lone sub-threshold feature does not appear at coarser zooms even where its
/// cell is otherwise empty. Point-kind features have no extent and are eligible from
/// level 0.
///
/// Cells already occupied by a feature assigned at a coarser level are blocked: any
/// remaining candidate whose center re-projects into such a cell at the current
/// level's grid is skipped (it stays in `remaining` for a finer level). Without
/// this, a tight cluster would surface one feature per level in the same visual
/// neighborhood — a coarse winner plus near-identical fine winners around it.
/// Blocking is per-kind because each kind uses its own grid pitch.
fn assign_levels(
    bboxes: &[Bbox],
    kinds: &[GeomKind],
    gsds: &[f64],
    units: InputUnits,
    thinning: ThinningFactors,
    visibility: VisibilityFactors,
    sort_ranks: &[u64],
) -> Result<Vec<u16>> {
    // WGS84 equatorial circumference / 360°: meters per degree of longitude at the equator.
    // Used only as a rendering-grade scale factor — see the README note on geodesy.
    const METERS_PER_DEGREE: f64 = 111_320.0;

    let n = bboxes.len();
    let mut assigned: Vec<i32> = vec![-1; n];
    let mut remaining: Vec<u32> = (0..n as u32).collect();
    let last_level = (gsds.len() - 1) as u16;

    let precs: Vec<f64> = gsds
        .iter()
        .map(|g| match units {
            InputUnits::Degrees => g / METERS_PER_DEGREE,
            InputUnits::Meters => *g,
            InputUnits::Auto => unreachable!("Auto must be resolved before assign_levels"),
        })
        .collect();

    // Kind-specific grid coarsening. Polygons span area so 1-pick-per-prec-cell
    // overlaps visually; points are 0D and lines are 1D in the cross-axis, so
    // their picks look saturated at the same density — coarsen each accordingly.
    let point_thin_mul = thinning.point as f64;
    let line_thin_mul = thinning.line as f64;
    let polygon_thin_mul = thinning.polygon as f64;
    let line_vis_mul = visibility.line as f64;
    let polygon_vis_mul = visibility.polygon as f64;

    // Coarsest level at which each feature is independently meaningful: its bbox
    // diagonal ≥ `vis_factor · prec` for the feature's kind. Diagonal — rather
    // than max(w, h) — so a 45° line is rated by its actual length, not its
    // axis-aligned shadow. Compared in squared form to avoid a per-row sqrt.
    // Points have no extent so are eligible from level 0. Used as a hard
    // eligibility gate in the per-cell selection below.
    let sq_line_vis: Vec<f64> = precs
        .iter()
        .map(|p| (p * line_vis_mul) * (p * line_vis_mul))
        .collect();
    let sq_polygon_vis: Vec<f64> = precs
        .iter()
        .map(|p| (p * polygon_vis_mul) * (p * polygon_vis_mul))
        .collect();
    let min_visible: Vec<u16> = bboxes
        .par_iter()
        .zip(kinds.par_iter())
        .map(|(b, k)| {
            if *k == GeomKind::Point {
                return 0u16;
            }
            let sq_diag = b.width().powi(2) + b.height().powi(2);
            if sq_diag <= 0.0 {
                return 0u16;
            }
            let thresholds = match k {
                GeomKind::Line => &sq_line_vis,
                GeomKind::Polygon => &sq_polygon_vis,
                GeomKind::Point => unreachable!(),
            };
            for (i, sp) in thresholds.iter().enumerate() {
                if sq_diag >= *sp {
                    return i as u16;
                }
            }
            last_level
        })
        .collect();

    let eff_prec = |k: GeomKind, prec: f64| -> f64 {
        match k {
            GeomKind::Point => prec * point_thin_mul,
            GeomKind::Line => prec * line_thin_mul,
            GeomKind::Polygon => prec * polygon_thin_mul,
        }
    };
    let cell_key = |row: u32, prec: f64| -> (u8, i64, i64) {
        let b = bboxes[row as usize];
        let k = kinds[row as usize];
        let ep = eff_prec(k, prec);
        (
            k as u8,
            (b.cx() / ep).floor() as i64,
            (b.cy() / ep).floor() as i64,
        )
    };

    let mut assigned_so_far: Vec<u32> = Vec::new();
    for (level_i, prec) in precs.iter().enumerate() {
        // Re-project every coarser-level feature onto this level's grid and
        // mark those cells as blocked, so a candidate falling into the same
        // (kind, ix, iy) cell as an already-placed coarse winner is skipped.
        // Rebuilt per level because each level's grid pitch differs.
        let blocked: std::collections::HashSet<(u8, i64, i64)> = assigned_so_far
            .par_iter()
            .map(|&row| cell_key(row, *prec))
            .collect();

        let prio = |row: u32| priority(&bboxes[row as usize], sort_ranks[row as usize], row);

        // Per-cell winner map built in parallel: each thread folds into a local
        // HashMap, then reduce merges them keeping the higher-score row on
        // collision. The key is `(kind, ix, iy)` — kind-tagged because each
        // kind uses a different grid pitch, and an untagged `(ix, iy)` would
        // conflate the grids.
        let best: HashMap<(u8, i64, i64), u32> = remaining
            .par_iter()
            .fold(HashMap::new, |mut local, &row| {
                // Hard visibility gate: a feature not yet meaningful at this
                // level (its bbox diagonal < `vis_factor · prec`) is excluded
                // and deferred to a finer level. This bounds a level's feature
                // count by screen density, so reads never fetch sub-resolution
                // features at a coarse zoom. Points are meaningful from level 0.
                if min_visible[row as usize] as usize > level_i {
                    return local;
                }
                let key = cell_key(row, *prec);
                if blocked.contains(&key) {
                    return local;
                }
                match local.get(&key) {
                    None => {
                        local.insert(key, row);
                    }
                    Some(&cur) => {
                        if prio(row) > prio(cur) {
                            local.insert(key, row);
                        }
                    }
                }
                local
            })
            .reduce(HashMap::new, |mut a, mut b| {
                if a.len() < b.len() {
                    std::mem::swap(&mut a, &mut b);
                }
                for (k, row) in b {
                    match a.get(&k) {
                        None => {
                            a.insert(k, row);
                        }
                        Some(&cur) => {
                            if prio(row) > prio(cur) {
                                a.insert(k, row);
                            }
                        }
                    }
                }
                a
            });
        let picked: Vec<u32> = best.values().copied().collect();
        for r in &picked {
            assigned[*r as usize] = level_i as i32;
        }
        assigned_so_far.extend(picked.iter().copied());
        let picked_set: std::collections::HashSet<u32> = picked.iter().copied().collect();
        remaining.retain(|r| !picked_set.contains(r));
        if remaining.is_empty() {
            break;
        }
    }
    for r in remaining {
        assigned[r as usize] = last_level as i32;
    }
    let mut out: Vec<u16> = Vec::with_capacity(n);
    for (i, a) in assigned.iter().enumerate() {
        if *a < 0 {
            bail!("internal: row {i} was never assigned");
        }
        out.push(*a as u16);
    }
    Ok(out)
}

/// Primary order: `sort_rank`, the optional `--sort-key` attribute rank — 0 for
/// every row when no key is given, so it drops out and bbox size leads as
/// before. Secondary: bbox diagonal `w² + h²` (squared, monotonic in the real
/// diagonal, bits give a total order over f64 including NaN guard) — a
/// kind-agnostic, orientation-independent "size" proxy: a 45° line scores the
/// same as an axis-aligned line of equal true length, and a square polygon
/// scores the same as a 90°-rotated one. Tertiary: hashed row index for a
/// deterministic tie-break.
///
/// `sort_rank` leads rather than trails the size: polygon/line bbox diagonals
/// are continuous and practically never tie, so a sort key ranked *below* size
/// could never decide anything for them. Above size it governs every kind, with
/// size breaking ties between equal-ranked features (e.g. same road class → keep
/// the longer one) and, for points, being a constant 0 so the rank fully orders.
fn priority(b: &Bbox, sort_rank: u64, row: u32) -> (u64, u64, u64) {
    let w = b.width().max(0.0);
    let h = b.height().max(0.0);
    let sq_diag = w * w + h * h;
    let sq_bits = if sq_diag.is_finite() && sq_diag >= 0.0 {
        sq_diag.to_bits()
    } else {
        0
    };
    let mut hash = row as u64;
    hash = hash.wrapping_mul(0x9E3779B97F4A7C15);
    hash ^= hash >> 30;
    (sort_rank, sq_bits, hash)
}

/// Per-axis sort direction inherited down the recursion tree.
#[derive(Clone, Copy)]
struct SortDir {
    rev_x: bool,
    rev_y: bool,
}

impl SortDir {
    /// Whether the sort along `axis` should be descending.
    fn reverse_on(self, split_x: bool) -> bool {
        if split_x { self.rev_x } else { self.rev_y }
    }

    /// Right-child direction: flip the *other* axis so the right subtree's
    /// next split along that axis runs in reverse, making the right
    /// subtree's first leaf land next to the left subtree's last leaf.
    fn flip_for_right_child(self, split_x: bool) -> Self {
        if split_x {
            Self { rev_x: self.rev_x, rev_y: !self.rev_y }
        } else {
            Self { rev_x: !self.rev_x, rev_y: self.rev_y }
        }
    }
}

/// Corner the snake traversal starts from at the root of a level.
#[derive(Clone, Copy)]
enum SnakeStart {
    /// (low x, high y) — even levels.
    TopLeft,
    /// (high x, low y) — odd levels; reverses the previous level's exit.
    BottomRight,
}

impl SnakeStart {
    fn for_level(level_idx: usize) -> Self {
        if level_idx.is_multiple_of(2) { Self::TopLeft } else { Self::BottomRight }
    }

    fn initial_dir(self) -> SortDir {
        match self {
            Self::TopLeft => SortDir { rev_x: false, rev_y: true },
            Self::BottomRight => SortDir { rev_x: true, rev_y: false },
        }
    }
}

/// Recursive STR bulk-loading with boustrophedon (snake) leaf ordering.
/// Splits the longer extent axis at a row-group boundary at each step. The
/// snake's starting corner alternates per `level_idx`, so adjacent COGP
/// levels enter/exit on the same side and read order stays spatially local.
fn str_pack(rows: &mut Vec<u32>, bboxes: &[Bbox], row_group_size: usize, level_idx: usize) {
    let dir = SnakeStart::for_level(level_idx).initial_dir();
    // Shared scratch buffer for DSU sorting; sliced (never reallocated) as
    // recursion descends, so total allocation is O(N) once.
    let mut scratch: Vec<(f64, u32)> = vec![(0.0, 0); rows.len()];
    str_pack_rec(rows.as_mut_slice(), &mut scratch, bboxes, row_group_size, dir);
}

fn str_pack_rec(
    rows: &mut [u32],
    scratch: &mut [(f64, u32)],
    bboxes: &[Bbox],
    m: usize,
    dir: SortDir,
) {
    let n = rows.len();
    if n <= m {
        return;
    }
    let extent = rows
        .par_iter()
        .fold(Bbox::empty, |mut acc, i| {
            acc.merge(&bboxes[*i as usize]);
            acc
        })
        .reduce(Bbox::empty, |mut a, b| {
            a.merge(&b);
            a
        });
    let split_x = extent.width() >= extent.height();
    let reverse = dir.reverse_on(split_x);

    // Decorate-Sort-Undecorate: stage (key, row_idx) pairs once, sort by
    // key, then write the reordered row indices back. The sort comparator
    // touches only the local scratch buffer instead of doing O(n log n)
    // random reads into `bboxes`.
    scratch
        .par_iter_mut()
        .zip(rows.par_iter())
        .for_each(|(slot, i)| {
            let b = &bboxes[*i as usize];
            let key = if split_x { b.cx() } else { b.cy() };
            *slot = (key, *i);
        });
    scratch.par_sort_unstable_by(|a, b| {
        let ord = a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal);
        if reverse { ord.reverse() } else { ord }
    });
    rows.par_iter_mut()
        .zip(scratch.par_iter())
        .for_each(|(r, (_, i))| {
            *r = *i;
        });

    // Split on a row-group boundary so every leaf is fully packed at M
    // (only the very last leaf in the level may be partial).
    let num_leaves = n.div_ceil(m);
    let left_leaves = (num_leaves / 2).max(1);
    let split_at = left_leaves * m;
    let (left_rows, right_rows) = rows.split_at_mut(split_at);
    let (left_scratch, right_scratch) = scratch.split_at_mut(split_at);
    let right_dir = dir.flip_for_right_child(split_x);
    rayon::join(
        || str_pack_rec(left_rows, left_scratch, bboxes, m, dir),
        || str_pack_rec(right_rows, right_scratch, bboxes, m, right_dir),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bb(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Bbox {
        Bbox { xmin, ymin, xmax, ymax }
    }

    #[test]
    fn web_mercator_gsds_monotonic_and_halving() {
        let g = web_mercator_gsds(0, 4, 1024);
        assert_eq!(g.len(), 5);
        for w in g.windows(2) {
            assert!((w[0] / 2.0 - w[1]).abs() < 1e-6, "expected halving, got {w:?}");
        }
        // Web Mercator equatorial circumference / 1024 at z0.
        assert!((g[0] - WEB_MERCATOR_CIRCUMFERENCE_M / 1024.0).abs() < 1e-6);
    }

    #[test]
    fn detect_input_units_branches() {
        // No metadata at all → degrees (CRS84 fallback).
        assert!(matches!(detect_input_units(None, "geom"), InputUnits::Degrees));

        let mk = |crs: Option<serde_json::Value>| {
            let mut cols = BTreeMap::new();
            cols.insert(
                "geom".to_string(),
                GeoColumn {
                    encoding: "WKB".into(),
                    geometry_types: vec![],
                    covering: None,
                    bbox: None,
                    crs,
                },
            );
            GeoMeta {
                version: "1.1.0".into(),
                primary_column: "geom".into(),
                columns: cols,
            }
        };

        // Missing column → degrees.
        let geo = mk(None);
        assert!(matches!(
            detect_input_units(Some(&geo), "other"),
            InputUnits::Degrees
        ));

        // crs absent / null → degrees.
        assert!(matches!(
            detect_input_units(Some(&geo), "geom"),
            InputUnits::Degrees
        ));
        let geo_null = mk(Some(serde_json::Value::Null));
        assert!(matches!(
            detect_input_units(Some(&geo_null), "geom"),
            InputUnits::Degrees
        ));

        // ProjectedCRS → meters.
        let geo_proj = mk(Some(serde_json::json!({"type": "ProjectedCRS"})));
        assert!(matches!(
            detect_input_units(Some(&geo_proj), "geom"),
            InputUnits::Meters
        ));

        // GeographicCRS → degrees.
        let geo_geog = mk(Some(serde_json::json!({"type": "GeographicCRS"})));
        assert!(matches!(
            detect_input_units(Some(&geo_geog), "geom"),
            InputUnits::Degrees
        ));

        // BoundCRS recurses into source_crs.
        let geo_bound = mk(Some(serde_json::json!({
            "type": "BoundCRS",
            "source_crs": {"type": "ProjectedCRS"},
        })));
        assert!(matches!(
            detect_input_units(Some(&geo_bound), "geom"),
            InputUnits::Meters
        ));

        // Unknown type → degrees (conservative default).
        let geo_unknown = mk(Some(serde_json::json!({"type": "SomethingElse"})));
        assert!(matches!(
            detect_input_units(Some(&geo_unknown), "geom"),
            InputUnits::Degrees
        ));
    }

    #[test]
    fn priority_orders_by_sort_rank_then_diagonal_then_hash() {
        // Higher sort rank wins even against a much larger bbox — the sort key
        // leads so it actually decides for polygons/lines, not just points.
        let big_low_rank = priority(&bb(0.0, 0.0, 10.0, 10.0), 1, 0);
        let small_high_rank = priority(&bb(0.0, 0.0, 1.0, 1.0), 2, 0);
        assert!(small_high_rank > big_low_rank);

        // Equal sort rank → larger diagonal wins.
        let small = priority(&bb(0.0, 0.0, 1.0, 1.0), 5, 0);
        let large = priority(&bb(0.0, 0.0, 10.0, 10.0), 5, 0);
        assert!(large > small);

        // Equal sort rank and diagonal, different row → deterministic but
        // distinguishable via the hashed tertiary key.
        let a = priority(&bb(0.0, 0.0, 1.0, 1.0), 0, 0);
        let b = priority(&bb(0.0, 0.0, 1.0, 1.0), 0, 1);
        assert_eq!((a.0, a.1), (b.0, b.1));
        assert_ne!(a.2, b.2);
    }

    #[test]
    fn assign_levels_points_always_eligible_from_level_zero() {
        // Two coarse points, far apart → both should land on level 0.
        let bboxes = vec![bb(0.0, 0.0, 0.0, 0.0), bb(1000.0, 1000.0, 1000.0, 1000.0)];
        let kinds = vec![GeomKind::Point, GeomKind::Point];
        let gsds = vec![100.0, 50.0];
        let out = assign_levels(
            &bboxes,
            &kinds,
            &gsds,
            InputUnits::Meters,
            ThinningFactors { point: 1, line: 1, polygon: 1 },
            VisibilityFactors { line: 1, polygon: 1 },
            &[0, 0],
        )
        .unwrap();
        assert_eq!(out, vec![0, 0]);
    }

    #[test]
    fn assign_levels_thins_dense_points_to_finer_levels() {
        // Two points falling into the same level-0 grid cell (`prec=100`):
        // one wins level 0, the other gets deferred. With point_thin=1
        // they're both in cell `(0, 0)` at the coarse level.
        let bboxes = vec![bb(10.0, 10.0, 10.0, 10.0), bb(20.0, 20.0, 20.0, 20.0)];
        let kinds = vec![GeomKind::Point, GeomKind::Point];
        let gsds = vec![100.0, 10.0];
        let out = assign_levels(
            &bboxes,
            &kinds,
            &gsds,
            InputUnits::Meters,
            ThinningFactors { point: 1, line: 1, polygon: 1 },
            VisibilityFactors { line: 1, polygon: 1 },
            &[0, 0],
        )
        .unwrap();
        let mut sorted = out.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1]);
    }

    #[test]
    fn assign_levels_sort_rank_picks_cell_winner() {
        // Same dense-cluster setup as above (both points share level-0 cell
        // (0,0)), but a higher sort rank on row 1 forces it to win level 0,
        // overriding the otherwise-arbitrary hashed tie-break.
        let bboxes = vec![bb(10.0, 10.0, 10.0, 10.0), bb(20.0, 20.0, 20.0, 20.0)];
        let kinds = vec![GeomKind::Point, GeomKind::Point];
        let gsds = vec![100.0, 10.0];
        let out = assign_levels(
            &bboxes,
            &kinds,
            &gsds,
            InputUnits::Meters,
            ThinningFactors { point: 1, line: 1, polygon: 1 },
            VisibilityFactors { line: 1, polygon: 1 },
            &[1, 5],
        )
        .unwrap();
        assert_eq!(out, vec![1, 0]);
    }

    #[test]
    fn compute_sort_ranks_orders_by_value_and_sinks_nulls() {
        use arrow::array::Int32Array;
        let col = Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(20)])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("score", DataType::Int32, true)]));
        let table = RecordBatch::try_new(schema, vec![col]).unwrap();

        // desc → largest value gets the highest rank, null the lowest.
        let desc = compute_sort_ranks(&table, Some("score"), SortKeyOrder::Desc).unwrap();
        assert!(desc[2] > desc[3] && desc[3] > desc[0] && desc[0] > desc[1]);

        // asc → smallest value gets the highest rank, null still lowest.
        let asc = compute_sort_ranks(&table, Some("score"), SortKeyOrder::Asc).unwrap();
        assert!(asc[0] > asc[3] && asc[3] > asc[2] && asc[2] > asc[1]);

        // No key → all zeros (priority unaffected).
        assert_eq!(
            compute_sort_ranks(&table, None, SortKeyOrder::Desc).unwrap(),
            vec![0, 0, 0, 0]
        );

        // Unknown column → error.
        assert!(compute_sort_ranks(&table, Some("missing"), SortKeyOrder::Desc).is_err());
    }

    #[test]
    fn assign_levels_subthreshold_feature_deferred_to_finer_level() {
        // A lone polygon below the visibility threshold (diagonal² ≈ 2 vs the
        // level-0 threshold² of (10·4)² = 1600) is excluded from the coarse
        // level by the hard gate even though its cell is otherwise empty, and
        // deferred to the finest level. This is what bounds a coarse-zoom read
        // by screen density instead of total dataset size.
        let bboxes = vec![bb(0.0, 0.0, 1.0, 1.0)];
        let kinds = vec![GeomKind::Polygon];
        let gsds = vec![10.0, 0.5];
        let out = assign_levels(
            &bboxes,
            &kinds,
            &gsds,
            InputUnits::Meters,
            ThinningFactors { point: 1, line: 1, polygon: 1 },
            VisibilityFactors { line: 2, polygon: 4 },
            &[0],
        )
        .unwrap();
        assert_eq!(out, vec![1]);
    }

    #[test]
    fn assign_levels_visible_feature_beats_subthreshold_in_same_cell() {
        // Two polygons in the same level-0 cell (pitch 10): the visible one
        // (diagonal² = 3200 ≥ 1600) takes level 0; the sub-threshold one
        // (diagonal² = 2) is gated out of level 0 and deferred to a finer
        // level, not dropped.
        let big = bb(-15.0, -15.0, 25.0, 25.0); // center (5,5)
        let small = bb(1.0, 1.0, 2.0, 2.0); // center (1.5,1.5)
        let bboxes = vec![big, small];
        let kinds = vec![GeomKind::Polygon, GeomKind::Polygon];
        let gsds = vec![10.0, 0.5];
        let out = assign_levels(
            &bboxes,
            &kinds,
            &gsds,
            InputUnits::Meters,
            ThinningFactors { point: 1, line: 1, polygon: 1 },
            VisibilityFactors { line: 2, polygon: 4 },
            &[0, 0],
        )
        .unwrap();
        assert_eq!(out, vec![0, 1]);
    }

    #[test]
    fn str_pack_preserves_set_and_uses_full_leaves() {
        // 17 features in 4 row-group bins of 5 → packs deterministically and
        // never drops a row.
        let mut bboxes = Vec::new();
        for i in 0..17 {
            let x = (i % 5) as f64;
            let y = (i / 5) as f64;
            bboxes.push(bb(x, y, x + 0.1, y + 0.1));
        }
        let mut rows: Vec<u32> = (0..17u32).collect();
        str_pack(&mut rows, &bboxes, 5, 0);
        let mut sorted = rows.clone();
        sorted.sort();
        let expected: Vec<u32> = (0..17u32).collect();
        assert_eq!(sorted, expected, "str_pack must preserve the row set");
    }

    #[test]
    fn flushed_row_group_end_errors_on_empty() {
        // The shim error path is hard to hit in real code (there's always
        // ≥1 row group), but the helper must refuse to lie.
        let buf = Vec::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let writer = ArrowWriter::try_new(buf, schema, None).unwrap();
        assert!(flushed_row_group_end(&writer).is_err());
    }

    #[test]
    fn guess_geometry_column_prefers_named() {
        use arrow::datatypes::Field;
        let s = Schema::new(vec![
            Field::new("attr", DataType::Binary, false),
            Field::new("geometry", DataType::Binary, false),
        ]);
        assert_eq!(guess_geometry_column(&s).as_deref(), Some("geometry"));

        let s = Schema::new(vec![Field::new("blob", DataType::LargeBinary, false)]);
        assert_eq!(guess_geometry_column(&s).as_deref(), Some("blob"));

        let s = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        assert_eq!(guess_geometry_column(&s), None);
    }

    #[test]
    fn snake_start_alternates_per_level() {
        // Even levels start top-left, odd levels bottom-right. The first
        // axis direction flips so the snake's exit on level N lines up
        // with the entry on level N+1.
        let d0 = SnakeStart::for_level(0).initial_dir();
        let d1 = SnakeStart::for_level(1).initial_dir();
        assert!(!d0.rev_x && d0.rev_y);
        assert!(d1.rev_x && !d1.rev_y);
    }
}
