//! Reader for COGP (Cloud Optimized GeoParquet Profile) files.
//!
//! The reader exposes COGP/GeoParquet metadata, lets callers select row groups by
//! level / GSD / bbox using the per-level `row_group_end` index and the covering
//! bbox column's Parquet statistics, and returns Arrow [`RecordBatch`] iterators.
//!
//! Geometries stay in their on-disk WKB form. Downstream callers can pull the
//! `Binary` / `LargeBinary` geometry column out of each batch and convert to
//! GeoJSON / WKT / geo-types / FlatGeobuf / ... with the `geozero` crate — see
//! the README for an end-to-end example.
//!
//! ```no_run
//! use cogp::reader::Reader;
//! let r = Reader::open("data.cogp.parquet")?;
//! for level in r.levels() {
//!     println!("gsd={} m, last row group={}", level.gsd, level.row_group_end);
//! }
//! # Ok::<(), anyhow::Error>(())
//! ```
use anyhow::{anyhow, Context, Result};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::file::reader::ChunkReader;
use parquet::file::statistics::Statistics;
use std::fs::File;
use std::ops::Range;
use std::path::Path;

use crate::meta::{CogpMeta, GeoMeta, Level, COGP_METADATA_KEY, GEO_METADATA_KEY};

pub struct Reader<R: ChunkReader + 'static> {
    builder: ParquetRecordBatchReaderBuilder<R>,
    geo_meta: GeoMeta,
    cogp_meta: CogpMeta,
}

impl Reader<File> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        Self::try_new(file)
    }
}

impl<R: ChunkReader + 'static> Reader<R> {
    pub fn try_new(reader: R) -> Result<Self> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
        let kv = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        let geo_str = kv
            .iter()
            .find(|kv| kv.key == GEO_METADATA_KEY)
            .and_then(|kv| kv.value.as_deref())
            .ok_or_else(|| anyhow!("missing `geo` key-value metadata (not a GeoParquet file)"))?;
        let geo_meta: GeoMeta = serde_json::from_str(geo_str)
            .map_err(|e| anyhow!("`geo` metadata is not valid JSON: {e}"))?;
        let cogp_str = kv
            .iter()
            .find(|kv| kv.key == COGP_METADATA_KEY)
            .and_then(|kv| kv.value.as_deref())
            .ok_or_else(|| anyhow!("missing `cogp` key-value metadata"))?;
        let cogp_meta: CogpMeta = serde_json::from_str(cogp_str)
            .map_err(|e| anyhow!("`cogp` metadata is not valid JSON: {e}"))?;
        Ok(Self {
            builder,
            geo_meta,
            cogp_meta,
        })
    }

    pub fn geo_meta(&self) -> &GeoMeta {
        &self.geo_meta
    }

    pub fn cogp_meta(&self) -> &CogpMeta {
        &self.cogp_meta
    }

    pub fn levels(&self) -> &[Level] {
        &self.cogp_meta.levels
    }

    pub fn primary_column(&self) -> &str {
        &self.geo_meta.primary_column
    }

    pub fn num_row_groups(&self) -> usize {
        self.builder.metadata().num_row_groups()
    }

    /// Row groups belonging to a single level (start..=end), or `None` if `level`
    /// is out of range. Row groups within one level are STR-packed so reading in
    /// natural order yields a spatially coherent stream.
    pub fn row_groups_in_level(&self, level: usize) -> Option<Range<usize>> {
        let l = self.cogp_meta.levels.get(level)?;
        let start = if level == 0 {
            0
        } else {
            (self.cogp_meta.levels[level - 1].row_group_end + 1) as usize
        };
        let end = (l.row_group_end + 1) as usize;
        Some(start..end)
    }

    /// Row groups for all levels up to and including `level` — the typical
    /// "progressive read" shape: include this level plus every coarser one.
    /// Clamps to the available level range; returns an empty range if there
    /// are no levels.
    pub fn row_groups_up_to_level(&self, level: usize) -> Range<usize> {
        if self.cogp_meta.levels.is_empty() {
            return 0..0;
        }
        let i = level.min(self.cogp_meta.levels.len() - 1);
        0..(self.cogp_meta.levels[i].row_group_end + 1) as usize
    }

    /// Row groups for every level whose GSD is `>= min_gsd` (coarser than the
    /// caller's target resolution). Use this when you have a target ground
    /// resolution (e.g. screen meters/pixel) and want every level that's
    /// still useful at that scale, plus all coarser overviews.
    pub fn row_groups_up_to_gsd(&self, min_gsd: f64) -> Range<usize> {
        let last = self
            .cogp_meta
            .levels
            .iter()
            .rposition(|l| l.gsd >= min_gsd);
        match last {
            Some(i) => 0..(self.cogp_meta.levels[i].row_group_end + 1) as usize,
            None => 0..0,
        }
    }

    /// Row groups whose covering-bbox envelope intersects `[xmin, ymin, xmax, ymax]`.
    /// Uses the Parquet `Double` min/max statistics on the covering bbox sub-columns
    /// declared by `geo.columns[primary].covering.bbox`. Row groups missing stats
    /// are conservatively kept (they may match).
    pub fn row_groups_intersecting_bbox(&self, bbox: [f64; 4]) -> Vec<usize> {
        let metadata = self.builder.metadata();
        let n = metadata.num_row_groups();
        let primary = &self.geo_meta.primary_column;
        let covering = match self
            .geo_meta
            .columns
            .get(primary)
            .and_then(|c| c.covering.as_ref())
        {
            Some(c) => c,
            None => return (0..n).collect(),
        };
        let schema = metadata.file_metadata().schema_descr();
        let find_col = |path_parts: &[String]| -> Option<usize> {
            let dotted = path_parts.join(".");
            (0..schema.num_columns()).find(|i| schema.column(*i).path().string() == dotted)
        };
        let (Some(xmin_i), Some(ymin_i), Some(xmax_i), Some(ymax_i)) = (
            find_col(&covering.bbox.xmin),
            find_col(&covering.bbox.ymin),
            find_col(&covering.bbox.xmax),
            find_col(&covering.bbox.ymax),
        ) else {
            return (0..n).collect();
        };
        let [qxmin, qymin, qxmax, qymax] = bbox;
        let mut out = Vec::with_capacity(n);
        for rg_i in 0..n {
            let rg = metadata.row_group(rg_i);
            let dmin = |idx: usize| -> Option<f64> {
                match rg.column(idx).statistics()? {
                    Statistics::Double(s) => s.min_opt().copied(),
                    _ => None,
                }
            };
            let dmax = |idx: usize| -> Option<f64> {
                match rg.column(idx).statistics()? {
                    Statistics::Double(s) => s.max_opt().copied(),
                    _ => None,
                }
            };
            // Row-group envelope: every feature's bbox satisfies
            //   feature.xmin >= min(xmin column), feature.xmax <= max(xmax column), …
            // so the group's overall envelope is [min xmin, min ymin, max xmax, max ymax].
            // Missing stats → keep the group (caller can re-filter per row).
            let keep = match (dmin(xmin_i), dmin(ymin_i), dmax(xmax_i), dmax(ymax_i)) {
                (Some(gxmin), Some(gymin), Some(gxmax), Some(gymax)) => {
                    gxmax >= qxmin && gxmin <= qxmax && gymax >= qymin && gymin <= qymax
                }
                _ => true,
            };
            if keep {
                out.push(rg_i);
            }
        }
        out
    }

    /// Build a [`ParquetRecordBatchReader`] over the given row groups. Consumes
    /// `self` because the underlying builder is single-shot.
    pub fn into_batch_reader<I>(self, row_groups: I) -> Result<ParquetRecordBatchReader>
    where
        I: IntoIterator<Item = usize>,
    {
        let rgs: Vec<usize> = row_groups.into_iter().collect();
        Ok(self.builder.with_row_groups(rgs).build()?)
    }

    /// Build a [`ParquetRecordBatchReader`] over every row group.
    pub fn into_batch_reader_all(self) -> Result<ParquetRecordBatchReader> {
        Ok(self.builder.build()?)
    }
}
