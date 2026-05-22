# cogp

Rust reference CLI for the [Cloud Optimized GeoParquet Profile (COGP)](../SPEC.md).

`convert` reorders the features of a GeoParquet file across row groups using
grid-based density thinning per level and Sort-Tile-Recursive (STR) bbox packing
inside each level. `validate` checks the structural rules in SPEC §5.

## Install

Pre-built binaries for Linux / macOS / Windows are attached to each
[GitHub release](https://github.com/Kanahiro/cloud-optimized-geoparquet/releases).

Or build from source:

```
cargo build --release -p cogp
# binary at cogp-rs/target/release/cogp
```

## Quickstart

```
cogp convert input.parquet output.cogp.parquet
cogp validate output.cogp.parquet
```

The output COGP file itself is projection-agnostic — it can be consumed by
any renderer regardless of projection. The defaults simply pick GSDs tuned
for a Web Mercator z0..=z16 tile pyramid (17 levels), since that's the most
common viewer target. Pass `--gsd` to optimize for a different renderer.
Works on any GeoParquet 1.x file with a WKB geometry column.

## convert

```
cogp convert <INPUT> <OUTPUT> [OPTIONS]
```

Examples:

```
# Narrow the zoom range and bump the row group size for a small dataset.
cogp convert input.parquet output.cogp.parquet \
    --webmerc-minzoom 4 --webmerc-maxzoom 12 --row-group-size 20000

# Optimize for a renderer other than Web Mercator: pass GSDs directly
# (meters, coarse to fine). The defaults still produce a valid COGP file for
# any consumer — use this only when you want level GSDs tuned to a specific
# pyramid.
cogp convert input.parquet output.cogp.parquet \
    --gsd 1000,500,100,50

# Point dataset already in a projected CRS; thin points more aggressively.
cogp convert points.parquet points.cogp.parquet \
    --input-units meters --point-thinning-factor 8
```

Level selection (mutually exclusive). These only choose the per-level GSDs
used during conversion; the resulting COGP file is projection-agnostic and
readable by any renderer regardless of which path you pick.

- `--gsd 1000,500,100,50` — explicit ground sample distances in **meters**,
  strictly decreasing. Use this to tune levels for a specific renderer (any
  projection — not just non-Web-Mercator).
- `--webmerc-minzoom` / `--webmerc-maxzoom` (default `0` / `16`) — derive
  GSDs from a Web Mercator tile pyramid:
  `GSD(z) = 40_075_016 / (webmerc_resolution · 2^z)` m. This is the default
  because Web Mercator is the most common viewer target, not because the
  output is restricted to it. Empty levels (no features assigned) are
  dropped automatically.
- `--webmerc-resolution` (default `1024`) — units per tile side used in the
  Web Mercator GSD formula above. `1024` keeps the thinning grid at ~4× the
  typical 256-pixel tile resolution, so features collapsing within a few
  subpixels are dropped. Controls thinning granularity only; ignored when
  `--gsd` is given.

Other options:

- `--row-group-size` (default `10000`) — max Parquet row group size in rows.
  Row group boundaries always align with level boundaries.
- `--row-group-max-bytes` — max estimated encoded Parquet row group size in
  bytes. This must be a numeric byte count, without suffixes. It is enforced
  using the Parquet writer's in-progress encoded-size estimate, checked at batch
  granularity, so an individual row group can exceed the target slightly.
- `--input-units` (default `auto`) — `auto` reads the GeoParquet `crs`
  PROJJSON (`ProjectedCRS` → meters, otherwise degrees; absent / null → degrees
  per OGC:CRS84). Override with `degrees` or `meters` explicitly.
  **For datasets spanning high latitudes or the antimeridian, reproject to a
  meter-based CRS before running `convert`.** The degree→meter conversion is
  rendering-grade, not geodesic.
- `--point-thinning-factor` (default `4`) — point-like features (WKB
  `Point` / `MultiPoint`) thin on a grid this many times coarser per axis
  than the level GSD. Points occupy a single cell visually, so equal grid
  density looks too dense compared to lines/polygons. Set to `1` to disable.
- `--line-thinning-factor` (default `2`) — line-like features (WKB
  `LineString` / `MultiLineString`) thin on a grid this many times coarser
  per axis than the level GSD. Lines are 1D so multiple parallel/near-parallel
  lines within one cell overlap visually even when their bbox centers fall
  into distinct cells. Smaller than `--point-thinning-factor` because lines
  still span many cells along their length. Set to `1` to disable.
- `--polygon-thinning-factor` (default `1`) — polygon-like features (WKB
  `Polygon` / `MultiPolygon`) thin on a grid this many times coarser per
  axis than the level GSD. Polygons span area so the default of `1` already
  looks well-covered; raise to thin further.
- `--line-visibility-factor` (default `2`) — coarsest level at which a
  LineString first becomes independently meaningful: its bbox diagonal must
  reach `factor · GSD` of that level. Lines are 1D so a diagonal equal to
  one GSD is only a hairline; the default defers such short lines to a
  finer level. Distinct from `--line-thinning-factor` (which controls grid
  cell pitch, not eligibility). Set to `1` to disable.
- `--polygon-visibility-factor` (default `4`) — coarsest level at which a
  Polygon first becomes independently meaningful: its bbox diagonal must
  reach `factor · GSD` of that level. The default defers polygons whose
  diagonal is under ~4 grid cells to a finer level, so coarse levels aren't
  crowded by tiny polygons. Distinct from `--polygon-thinning-factor`.
  Set to `1` to disable.
- `--geometry-column` — override the auto-detected primary geometry column.
  Input must be a WKB `Binary`/`LargeBinary` Arrow column.

The output file:

- preserves all original columns except the covering bbox: if the input has a
  GeoParquet 1.1 `covering.bbox` struct, its values are reused (not
  recomputed) and the original column is dropped; any column literally named
  `bbox` is also dropped;
- writes a canonical `bbox` struct column (`xmin/ymin/xmax/ymax: f64`) and
  points GeoParquet 1.1 `covering.bbox` at it;
- emits one or more row groups per level, written in coarse-to-fine order;
- writes `cogp` metadata listing the `row_group_end` and `gsd` of each level.

## Library use — reading COGP files

The crate also exposes a `Reader` for reading COGP files from Rust. The reader
hands back Arrow `RecordBatch`es with the geometry column kept in its on-disk
WKB form, so downstream users plug in [`geozero`](https://crates.io/crates/geozero)
(or any other WKB consumer) to convert into GeoJSON / WKT / `geo-types` /
FlatGeobuf / etc.

```toml
[dependencies]
cogp = "0.1"
geozero = { version = "0.14", features = ["with-wkb"] }
arrow-array = "56"
```

```rust
use arrow_array::{Array, BinaryArray, LargeBinaryArray};
use cogp::reader::Reader;
use geozero::wkb::Wkb;
use geozero::ToJson;

let r = Reader::open("data.cogp.parquet")?;
let primary = r.primary_column().to_string();

// Progressive read: every level whose GSD is >= 1000 m (coarsest overviews).
// Use `row_groups_up_to_level(i)` for level-based selection, or
// `row_groups_intersecting_bbox([xmin, ymin, xmax, ymax])` for a spatial query.
let rgs = r.row_groups_up_to_gsd(1000.0);
let batches = r.into_batch_reader(rgs)?;

for batch in batches {
    let batch = batch?;
    let geom = batch.column_by_name(&primary).unwrap();
    if let Some(arr) = geom.as_any().downcast_ref::<BinaryArray>() {
        for i in 0..arr.len() {
            let geojson = Wkb(arr.value(i).to_vec()).to_json()?;
            println!("{geojson}");
        }
    } else if let Some(arr) = geom.as_any().downcast_ref::<LargeBinaryArray>() {
        for i in 0..arr.len() {
            let geojson = Wkb(arr.value(i).to_vec()).to_json()?;
            println!("{geojson}");
        }
    }
}
# Ok::<(), anyhow::Error>(())
```

Reader API at a glance:

- `Reader::open(path)` / `Reader::try_new(reader)` — open a file or any
  `parquet::file::reader::ChunkReader`.
- `levels()`, `cogp_meta()`, `geo_meta()`, `primary_column()` — inspect metadata.
- `row_groups_in_level(i)` — row groups belonging to a single level.
- `row_groups_up_to_level(i)` — every level up to and including `i` (coarse → fine).
- `row_groups_up_to_gsd(min_gsd)` — every level whose GSD is `>= min_gsd`.
- `row_groups_intersecting_bbox([xmin, ymin, xmax, ymax])` — row groups whose
  covering-bbox envelope intersects the query, using Parquet column statistics.
- `into_batch_reader(rgs)` / `into_batch_reader_all()` — build the underlying
  `ParquetRecordBatchReader`.

Combine the row-group selectors with set intersection to do, e.g., "give me
every feature in this bbox at zoom <= 8":

```rust
# use cogp::reader::Reader;
# let r = Reader::open("data.cogp.parquet")?;
let by_level: std::ops::Range<usize> = r.row_groups_up_to_level(8);
let by_bbox: Vec<usize> = r.row_groups_intersecting_bbox([139.0, 35.0, 140.0, 36.0]);
let rgs: Vec<usize> = by_bbox.into_iter().filter(|i| by_level.contains(i)).collect();
let batches = r.into_batch_reader(rgs)?;
# Ok::<(), anyhow::Error>(())
```

## validate

```
cogp validate <FILE>
```

Checks SPEC §5:

- `geo` metadata is present (GeoParquet 1.x) with a `covering.bbox`;
- each covering bbox column has Parquet row group min/max statistics;
- `cogp` metadata is present with a non-empty `levels` array;
- `row_group_end` values are strictly increasing and end at `num_row_groups - 1`;
- `gsd` values are positive and strictly decreasing.

Exits non-zero on failure.
