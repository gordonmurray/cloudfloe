# Sample Dataset

This directory contains **37,537 IMDb movies** as a source for the demo
Iceberg table. The local files are Hive-partitioned Parquet; on `docker
compose up`, the init container reads them and writes an Apache Iceberg v2
table to MinIO at `s3://movies/warehouse/demo/movies`.

## Source layout (local)

```
sample_data/data/
├── decade=1890/titleType=movie/data.parquet
├── decade=1900/titleType=movie/data.parquet
├── decade=1910/titleType=movie/data.parquet
...
└── decade=2010/titleType=movie/data.parquet
```

## Dataset details

- **Target format in MinIO**: Apache Iceberg v2 (written by pyiceberg)
- **Local source format**: Hive-partitioned Parquet (by decade / titleType)
- **Total rows**: 37,537 movies and TV shows
- **Source size**: ~1.8 MB compressed
- **Source**: IMDb title.basics dataset (sample)

## Columns

- `tconst` — IMDb identifier
- `titleType` — Type of title (movie, tvSeries, etc.)
- `primaryTitle` — Popular title
- `originalTitle` — Original title
- `isAdult` — Adult content flag
- `startYear` — Release year
- `endYear` — End year (for TV series)
- `runtimeMinutes` — Runtime in minutes
- `genres` — Comma-separated genres
- `decade` — Materialised from the Hive partition
- `titleType` — Materialised from the Hive partition

## Usage

The demo Iceberg table is seeded automatically by `docker compose up -d`.
Query it via DuckDB:

```sql
SELECT * FROM iceberg_scan('s3://movies/warehouse/demo/movies') LIMIT 100;
```

## About This Dataset

This sample data was generated from the IMDb [title.basics dataset](https://datasets.imdbws.com/) and converted to partitioned Parquet format for demonstration purposes. It includes a diverse selection of movies and TV shows from 1890-2010s to showcase partition pruning and aggregation queries.
