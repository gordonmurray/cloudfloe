# Sample Dataset

This directory contains **37,537 IMDb movies** in partitioned Parquet format.

## Structure

```
sample_data/data/
├── decade=1890/titleType=movie/data.parquet
├── decade=1900/titleType=movie/data.parquet
├── decade=1910/titleType=movie/data.parquet
...
└── decade=2010/titleType=movie/data.parquet
```

## Dataset Details

- **Format**: Partitioned Parquet (Iceberg-compatible)
- **Partitions**: By decade and title type (20 partitions)
- **Total Rows**: 37,537 movies and TV shows
- **Total Size**: ~1.8 MB compressed
- **Source**: IMDb title.basics dataset (sample)

## Columns

- `tconst` - IMDb identifier
- `titleType` - Type of title (movie, tvSeries, etc.)
- `primaryTitle` - Popular title
- `originalTitle` - Original title
- `isAdult` - Adult content flag
- `startYear` - Release year
- `endYear` - End year (for TV series)
- `runtimeMinutes` - Runtime in minutes
- `genres` - Comma-separated genres

## Usage

This data is automatically loaded into MinIO when you run:

```bash
docker compose up -d
```

Then query it via DuckDB:

```sql
SELECT * FROM read_parquet('s3://movies/data/**/*.parquet') LIMIT 100;
```

## About This Dataset

This sample data was generated from the IMDb [title.basics dataset](https://datasets.imdbws.com/) and converted to partitioned Parquet format for demonstration purposes. It includes a diverse selection of movies and TV shows from 1890-2010s to showcase partition pruning and aggregation queries.
