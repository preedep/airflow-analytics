# airflow-analytics

Rust CLI that turns an Airflow 3 metadata export into a single, self-contained HTML report about the
DAG processor: how long each DAG takes to parse, how long a full parse loop really takes, which DAGs
are removed (`is_stale`), overdue or non-deterministic, and when a deployed change should become
visible in the UI.

The report embeds its data and charting library ([Apache ECharts](https://echarts.apache.org/),
vendored in `assets/`), so it opens offline. It supports light/dark mode and has English/Thai
explanations.

## Input

A CSV export of the `dag` table joined with `dag_version`, for example:

```sql
SELECT
    d.dag_id, d.fileloc, d.last_parsed_time,
    ROUND(d.last_parse_duration::numeric, 2) AS parse_seconds,
    d.is_stale, d.bundle_name, d.bundle_version,
    dv.version_number,
    dv.created_at AS version_created_at,
    MAX(dv.version_number) OVER w   AS latest_version,
    COUNT(dv.version_number) OVER w AS version_count,
    dv.version_number = MAX(dv.version_number) OVER w AS is_latest,
    ROUND(EXTRACT(EPOCH FROM dv.created_at - LAG(dv.created_at) OVER w_seq)::numeric, 1)
        AS seconds_since_prev_version
FROM dag d
LEFT JOIN dag_version dv ON dv.dag_id = d.dag_id
WINDOW w AS (PARTITION BY d.dag_id),
       w_seq AS (PARTITION BY d.dag_id ORDER BY dv.version_number);
```

Older, narrower exports (without `is_stale` or version history) also work; the sections that need
the missing columns are hidden.

## Usage

```sh
# defaults: --input dataset/DAG_processing_time.csv --output report/dag_processor_report.html
./generate-report.sh --open

# match your Airflow config
IMPORT_TIMEOUT=600 FILE_PROCESSOR_TIMEOUT=600 PARSING_PROCESSES=32 ./generate-report.sh

cargo run --release -- --help
```

## Development

```sh
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

`dataset/` and `report/` are git-ignored; never commit real exports.
