"""
Weather ETL DAG

Pipeline: Extract → Archive to S3 → Transform → Load to RDS → Validate
Schedule: hourly

Each phase is a separate Airflow task so failures are visible at the task level
and retries only re-run the failed step, not the whole pipeline.

Data flows between tasks via XCom:
  extract        → pushes raw_data (list of API response dicts)
  archive_to_s3  → pushes s3_key
  transform      → pushes transformed_records (JSON string)
  load           → pushes load_stats dict
  validate       → logs DB totals (terminal task, no push needed)
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the project's modules importable from the DAG
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

log = logging.getLogger(__name__)

# ── Default args applied to every task ────────────────────────────────────────
default_args = {
    'owner': 'weather-etl',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}


# ── Task callables ─────────────────────────────────────────────────────────────

def extract(**context):
    from extractors.weather_api import WeatherAPIExtractor
    extractor = WeatherAPIExtractor()
    raw_data = extractor.extract_from_config()
    if not raw_data:
        raise ValueError("Extraction returned no data — check API key and CITIES config.")
    context['ti'].xcom_push(key='raw_data', value=raw_data)
    log.info("Extracted %d records", len(raw_data))
    return len(raw_data)


def archive_to_s3(**context):
    from loaders.s3_loader import S3Loader
    raw_data = context['ti'].xcom_pull(task_ids='extract', key='raw_data')
    run_ts = context['logical_date'].replace(tzinfo=None)  # naive UTC datetime
    s3_loader = S3Loader()
    s3_key = s3_loader.upload_raw_data(raw_data, run_ts)
    log.info("Archived to S3: %s", s3_key)
    return s3_key


def transform(**context):
    import pandas as pd
    from transformers.weather_transform import WeatherTransformer

    raw_data = context['ti'].xcom_pull(task_ids='extract', key='raw_data')
    transformer = WeatherTransformer()
    df = transformer.transform_batch(raw_data)

    if df.empty:
        raise ValueError("Transformation returned empty DataFrame.")

    # Serialize to JSON for XCom (DataFrames aren't XCom-safe directly)
    records_json = df.to_json(orient='records', date_format='iso')
    context['ti'].xcom_push(key='transformed_records', value=records_json)
    log.info("Transformed %d records", len(df))
    return len(df)


def load(**context):
    import io
    import pandas as pd
    from config.database import DatabaseConfig
    from loaders.database_loader import DatabaseLoader

    records_json = context['ti'].xcom_pull(task_ids='transform', key='transformed_records')
    df = pd.read_json(io.StringIO(records_json), orient='records')

    db = DatabaseConfig()
    loader = DatabaseLoader(db)
    cities, records = loader.load_complete_batch(df)

    stats = {'cities': cities, 'records': records}
    context['ti'].xcom_push(key='load_stats', value=stats)
    log.info("Loaded — cities: %d, records: %d", cities, records)
    return records


def validate(**context):
    from config.database import DatabaseConfig
    from loaders.database_loader import DatabaseLoader

    load_stats = context['ti'].xcom_pull(task_ids='load', key='load_stats')
    db = DatabaseConfig()
    loader = DatabaseLoader(db)
    db_stats = loader.get_city_stats()

    log.info(
        "Run loaded %s | DB totals: cities=%d, records=%d, range=%s to %s",
        load_stats,
        db_stats['cities'],
        db_stats['weather_records'],
        db_stats['earliest_record'],
        db_stats['latest_record'],
    )
    return db_stats['weather_records']


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id='weather_etl',
    default_args=default_args,
    description='Hourly weather ETL: OpenWeatherMap API → S3 (raw) → RDS PostgreSQL',
    schedule='@hourly',
    start_date=datetime(2026, 4, 28, tzinfo=timezone.utc),
    catchup=False,
    tags=['etl', 'weather', 'aws'],
    doc_md="""
## Weather ETL Pipeline

Fetches current weather for 5 cities every hour and stores the data in:
- **S3** — raw JSON archive for audit/replay
- **RDS PostgreSQL** — `cities` dimension + `weather_data` fact table

### Tasks
| Task | Description |
|---|---|
| `extract` | Calls OpenWeatherMap API for each city |
| `archive_to_s3` | Saves raw API response to S3 |
| `transform` | Flattens JSON, validates schema, fills nulls |
| `load` | Upserts into RDS (cities + weather_data) |
| `validate` | Logs DB totals as a final sanity check |
    """,
) as dag:

    t_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    t_archive = PythonOperator(
        task_id='archive_to_s3',
        python_callable=archive_to_s3,
    )

    t_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    t_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    t_validate = PythonOperator(
        task_id='validate',
        python_callable=validate,
    )

    t_extract >> t_archive >> t_transform >> t_load >> t_validate
