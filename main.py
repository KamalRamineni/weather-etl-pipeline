import logging
import sys
from datetime import datetime

from config.database import DatabaseConfig
from extractors.weather_api import WeatherAPIExtractor
from transformers.weather_transform import WeatherTransformer
from loaders.database_loader import DatabaseLoader
from loaders.s3_loader import S3Loader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class WeatherETLPipeline:
    """Orchestrates the Extract → S3 archive → Transform → Load → Validate pipeline."""

    def __init__(self):
        logger.info("Initializing Weather ETL Pipeline")
        try:
            self.db_config = DatabaseConfig()
            self.extractor = WeatherAPIExtractor()
            self.transformer = WeatherTransformer()
            self.loader = DatabaseLoader(self.db_config)
            self.s3_loader = S3Loader()
            logger.info("All components initialized")
        except Exception as e:
            logger.error("Initialization failed: %s", e)
            sys.exit(1)

    def run_pipeline(self, export_csv=False):
        """
        Run the full ETL pipeline.

        Returns:
            dict: Execution stats (success, counts, s3_key, duration).
        """
        start_time = datetime.utcnow()
        logger.info("Pipeline run started — %s UTC", start_time.strftime('%Y-%m-%d %H:%M:%S'))

        stats = {
            'start_time': start_time,
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            's3_key': None,
            'errors': [],
            'success': False,
        }

        try:
            # ── PHASE 1: EXTRACT ──────────────────────────────────────────
            logger.info("Phase 1: Extract")
            raw_data = self.extractor.extract_from_config()
            stats['extracted'] = len(raw_data)
            if not raw_data:
                raise RuntimeError("No data extracted — check API key and CITIES config.")
            logger.info("Extracted %d records", len(raw_data))

            # ── PHASE 1b: ARCHIVE RAW DATA TO S3 ─────────────────────────
            logger.info("Archiving raw data to S3")
            stats['s3_key'] = self.s3_loader.upload_raw_data(raw_data, start_time)

            # ── PHASE 2: TRANSFORM ────────────────────────────────────────
            logger.info("Phase 2: Transform")
            transformed_df = self.transformer.transform_batch(raw_data)
            stats['transformed'] = len(transformed_df)
            if transformed_df.empty:
                raise RuntimeError("Transformation returned empty dataset.")
            if export_csv:
                self.transformer.export_to_csv(
                    transformed_df,
                    f"weather_data_{start_time.strftime('%Y%m%d_%H%M%S')}.csv",
                )
            logger.info("Transformed %d records", len(transformed_df))

            # ── PHASE 3: LOAD ─────────────────────────────────────────────
            logger.info("Phase 3: Load")
            cities_loaded, records_loaded = self.loader.load_complete_batch(transformed_df)
            stats['loaded'] = records_loaded
            logger.info("Loaded %d cities, %d weather records", cities_loaded, records_loaded)

            # ── PHASE 4: VALIDATE ─────────────────────────────────────────
            logger.info("Phase 4: Validate")
            latest = self.loader.get_latest_records(limit=5)
            logger.info("Latest records:\n%s", latest.to_string(index=False))
            db_stats = self.loader.get_city_stats()
            logger.info(
                "DB totals — cities: %d, records: %d, range: %s → %s",
                db_stats['cities'],
                db_stats['weather_records'],
                db_stats['earliest_record'],
                db_stats['latest_record'],
            )

            stats['success'] = True

        except Exception as e:
            logger.error("Pipeline error: %s", e)
            stats['errors'].append(str(e))

        finally:
            end_time = datetime.utcnow()
            stats['end_time'] = end_time
            stats['duration_seconds'] = (end_time - start_time).total_seconds()
            self._log_summary(stats)

        return stats

    def _log_summary(self, stats):
        status = "SUCCESS" if stats['success'] else "FAILED"
        logger.info(
            "Run %s | %.2fs | extracted=%d transformed=%d loaded=%d s3=%s",
            status,
            stats['duration_seconds'],
            stats['extracted'],
            stats['transformed'],
            stats['loaded'],
            stats.get('s3_key', 'none'),
        )
        if not stats['success']:
            for err in stats['errors']:
                logger.error("  %s", err)

    def setup_database(self):
        """Create the DB and schema. Run once before the first pipeline execution."""
        logger.info("Setting up database")
        try:
            # Step 1: create the database itself (connect to default 'postgres' db)
            self._create_database_if_not_exists()

            # Step 2: create tables inside weather_db
            self.db_config.execute_sql_file('sql/create_tables.sql')
            with self.db_config.get_connection() as conn:
                from sqlalchemy import text
                tables = conn.execute(text(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                )).fetchall()
            logger.info("Schema ready — tables: %s", [t[0] for t in tables])
        except Exception as e:
            logger.error("Schema setup failed: %s", e)
            raise

    def _create_database_if_not_exists(self):
        """Connect to the default 'postgres' DB and create weather_db if missing."""
        from sqlalchemy import create_engine, text
        from urllib.parse import quote_plus

        cfg = self.db_config
        encoded_user = quote_plus(cfg.user)
        encoded_password = quote_plus(cfg.password)
        admin_url = (
            f"postgresql://{encoded_user}:{encoded_password}@"
            f"{cfg.host}:{cfg.port}/postgres"
        )
        engine = create_engine(
            admin_url,
            isolation_level="AUTOCOMMIT",
            connect_args={"sslmode": "require"},
        )
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": cfg.database},
            ).fetchone()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{cfg.database}"'))
                logger.info("Created database: %s", cfg.database)
            else:
                logger.info("Database already exists: %s", cfg.database)
        engine.dispose()


def main():
    """
    Entry points:
        python main.py           # run pipeline once
        python main.py --setup   # create DB schema (run once)
        python main.py --export  # run pipeline + export CSV
    """
    import argparse

    parser = argparse.ArgumentParser(description='Weather ETL Pipeline')
    parser.add_argument('--setup', action='store_true', help='Create DB schema')
    parser.add_argument('--export', action='store_true', help='Export CSV after transform')
    args = parser.parse_args()

    pipeline = WeatherETLPipeline()

    if args.setup:
        pipeline.setup_database()
        return

    stats = pipeline.run_pipeline(export_csv=args.export)
    sys.exit(0 if stats['success'] else 1)


if __name__ == "__main__":
    main()
