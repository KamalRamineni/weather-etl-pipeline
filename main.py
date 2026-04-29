"""
Main ETL Pipeline Orchestrator

This script orchestrates the complete ETL workflow:
E - Extract data from OpenWeatherMap API
T - Transform raw JSON into clean DataFrames
L - Load data into PostgreSQL database

Key Data Engineering Concepts:
- Pipeline orchestration
- Error handling and logging
- Scheduling (can be extended with cron/Airflow)
- Monitoring and observability
"""

import sys
from datetime import datetime
from config.database import DatabaseConfig
from extractors.weather_api import WeatherAPIExtractor
from transformers.weather_transform import WeatherTransformer
from loaders.database_loader import DatabaseLoader


class WeatherETLPipeline:
    """
    Orchestrates the complete weather data ETL pipeline.
    
    Design Pattern: This follows the "Controller" pattern
    - Coordinates between different components
    - Handles errors at pipeline level
    - Provides monitoring and logging
    """
    
    def __init__(self):
        """Initialize ETL components"""
        print("\n" + "=" * 70)
        print("🌤️  WEATHER DATA ETL PIPELINE")
        print("=" * 70)
        
        try:
            # Initialize components
            self.db_config = DatabaseConfig()
            self.extractor = WeatherAPIExtractor()
            self.transformer = WeatherTransformer()
            self.loader = DatabaseLoader(self.db_config)
            
            print("✅ All components initialized successfully")
        
        except Exception as e:
            print(f"❌ Failed to initialize pipeline: {e}")
            sys.exit(1)
    
    def run_pipeline(self, export_csv=False):
        """
        Execute the complete ETL pipeline.
        
        Args:
            export_csv (bool): Whether to export transformed data to CSV
        
        Returns:
            dict: Pipeline execution statistics
        
        Pipeline Flow:
        1. Extract → API calls
        2. Transform → Data cleaning
        3. Load → Database insertion
        4. Validate → Quality checks
        """
        start_time = datetime.now()
        print(f"\n🚀 Pipeline started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        stats = {
            'start_time': start_time,
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'errors': [],
            'success': False
        }
        
        try:
            # ========================================
            # PHASE 1: EXTRACT
            # ========================================
            print("\n" + "-" * 70)
            print("PHASE 1: EXTRACT")
            print("-" * 70)
            
            raw_data = self.extractor.extract_from_config()
            stats['extracted'] = len(raw_data)
            
            if not raw_data:
                raise Exception("No data extracted. Check API key and city configuration.")
            
            print(f"✅ Extracted {len(raw_data)} records from API")
            
            # ========================================
            # PHASE 2: TRANSFORM
            # ========================================
            print("\n" + "-" * 70)
            print("PHASE 2: TRANSFORM")
            print("-" * 70)
            
            transformed_df = self.transformer.transform_batch(raw_data)
            stats['transformed'] = len(transformed_df)
            
            if transformed_df.empty:
                raise Exception("Transformation resulted in empty dataset.")
            
            # Optional: Export to CSV for inspection
            if export_csv:
                self.transformer.export_to_csv(
                    transformed_df,
                    f"weather_data_{start_time.strftime('%Y%m%d_%H%M%S')}.csv"
                )
            
            print(f"✅ Transformed {len(transformed_df)} records")
            
            # ========================================
            # PHASE 3: LOAD
            # ========================================
            print("\n" + "-" * 70)
            print("PHASE 3: LOAD")
            print("-" * 70)
            
            cities_loaded, records_loaded = self.loader.load_complete_batch(transformed_df)
            stats['loaded'] = records_loaded
            
            print(f"✅ Loaded {cities_loaded} cities and {records_loaded} weather records")
            
            # ========================================
            # PHASE 4: VALIDATE
            # ========================================
            print("\n" + "-" * 70)
            print("PHASE 4: VALIDATE")
            print("-" * 70)
            
            # Verify data was loaded correctly
            latest_records = self.loader.get_latest_records(limit=5)
            print(f"📊 Latest {len(latest_records)} records in database:")
            print(latest_records.to_string(index=False))
            
            # Get overall statistics
            db_stats = self.loader.get_city_stats()
            print(f"\n📈 Database Statistics:")
            print(f"  Total cities tracked: {db_stats['cities']}")
            print(f"  Total weather records: {db_stats['weather_records']}")
            print(f"  Data spans: {db_stats['earliest_record']} to {db_stats['latest_record']}")
            
            stats['success'] = True
        
        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            print(f"\n❌ {error_msg}")
            stats['errors'].append(error_msg)
            stats['success'] = False
        
        finally:
            # Calculate execution time
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            stats['end_time'] = end_time
            stats['duration_seconds'] = duration
            
            # Print summary
            self._print_summary(stats)
        
        return stats
    
    def _print_summary(self, stats):
        """Print pipeline execution summary"""
        print("\n" + "=" * 70)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("=" * 70)
        
        print(f"Start time:    {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time:      {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration:      {stats['duration_seconds']:.2f} seconds")
        print(f"\nRecords:")
        print(f"  Extracted:   {stats['extracted']}")
        print(f"  Transformed: {stats['transformed']}")
        print(f"  Loaded:      {stats['loaded']}")
        
        if stats['success']:
            print(f"\n✅ Pipeline completed successfully!")
        else:
            print(f"\n❌ Pipeline failed!")
            for error in stats['errors']:
                print(f"  - {error}")
        
        print("=" * 70 + "\n")
    
    def setup_database(self):
        """
        Initialize database schema.
        Run this once before first pipeline execution.
        """
        print("\n🔧 Setting up database schema...")
        
        try:
            self.db_config.execute_sql_file('sql/create_tables.sql')
            print("✅ Database schema created successfully")
            
            # Verify tables were created
            with self.db_config.get_connection() as conn:
                from sqlalchemy import text
                tables = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)).fetchall()
                
                print(f"\n📋 Created tables:")
                for table in tables:
                    print(f"  - {table[0]}")
        
        except Exception as e:
            print(f"❌ Database setup failed: {e}")
            raise


def main():
    """
    Main entry point for the ETL pipeline.
    
    Usage:
        python main.py              # Run pipeline once
        python main.py --setup      # Setup database schema
        python main.py --export     # Run pipeline and export CSV
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Weather ETL Pipeline')
    parser.add_argument(
        '--setup',
        action='store_true',
        help='Setup database schema (run once before first execution)'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Export transformed data to CSV'
    )
    
    args = parser.parse_args()
    
    # Create pipeline instance
    pipeline = WeatherETLPipeline()
    
    # Setup database if requested
    if args.setup:
        pipeline.setup_database()
        return
    
    # Run pipeline
    stats = pipeline.run_pipeline(export_csv=args.export)
    
    # Exit with appropriate code
    # 0 = success, 1 = failure
    # This is important for scheduling systems (cron, Airflow)
    sys.exit(0 if stats['success'] else 1)


if __name__ == "__main__":
    main()
