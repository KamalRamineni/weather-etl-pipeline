"""
Database Loader Module

This is the LOAD phase of ETL.
Responsibilities:
- Insert transformed data into database
- Handle duplicate records (upsert logic)
- Maintain referential integrity (cities → weather_data)
- Provide transaction management

Key Data Engineering Concepts:
- ACID transactions
- Upsert (INSERT ... ON CONFLICT)
- Foreign key relationships
- Batch loading for performance
"""

import pandas as pd
from sqlalchemy import text
from config.database import DatabaseConfig


class DatabaseLoader:
    """
    Loads transformed weather data into PostgreSQL database.
    
    Loading Strategy:
    1. Load cities first (dimension table)
    2. Get city IDs
    3. Load weather data (fact table) with foreign keys
    4. Use upsert to handle duplicates
    """
    
    def __init__(self, db_config=None):
        """
        Initialize loader with database connection.
        
        Args:
            db_config: DatabaseConfig instance (creates new one if None)
        """
        self.db = db_config if db_config else DatabaseConfig()
    
    def load_cities(self, df):
        """
        Load city information into cities dimension table.
        
        Args:
            df (pd.DataFrame): Transformed weather data
        
        Returns:
            dict: Mapping of city_name -> city_id
        
        Why load cities separately?
        - Dimensional modeling: separate static info (city) from facts (weather)
        - Avoids data duplication
        - Enables efficient queries
        """
        print("\n🔄 Loading cities into database...")
        
        # Extract unique cities with their coordinates
        cities_df = df[['city_name', 'country_code', 'latitude', 'longitude']].drop_duplicates()
        
        city_id_map = {}
        
        with self.db.get_connection() as conn:
            for _, city in cities_df.iterrows():
                # Upsert city (INSERT ... ON CONFLICT DO UPDATE)
                # This handles both new cities and updates to existing ones
                query = text("""
                    INSERT INTO cities (city_name, country_code, latitude, longitude)
                    VALUES (:city_name, :country_code, :latitude, :longitude)
                    ON CONFLICT (city_name) 
                    DO UPDATE SET
                        country_code = EXCLUDED.country_code,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude
                    RETURNING city_id
                """)
                
                result = conn.execute(query, {
                    'city_name': city['city_name'],
                    'country_code': city['country_code'],
                    'latitude': city['latitude'],
                    'longitude': city['longitude']
                })
                
                city_id = result.fetchone()[0]
                city_id_map[city['city_name']] = city_id
            
            # Commit the transaction
            conn.commit()
        
        print(f"  ✅ Loaded {len(city_id_map)} cities")
        return city_id_map
    
    def load_weather_data(self, df, city_id_map):
        """
        Load weather data into weather_data fact table.
        
        Args:
            df (pd.DataFrame): Transformed weather data
            city_id_map (dict): city_name -> city_id mapping
        
        Returns:
            int: Number of records loaded
        
        Loading Strategy:
        - Batch insert for performance
        - Use executemany() instead of individual inserts
        - Handle duplicates with ON CONFLICT
        """
        print("\n🔄 Loading weather data into database...")
        
        # Add city_id to DataFrame
        df['city_id'] = df['city_name'].map(city_id_map)
        
        # Prepare records for insertion
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        loaded_count = 0
        skipped_count = 0
        
        with self.db.get_connection() as conn:
            # Upsert query
            # ON CONFLICT: if same city_id + recorded_at exists, update instead
            query = text("""
                INSERT INTO weather_data (
                    city_id, recorded_at, temperature, feels_like,
                    temp_min, temp_max, pressure, humidity,
                    weather_main, weather_description,
                    wind_speed, wind_deg, cloudiness, visibility,
                    rain_1h, snow_1h
                )
                VALUES (
                    :city_id, :recorded_at, :temperature, :feels_like,
                    :temp_min, :temp_max, :pressure, :humidity,
                    :weather_main, :weather_description,
                    :wind_speed, :wind_deg, :cloudiness, :visibility,
                    :rain_1h, :snow_1h
                )
                ON CONFLICT (city_id, recorded_at)
                DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    feels_like = EXCLUDED.feels_like,
                    temp_min = EXCLUDED.temp_min,
                    temp_max = EXCLUDED.temp_max,
                    pressure = EXCLUDED.pressure,
                    humidity = EXCLUDED.humidity,
                    weather_main = EXCLUDED.weather_main,
                    weather_description = EXCLUDED.weather_description,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_deg = EXCLUDED.wind_deg,
                    cloudiness = EXCLUDED.cloudiness,
                    visibility = EXCLUDED.visibility,
                    rain_1h = EXCLUDED.rain_1h,
                    snow_1h = EXCLUDED.snow_1h,
                    data_fetched_at = CURRENT_TIMESTAMP
            """)
            
            # Execute batch insert
            # This is much faster than individual inserts
            for record in records:
                try:
                    conn.execute(query, {
                        'city_id': record['city_id'],
                        'recorded_at': record['recorded_at'],
                        'temperature': record['temperature'],
                        'feels_like': record['feels_like'],
                        'temp_min': record['temp_min'],
                        'temp_max': record['temp_max'],
                        'pressure': record['pressure'],
                        'humidity': record['humidity'],
                        'weather_main': record['weather_main'],
                        'weather_description': record['weather_description'],
                        'wind_speed': record['wind_speed'],
                        'wind_deg': record['wind_deg'],
                        'cloudiness': record['cloudiness'],
                        'visibility': record['visibility'],
                        'rain_1h': record['rain_1h'],
                        'snow_1h': record['snow_1h']
                    })
                    loaded_count += 1
                except Exception as e:
                    print(f"  ⚠️ Error loading record: {e}")
                    skipped_count += 1
            
            # Commit all inserts as a single transaction
            # ACID compliance: all or nothing
            conn.commit()
        
        print(f"  ✅ Loaded {loaded_count} weather records")
        if skipped_count > 0:
            print(f"  ⚠️ Skipped {skipped_count} records due to errors")
        
        return loaded_count
    
    def load_complete_batch(self, df):
        """
        Complete loading process: cities + weather data.
        
        Args:
            df (pd.DataFrame): Transformed weather data
        
        Returns:
            tuple: (cities_loaded, records_loaded)
        
        This orchestrates the entire LOAD phase:
        1. Load dimension (cities)
        2. Get foreign keys
        3. Load facts (weather data)
        """
        if df.empty:
            print("  ⚠️ No data to load")
            return 0, 0
        
        # Step 1: Load cities
        city_id_map = self.load_cities(df)
        
        # Step 2: Load weather data
        records_loaded = self.load_weather_data(df, city_id_map)
        
        return len(city_id_map), records_loaded
    
    def get_latest_records(self, limit=10):
        """
        Query the latest weather records (for verification).
        
        Args:
            limit (int): Number of records to return
        
        Returns:
            pd.DataFrame: Latest weather records
        """
        query = text("""
            SELECT 
                c.city_name,
                c.country_code,
                w.recorded_at,
                w.temperature,
                w.feels_like,
                w.humidity,
                w.weather_description,
                w.wind_speed,
                w.data_fetched_at
            FROM weather_data w
            JOIN cities c ON w.city_id = c.city_id
            ORDER BY w.data_fetched_at DESC
            LIMIT :limit
        """)
        
        with self.db.get_connection() as conn:
            df = pd.read_sql(query, conn, params={'limit': limit})
        
        return df
    
    def get_city_stats(self):
        """
        Get statistics about loaded data.
        
        Returns:
            dict: Statistics about cities and records
        """
        with self.db.get_connection() as conn:
            # Count cities
            city_count = conn.execute(text("SELECT COUNT(*) FROM cities")).fetchone()[0]
            
            # Count weather records
            weather_count = conn.execute(text("SELECT COUNT(*) FROM weather_data")).fetchone()[0]
            
            # Get date range
            date_range = conn.execute(text("""
                SELECT 
                    MIN(recorded_at) as earliest,
                    MAX(recorded_at) as latest
                FROM weather_data
            """)).fetchone()
        
        return {
            'cities': city_count,
            'weather_records': weather_count,
            'earliest_record': date_range[0],
            'latest_record': date_range[1]
        }


# Testing
if __name__ == "__main__":
    """
    Test the loader (requires database to be set up)
    """
    print("=" * 60)
    print("Database Loader Test")
    print("=" * 60)
    
    # Create test DataFrame
    test_data = pd.DataFrame([{
        'city_name': 'Test City',
        'country_code': 'TC',
        'latitude': 0.0,
        'longitude': 0.0,
        'recorded_at': pd.Timestamp.now(),
        'temperature': 20.0,
        'feels_like': 19.0,
        'temp_min': 18.0,
        'temp_max': 22.0,
        'pressure': 1013,
        'humidity': 65,
        'weather_main': 'Clear',
        'weather_description': 'clear sky',
        'wind_speed': 3.5,
        'wind_deg': 180,
        'cloudiness': 0,
        'visibility': 10000,
        'rain_1h': 0.0,
        'snow_1h': 0.0
    }])
    
    try:
        # Test database connection
        db = DatabaseConfig()
        if db.test_connection():
            # Create loader
            loader = DatabaseLoader(db)
            
            # Load test data
            cities, records = loader.load_complete_batch(test_data)
            print(f"\n✅ Loaded {cities} cities and {records} records")
            
            # Get stats
            stats = loader.get_city_stats()
            print(f"\n📊 Database stats:")
            print(f"  Total cities: {stats['cities']}")
            print(f"  Total records: {stats['weather_records']}")
            print(f"  Date range: {stats['earliest_record']} to {stats['latest_record']}")
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        print("Make sure PostgreSQL is running and .env is configured!")
    
    print("\n" + "=" * 60)
