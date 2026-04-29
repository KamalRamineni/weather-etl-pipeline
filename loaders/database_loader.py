import pandas as pd
from sqlalchemy import text
from config.database import DatabaseConfig


class DatabaseLoader:
    """Loads transformed weather data into PostgreSQL (cities dimension + weather_data fact)."""

    def __init__(self, db_config=None):
        self.db = db_config if db_config else DatabaseConfig()

    def load_cities(self, df):
        print("\nLoading cities into database...")
        cities_df = df[['city_name', 'country_code', 'latitude', 'longitude']].drop_duplicates()
        city_id_map = {}

        with self.db.engine.begin() as conn:
            for _, city in cities_df.iterrows():
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
                    'longitude': city['longitude'],
                })
                city_id_map[city['city_name']] = result.fetchone()[0]

        print(f"  Loaded {len(city_id_map)} cities")
        return city_id_map

    def load_weather_data(self, df, city_id_map):
        print("\nLoading weather data into database...")
        df = df.copy()
        df['city_id'] = df['city_name'].map(city_id_map)
        records = df.to_dict('records')

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

        loaded_count = 0
        skipped_count = 0

        with self.db.engine.begin() as conn:
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
                        'snow_1h': record['snow_1h'],
                    })
                    loaded_count += 1
                except Exception as e:
                    print(f"  Warning: error loading record: {e}")
                    skipped_count += 1

        print(f"  Loaded {loaded_count} weather records")
        if skipped_count:
            print(f"  Skipped {skipped_count} records due to errors")
        return loaded_count

    def load_complete_batch(self, df):
        if df.empty:
            print("  No data to load")
            return 0, 0
        city_id_map = self.load_cities(df)
        records_loaded = self.load_weather_data(df, city_id_map)
        return len(city_id_map), records_loaded

    def get_latest_records(self, limit=10):
        query = text("""
            SELECT
                c.city_name, c.country_code, w.recorded_at,
                w.temperature, w.feels_like, w.humidity,
                w.weather_description, w.wind_speed, w.data_fetched_at
            FROM weather_data w
            JOIN cities c ON w.city_id = c.city_id
            ORDER BY w.data_fetched_at DESC
            LIMIT :limit
        """)
        with self.db.get_connection() as conn:
            return pd.read_sql(query, conn, params={'limit': limit})

    def get_city_stats(self):
        with self.db.get_connection() as conn:
            city_count = conn.execute(text("SELECT COUNT(*) FROM cities")).fetchone()[0]
            weather_count = conn.execute(text("SELECT COUNT(*) FROM weather_data")).fetchone()[0]
            date_range = conn.execute(text(
                "SELECT MIN(recorded_at), MAX(recorded_at) FROM weather_data"
            )).fetchone()
        return {
            'cities': city_count,
            'weather_records': weather_count,
            'earliest_record': date_range[0],
            'latest_record': date_range[1],
        }
