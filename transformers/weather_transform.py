import pandas as pd
from datetime import datetime


class WeatherTransformer:
    """Transforms raw OpenWeatherMap API responses into a clean, flat DataFrame."""

    def __init__(self):
        self.schema = {
            'city_name': str,
            'country_code': str,
            'latitude': float,
            'longitude': float,
            'recorded_at': 'datetime64[ns]',
            'temperature': float,
            'feels_like': float,
            'temp_min': float,
            'temp_max': float,
            'pressure': int,
            'humidity': int,
            'weather_main': str,
            'weather_description': str,
            'wind_speed': float,
            'wind_deg': int,
            'cloudiness': int,
            'visibility': int,
            'rain_1h': float,
            'snow_1h': float,
        }

    def transform_single_record(self, raw_data):
        try:
            main_data = raw_data.get('main', {})
            weather_data = raw_data.get('weather', [{}])[0]
            wind_data = raw_data.get('wind', {})
            clouds_data = raw_data.get('clouds', {})
            rain_data = raw_data.get('rain', {})
            snow_data = raw_data.get('snow', {})
            coord_data = raw_data.get('coord', {})
            sys_data = raw_data.get('sys', {})

            return {
                'city_name': raw_data.get('name'),
                'country_code': sys_data.get('country'),
                'latitude': coord_data.get('lat'),
                'longitude': coord_data.get('lon'),
                'recorded_at': datetime.fromtimestamp(raw_data.get('dt', 0)),
                'temperature': main_data.get('temp'),
                'feels_like': main_data.get('feels_like'),
                'temp_min': main_data.get('temp_min'),
                'temp_max': main_data.get('temp_max'),
                'pressure': main_data.get('pressure'),
                'humidity': main_data.get('humidity'),
                'weather_main': weather_data.get('main'),
                'weather_description': weather_data.get('description'),
                'wind_speed': wind_data.get('speed'),
                'wind_deg': wind_data.get('deg', 0),
                'cloudiness': clouds_data.get('all', 0),
                'visibility': raw_data.get('visibility'),
                'rain_1h': rain_data.get('1h'),
                'snow_1h': snow_data.get('1h'),
            }
        except Exception as e:
            print(f"  Warning: error transforming record: {e}")
            return None

    def transform_batch(self, raw_data_list):
        print(f"\nTransforming {len(raw_data_list)} records...")

        if not raw_data_list:
            print("  No data to transform")
            return pd.DataFrame()

        transformed_records = [
            r for raw in raw_data_list
            if (r := self.transform_single_record(raw)) is not None
        ]

        df = pd.DataFrame(transformed_records)

        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['city_name', 'recorded_at'])
        if len(df) < initial_count:
            print(f"  Removed {initial_count - len(df)} duplicate records")

        # Drop rows missing critical fields
        df = df.dropna(subset=['city_name', 'recorded_at', 'temperature'])

        # Fill optional fields
        df['rain_1h'] = df['rain_1h'].fillna(0.0)
        df['snow_1h'] = df['snow_1h'].fillna(0.0)
        df['wind_deg'] = df['wind_deg'].fillna(0)
        df['visibility'] = df['visibility'].fillna(10000)

        # Enforce schema types
        for column, dtype in self.schema.items():
            if column in df.columns:
                try:
                    if dtype == 'datetime64[ns]':
                        df[column] = pd.to_datetime(df[column])
                    else:
                        df[column] = df[column].astype(dtype)
                except Exception as e:
                    print(f"  Warning: could not convert {column} to {dtype}: {e}")

        # Range validation
        if (df['temperature'] < -100).any() or (df['temperature'] > 60).any():
            print("  Warning: temperature out of expected range")
        if (df['humidity'] < 0).any() or (df['humidity'] > 100).any():
            print("  Warning: humidity out of expected range")

        df['transformed_at'] = datetime.now()

        print(f"  Transformation complete: {len(df)} records ready for loading")
        self._print_quality_summary(df)
        return df

    def _print_quality_summary(self, df):
        print(f"\n  Data Quality Summary:")
        print(f"    Total records: {len(df)}")
        print(f"    Unique cities: {df['city_name'].nunique()}")
        print(f"    Date range: {df['recorded_at'].min()} to {df['recorded_at'].max()}")
        null_counts = df.isnull().sum()
        null_columns = null_counts[null_counts > 0]
        if len(null_columns) == 0:
            print("    Missing values: None")
        else:
            for col, count in null_columns.items():
                print(f"    Missing {col}: {count} ({count/len(df)*100:.1f}%)")

    def export_to_csv(self, df, filename='transformed_weather_data.csv'):
        try:
            df.to_csv(filename, index=False)
            print(f"\n  Data exported to {filename}")
        except Exception as e:
            print(f"  Warning: error exporting to CSV: {e}")
