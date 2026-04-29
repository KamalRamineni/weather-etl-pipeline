"""
Weather Data Transformer Module

This is the TRANSFORM phase of ETL.
Responsibilities:
- Clean raw API data
- Normalize data structure
- Handle missing values
- Convert data types
- Create analytics-ready format

Key Data Engineering Concepts:
- Data quality checks
- Schema validation
- Data normalization
- Handling nulls and defaults
"""

import pandas as pd
from datetime import datetime


class WeatherTransformer:
    """
    Transforms raw weather API data into clean, structured format.
    
    Transformation Strategy:
    1. Extract nested JSON fields
    2. Flatten structure
    3. Apply data types
    4. Handle missing values
    5. Add derived fields
    """
    
    def __init__(self):
        """Initialize transformer with expected schema"""
        
        # Define expected output schema (data types)
        # This ensures data quality and consistency
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
            'snow_1h': float
        }
    
    def transform_single_record(self, raw_data):
        """
        Transform a single weather record from API format to clean format.
        
        Args:
            raw_data (dict): Raw JSON from OpenWeatherMap API
        
        Returns:
            dict: Cleaned and flattened data
        
        Transformation Steps:
        1. Extract nested fields (main, weather, wind, etc.)
        2. Flatten to single-level dictionary
        3. Handle missing optional fields
        4. Convert Unix timestamp to datetime
        """
        try:
            # Extract nested data structures
            # API returns data in nested JSON - we need to flatten it
            main_data = raw_data.get('main', {})
            weather_data = raw_data.get('weather', [{}])[0]
            wind_data = raw_data.get('wind', {})
            clouds_data = raw_data.get('clouds', {})
            rain_data = raw_data.get('rain', {})
            snow_data = raw_data.get('snow', {})
            coord_data = raw_data.get('coord', {})
            sys_data = raw_data.get('sys', {})
            
            # Create flattened record
            # This is the TRANSFORMATION - converting nested API format
            # to flat database-ready format
            transformed = {
                # City information
                'city_name': raw_data.get('name'),
                'country_code': sys_data.get('country'),
                'latitude': coord_data.get('lat'),
                'longitude': coord_data.get('lon'),
                
                # Timestamp - convert Unix timestamp to datetime
                # OpenWeatherMap uses Unix epoch (seconds since 1970-01-01)
                'recorded_at': datetime.fromtimestamp(
                    raw_data.get('dt', 0)
                ),
                
                # Temperature data (already in Celsius due to units=metric)
                'temperature': main_data.get('temp'),
                'feels_like': main_data.get('feels_like'),
                'temp_min': main_data.get('temp_min'),
                'temp_max': main_data.get('temp_max'),
                
                # Atmospheric conditions
                'pressure': main_data.get('pressure'),
                'humidity': main_data.get('humidity'),
                
                # Weather description
                'weather_main': weather_data.get('main'),
                'weather_description': weather_data.get('description'),
                
                # Wind data
                'wind_speed': wind_data.get('speed'),
                'wind_deg': wind_data.get('deg', 0),
                
                # Clouds and visibility
                'cloudiness': clouds_data.get('all', 0),
                'visibility': raw_data.get('visibility'),
                
                # Precipitation (optional - often missing)
                # Using .get() with None default - we'll handle nulls later
                'rain_1h': rain_data.get('1h'),
                'snow_1h': snow_data.get('1h'),
            }
            
            return transformed
        
        except Exception as e:
            print(f"  ⚠️ Error transforming record: {e}")
            return None
    
    def transform_batch(self, raw_data_list):
        """
        Transform multiple weather records into a pandas DataFrame.
        
        Args:
            raw_data_list (list): List of raw API responses
        
        Returns:
            pd.DataFrame: Cleaned data ready for loading
        
        Why use pandas DataFrame?
        - Efficient batch operations
        - Easy data type conversion
        - Built-in null handling
        - Compatible with SQL databases
        """
        print(f"\n🔄 Transforming {len(raw_data_list)} records...")
        
        if not raw_data_list:
            print("  ⚠️ No data to transform")
            return pd.DataFrame()
        
        # Transform each record
        transformed_records = []
        for raw_data in raw_data_list:
            transformed = self.transform_single_record(raw_data)
            if transformed:
                transformed_records.append(transformed)
        
        # Create DataFrame
        df = pd.DataFrame(transformed_records)
        
        # Data Quality Checks
        print(f"  Initial record count: {len(df)}")
        
        # 1. Remove duplicates (same city and time)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['city_name', 'recorded_at'])
        if len(df) < initial_count:
            print(f"  Removed {initial_count - len(df)} duplicate records")
        
        # 2. Handle missing values
        # Strategy: Different approaches for different columns
        
        # Critical fields - drop rows if missing
        critical_fields = ['city_name', 'recorded_at', 'temperature']
        df = df.dropna(subset=critical_fields)
        
        # Optional fields - fill with defaults
        df['rain_1h'] = df['rain_1h'].fillna(0.0)
        df['snow_1h'] = df['snow_1h'].fillna(0.0)
        df['wind_deg'] = df['wind_deg'].fillna(0)
        df['visibility'] = df['visibility'].fillna(10000)  # Default: 10km
        
        # 3. Apply data types (schema enforcement)
        # This ensures consistency and catches data quality issues
        for column, dtype in self.schema.items():
            if column in df.columns:
                try:
                    if dtype == 'datetime64[ns]':
                        df[column] = pd.to_datetime(df[column])
                    else:
                        df[column] = df[column].astype(dtype)
                except Exception as e:
                    print(f"  ⚠️ Error converting {column} to {dtype}: {e}")
        
        # 4. Data validation
        # Check for reasonable value ranges
        validation_errors = []
        
        if (df['temperature'] < -100).any() or (df['temperature'] > 60).any():
            validation_errors.append("Temperature out of range")
        
        if (df['humidity'] < 0).any() or (df['humidity'] > 100).any():
            validation_errors.append("Humidity out of range")
        
        if validation_errors:
            print(f"  ⚠️ Validation warnings: {', '.join(validation_errors)}")
        
        # 5. Add transformation metadata
        df['transformed_at'] = datetime.now()
        
        print(f"  ✅ Transformation complete: {len(df)} records ready for loading")
        
        # Show data quality summary
        self._print_quality_summary(df)
        
        return df
    
    def _print_quality_summary(self, df):
        """Print data quality statistics"""
        print(f"\n  📊 Data Quality Summary:")
        print(f"    Total records: {len(df)}")
        print(f"    Unique cities: {df['city_name'].nunique()}")
        print(f"    Date range: {df['recorded_at'].min()} to {df['recorded_at'].max()}")
        print(f"    Missing values:")
        
        # Count nulls per column
        null_counts = df.isnull().sum()
        null_columns = null_counts[null_counts > 0]
        
        if len(null_columns) == 0:
            print(f"      None! 🎉")
        else:
            for col, count in null_columns.items():
                print(f"      {col}: {count} ({count/len(df)*100:.1f}%)")
    
    def export_to_csv(self, df, filename='transformed_weather_data.csv'):
        """
        Export transformed data to CSV for inspection.
        
        Args:
            df (pd.DataFrame): Transformed data
            filename (str): Output filename
        
        Why export to CSV?
        - Easy to inspect data manually
        - Can import into Excel/Google Sheets
        - Useful for debugging
        """
        try:
            df.to_csv(filename, index=False)
            print(f"\n  💾 Data exported to {filename}")
        except Exception as e:
            print(f"  ⚠️ Error exporting to CSV: {e}")


# Testing and examples
if __name__ == "__main__":
    """
    Test the transformer with sample data
    """
    print("=" * 60)
    print("Weather Data Transformer Test")
    print("=" * 60)
    
    # Sample raw data (what API returns)
    sample_raw_data = {
        "coord": {"lon": -0.1257, "lat": 51.5085},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky"}],
        "base": "stations",
        "main": {
            "temp": 15.5,
            "feels_like": 14.8,
            "temp_min": 13.2,
            "temp_max": 17.1,
            "pressure": 1013,
            "humidity": 72
        },
        "visibility": 10000,
        "wind": {"speed": 3.5, "deg": 230},
        "clouds": {"all": 0},
        "dt": 1704067200,
        "sys": {"country": "GB"},
        "name": "London"
    }
    
    # Create transformer
    transformer = WeatherTransformer()
    
    # Test single record transformation
    print("\n1. Testing single record transformation:")
    transformed = transformer.transform_single_record(sample_raw_data)
    
    if transformed:
        print("\nTransformed record:")
        for key, value in transformed.items():
            print(f"  {key}: {value}")
    
    # Test batch transformation
    print("\n2. Testing batch transformation:")
    batch_data = [sample_raw_data] * 3  # Simulate 3 records
    df = transformer.transform_batch(batch_data)
    
    print("\nDataFrame preview:")
    print(df.head())
    
    print("\n" + "=" * 60)
    print("✅ Transformer test complete!")
    print("=" * 60)
