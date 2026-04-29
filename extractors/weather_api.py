"""
Weather API Extractor Module

This is the EXTRACT phase of ETL.
Responsibilities:
- Fetch data from external API
- Handle errors (network issues, API limits, invalid responses)
- Return raw data for transformation

Key Data Engineering Concepts:
- API authentication
- Rate limiting
- Error handling and retries
- Idempotency (safe to run multiple times)
"""

import os
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class WeatherAPIExtractor:
    """
    Extracts weather data from OpenWeatherMap API.
    
    API Documentation: https://openweathermap.org/current
    """
    
    def __init__(self):
        """Initialize API configuration"""
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        
        # Validate API key exists
        if not self.api_key:
            raise ValueError(
                "OPENWEATHER_API_KEY not found in environment variables. "
                "Please check your .env file."
            )
        
        # API rate limits (free tier: 60 calls/minute)
        self.rate_limit_delay = 1  # seconds between requests
    
    def extract_city_weather(self, city_name, retry_count=3):
        """
        Extract weather data for a single city.
        
        Args:
            city_name (str): Name of the city
            retry_count (int): Number of retry attempts on failure
        
        Returns:
            dict: Raw weather data from API, or None if failed
        
        Data Engineering Best Practices:
        - Retry logic for transient failures
        - Logging extraction timestamp
        - Preserving raw data (don't transform here!)
        """
        params = {
            'q': city_name,
            'appid': self.api_key,
            'units': 'metric'  # Celsius, meters/sec
        }
        
        for attempt in range(retry_count):
            try:
                print(f"  Fetching data for {city_name}... (attempt {attempt + 1})")
                
                # Make HTTP GET request
                response = requests.get(
                    self.base_url,
                    params=params,
                    timeout=10  # Timeout after 10 seconds
                )
                
                # Check HTTP status code
                # 200 = success, 401 = bad API key, 404 = city not found
                if response.status_code == 200:
                    data = response.json()
                    
                    # Add extraction metadata
                    # This helps with debugging and data lineage
                    data['extraction_timestamp'] = datetime.now().isoformat()
                    data['source'] = 'openweathermap_api'
                    
                    print(f"  ✅ Successfully extracted data for {city_name}")
                    return data
                
                elif response.status_code == 404:
                    print(f"  ⚠️ City not found: {city_name}")
                    return None
                
                elif response.status_code == 401:
                    print(f"  ❌ Invalid API key. Check your .env file.")
                    return None
                
                else:
                    print(f"  ⚠️ API returned status code {response.status_code}")
                    # Fall through to retry logic
            
            except requests.exceptions.Timeout:
                print(f"  ⚠️ Request timeout for {city_name}")
            
            except requests.exceptions.ConnectionError:
                print(f"  ⚠️ Connection error for {city_name}")
            
            except requests.exceptions.RequestException as e:
                print(f"  ⚠️ Request error: {e}")
            
            except Exception as e:
                print(f"  ❌ Unexpected error: {e}")
                return None
            
            # Wait before retry (exponential backoff)
            if attempt < retry_count - 1:
                wait_time = 2 ** attempt  # 1s, 2s, 4s
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        print(f"  ❌ Failed to extract data for {city_name} after {retry_count} attempts")
        return None
    
    def extract_multiple_cities(self, city_list):
        """
        Extract weather data for multiple cities.
        
        Args:
            city_list (list): List of city names
        
        Returns:
            list: List of weather data dictionaries
        
        Note: This is a batch extraction pattern.
        In production, you might use parallel processing for better performance.
        """
        print(f"\n🔄 Starting extraction for {len(city_list)} cities...")
        
        results = []
        
        for city in city_list:
            data = self.extract_city_weather(city)
            
            if data:
                results.append(data)
            
            # Respect rate limits
            time.sleep(self.rate_limit_delay)
        
        print(f"\n✅ Extraction complete: {len(results)}/{len(city_list)} successful")
        return results
    
    def extract_from_config(self):
        """
        Extract weather data for cities defined in .env file.
        
        Returns:
            list: List of weather data dictionaries
        """
        cities_str = os.getenv('CITIES', 'New York,London,Tokyo')
        city_list = [city.strip() for city in cities_str.split(',')]
        
        return self.extract_multiple_cities(city_list)


# Example usage and testing
if __name__ == "__main__":
    """
    This section runs when you execute: python extractors/weather_api.py
    Great for testing your extractor independently!
    """
    
    print("=" * 60)
    print("Weather API Extractor Test")
    print("=" * 60)
    
    # Create extractor instance
    extractor = WeatherAPIExtractor()
    
    # Test with a single city
    print("\n1. Testing single city extraction:")
    test_data = extractor.extract_city_weather("London")
    
    if test_data:
        print("\nSample data structure:")
        print(f"  City: {test_data.get('name')}")
        print(f"  Temperature: {test_data.get('main', {}).get('temp')}°C")
        print(f"  Weather: {test_data.get('weather', [{}])[0].get('description')}")
        print(f"  Extraction time: {test_data.get('extraction_timestamp')}")
    
    # Test with multiple cities
    print("\n2. Testing multiple cities extraction:")
    test_cities = ["New York", "Tokyo", "Sydney"]
    results = extractor.extract_multiple_cities(test_cities)
    
    print(f"\nExtracted {len(results)} city weather records")
    
    print("\n" + "=" * 60)
    print("✅ Extractor test complete!")
    print("=" * 60)
