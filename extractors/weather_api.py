import os
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class WeatherAPIExtractor:
    """Extracts current weather data from OpenWeatherMap API."""

    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.rate_limit_delay = 1  # seconds between requests

        if not self.api_key:
            raise ValueError("OPENWEATHER_API_KEY not set in environment variables.")

    def extract_city_weather(self, city_name, retry_count=3):
        params = {
            'q': city_name,
            'appid': self.api_key,
            'units': 'metric',
        }

        for attempt in range(retry_count):
            try:
                print(f"  Fetching data for {city_name}... (attempt {attempt + 1})")
                response = requests.get(self.base_url, params=params, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    data['extraction_timestamp'] = datetime.now().isoformat()
                    data['source'] = 'openweathermap_api'
                    print(f"  Successfully extracted data for {city_name}")
                    return data

                elif response.status_code == 404:
                    print(f"  City not found: {city_name}")
                    return None

                elif response.status_code == 401:
                    print(f"  Invalid API key. Check your .env file.")
                    return None

                else:
                    print(f"  API returned status code {response.status_code}")

            except requests.exceptions.Timeout:
                print(f"  Request timeout for {city_name}")
            except requests.exceptions.ConnectionError:
                print(f"  Connection error for {city_name}")
            except requests.exceptions.RequestException as e:
                print(f"  Request error: {e}")
            except Exception as e:
                print(f"  Unexpected error: {e}")
                return None

            if attempt < retry_count - 1:
                wait_time = 2 ** attempt
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        print(f"  Failed to extract data for {city_name} after {retry_count} attempts")
        return None

    def extract_multiple_cities(self, city_list):
        print(f"\nStarting extraction for {len(city_list)} cities...")
        results = []
        for city in city_list:
            data = self.extract_city_weather(city)
            if data:
                results.append(data)
            time.sleep(self.rate_limit_delay)
        print(f"\nExtraction complete: {len(results)}/{len(city_list)} successful")
        return results

    def extract_from_config(self):
        cities_str = os.getenv('CITIES', 'New York,London,Tokyo')
        city_list = [city.strip() for city in cities_str.split(',')]
        return self.extract_multiple_cities(city_list)
