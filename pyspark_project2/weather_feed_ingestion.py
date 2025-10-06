from pyspark.sql import DataFrame, functions as F
import requests
from core.base_processor import BaseProcessor

class WeatherFeedIngestion(BaseProcessor):
    """
    Module: 1.5 - Pulls weather and ocean current data via API
    Metadata:
      - Version: 1.1
      - API: NOAA, OpenWeather, Marine Weather Services
      - Update Frequency: Hourly
    """
    
    def fetch_weather_data(self, api_config: Dict) -> DataFrame:
        """Fetch weather data from external APIs"""
        weather_data = []
        
        for location in api_config.get("locations", []):
            try:
                response = requests.get(
                    api_config["base_url"],
                    params={
                        "lat": location["latitude"],
                        "lon": location["longitude"],
                        "appid": api_config["api_key"]
                    }
                )
                data = response.json()
                weather_data.append({
                    "location_id": location["id"],
                    "latitude": location["latitude"],
                    "longitude": location["longitude"],
                    "temperature": data["main"]["temp"],
                    "wind_speed": data["wind"]["speed"],
                    "wind_direction": data["wind"].get("deg"),
                    "wave_height": data.get("waves", {}).get("height"),
                    "visibility": data.get("visibility"),
                    "timestamp": data["dt"]
                })
            except Exception as e:
                self.logger.warning(f"Weather data fetch failed for {location}: {e}")
        
        # Convert to DataFrame
        return self.spark.createDataFrame(weather_data) \
            .withColumn("fetch_timestamp", F.current_timestamp())