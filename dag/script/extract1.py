import os
import requests
import pandas as pd
from datetime import datetime
import logging

def extract_meteo(city: str, api_key: str, date: str) -> str:
    """
    Enhanced weather data extraction with better error handling and logging
    
    Returns:
        str: Path to the saved file
    """
    try:
        # API Configuration
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }
        
        # Request with timeout and retry logic
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract more comprehensive weather data
        weather_data = {
            'ville': city,
            'date_extraction': datetime.utcnow().isoformat(),
            'temp': data['main']['temp'],
            'temp_min': data['main']['temp_min'],
            'temp_max': data['main']['temp_max'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind'].get('speed', 0),
            'wind_deg': data['wind'].get('deg', None),
            'rain_1h': data.get('rain', {}).get('1h', 0),
            'snow_1h': data.get('snow', {}).get('1h', 0),
            'clouds': data['clouds']['all'],
            'weather_main': data['weather'][0]['main'],
            'weather_desc': data['weather'][0]['description'],
            'timezone': data.get('timezone', 0),
            'sunrise': datetime.utcfromtimestamp(data['sys']['sunrise']).isoformat(),
            'sunset': datetime.utcfromtimestamp(data['sys']['sunset']).isoformat()
        }
        
        # Create directory structure
        os.makedirs(f"data/raw/{date}", exist_ok=True)
        file_path = f"data/raw/{date}/meteo_{city.lower().replace(' ', '_')}.csv"
        
        # Save to CSV
        pd.DataFrame([weather_data]).to_csv(file_path, index=False)
        
        logging.info(f"Successfully extracted data for {city}")
        return file_path
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {city}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error for {city}: {str(e)}")
        raise