import pandas as pd
import sys
import os
import logging
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.cities_config import CITIES



def process_historical_data(input_dir, output_path):
    """Charge et fusionne les données historiques locales"""
    logging.info(f"Processing historical data from {input_dir}")
    
    all_data = []
    for city in CITIES:
        file_path = os.path.join(input_dir, f"{city.lower().replace(' ', '_')}.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                df['ville'] = city
                all_data.append(df)
                logging.info(f"Loaded data for {city}")
            except Exception as e:
                logging.error(f"Error loading {file_path}: {str(e)}")
    
    if not all_data:
        raise ValueError("No historical data found")
    
    merged = pd.concat(all_data, ignore_index=True)
    
    # Standardisation des colonnes
    col_mapping = {
        'temperature': 'temp_avg',
        'tavg': 'temp_avg',
        'tmin': 'temp_min',
        'tmax': 'temp_max',
        'prcp': 'precip',
        'wspd': 'wind_speed',
        'date': 'date_extraction',
        'Date': 'date_extraction'
    }
    
    merged = merged.rename(columns={k: v for k, v in col_mapping.items() if k in merged.columns})
    
    # Ensure required columns exist
    for col in ['temp_avg', 'date_extraction']:
        if col not in merged.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Clean dates
    merged['date_extraction'] = pd.to_datetime(merged['date_extraction'], errors='coerce')
    merged = merged.dropna(subset=['date_extraction'])
    
    # Save processed data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    merged.to_csv(output_path, index=False)
    logging.info(f"Saved merged data to {output_path}")
    
    return output_path