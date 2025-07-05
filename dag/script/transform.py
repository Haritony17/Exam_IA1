import pandas as pd
import os
import logging
from config.cities_config import SCORE_PARAMS

def calculate_comfort_scores(input_path, output_path):
    """Calcule les scores de confort météo"""
    logging.info(f"Calculating comfort scores for {input_path}")
    
    df = pd.read_csv(input_path)
    
    # Température score (idéal 22-28°C)
    temp_ideal = SCORE_PARAMS['temp_range']
    df['temp_score'] = df['temp_avg'].apply(
        lambda x: max(0, 5 - abs((x - sum(temp_ideal)/2)/3))
    )
    
    # Pluie score
    df['rain_score'] = df['precip'].apply(
        lambda x: 5 if x == 0 else (3 if x < 5 else 1)
    )
    
    # Vent score
    df['wind_score'] = df['wind_speed'].apply(
        lambda x: 5 if x < 10 else (3 if x < 20 else 1)
    )
    
    # Score total pondéré
    df['comfort_score'] = (
        SCORE_PARAMS['temp_weight'] * df['temp_score'] +
        SCORE_PARAMS['rain_weight'] * df['rain_score'] +
        SCORE_PARAMS['wind_weight'] * df['wind_score']
    )
    
    # Save scored data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logging.info(f"Saved scored data to {output_path}")
    
    return output_path

def create_star_schema(input_path, output_dir):
    """Crée le schéma en étoile"""
    logging.info(f"Creating star schema from {input_path}")
    
    df = pd.read_csv(input_path)
    df['date_extraction'] = pd.to_datetime(df['date_extraction'])
    
    # Dimension Ville
    dim_ville = pd.DataFrame({
        'ville_id': range(1, len(CITIES)+1),
        'ville': CITIES
    })
    
    # Dimension Date
    df['date_id'] = df['date_extraction'].dt.strftime('%Y%m%d').astype(int)
    dim_date = pd.DataFrame({
        'date_id': df['date_id'].unique(),
        'date': pd.to_datetime(df['date_id'].unique(), format='%Y%m%d'),
        'day': pd.to_datetime(df['date_id'].unique(), format='%Y%m%d').day,
        'month': pd.to_datetime(df['date_id'].unique(), format='%Y%m%d').month,
        'year': pd.to_datetime(df['date_id'].unique(), format='%Y%m%d').year,
        'season': pd.to_datetime(df['date_id'].unique(), format='%Y%m%d').month.map(get_season)
    })
    
    # Table de faits
    fact_weather = df.merge(dim_ville, on='ville').merge(
        dim_date, on='date_id'
    )[['ville_id', 'date_id', 'temp_avg', 'temp_min', 'temp_max', 
       'precip', 'wind_speed', 'comfort_score']]
    
    # Sauvegarde
    os.makedirs(output_dir, exist_ok=True)
    dim_ville.to_csv(os.path.join(output_dir, 'dim_ville.csv'), index=False)
    dim_date.to_csv(os.path.join(output_dir, 'dim_date.csv'), index=False)
    fact_weather.to_csv(os.path.join(output_dir, 'fact_weather.csv'), index=False)
    
    logging.info(f"Star schema saved to {output_dir}")
    return output_dir

def get_season(month):
    """Détermine la saison à partir du mois"""
    if month in [12, 1, 2]: return 'Winter'
    if month in [3, 4, 5]: return 'Spring'
    if month in [6, 7, 8]: return 'Summer'
    return 'Autumn'