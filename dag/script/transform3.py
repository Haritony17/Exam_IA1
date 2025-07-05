import pandas as pd
import os
from typing import Dict
import logging

def transform_to_star(date: str) -> Dict[str, str]:
    """
    Transforme les données météo en modèle en étoile :
    - dim_ville
    - dim_date
    - fact_weather

    Args:
        date (str): Date d'extraction (non utilisée ici mais laissée pour compatibilité)

    Returns:
        Dict[str, str]: chemins des fichiers générés
    """
    logging.basicConfig(level=logging.INFO)
    
    input_file = "data/processed/meteo_global.csv"
    output_dir = "data/star_schema"
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Fichier introuvable : {input_file}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Chargement du fichier météo
    meteo = pd.read_csv(input_file)

    # Conversion des dates (ISO 8601 compatible)
    meteo['date_extraction'] = pd.to_datetime(meteo['date_extraction'], format='ISO8601', errors='coerce')

    # Création de dim_ville
    dim_ville = meteo[['ville']].drop_duplicates().reset_index(drop=True)
    dim_ville['ville_id'] = dim_ville.index + 1
    dim_ville_path = os.path.join(output_dir, "dim_ville.csv")
    dim_ville.to_csv(dim_ville_path, index=False)
    
    # Création de dim_date
    meteo['date_id'] = meteo['date_extraction'].dt.strftime('%Y%m%d').astype(int)
    dim_date = meteo[['date_id', 'date_extraction']].drop_duplicates().copy()
    dim_date['day'] = dim_date['date_extraction'].dt.day
    dim_date['month'] = dim_date['date_extraction'].dt.month
    dim_date['year'] = dim_date['date_extraction'].dt.year
    dim_date['quarter'] = dim_date['date_extraction'].dt.quarter
    dim_date['day_of_week'] = dim_date['date_extraction'].dt.dayofweek
    dim_date_path = os.path.join(output_dir, "dim_date.csv")
    dim_date.to_csv(dim_date_path, index=False)

    # Merge pour créer la table de faits
    meteo = meteo.merge(dim_ville, on="ville", how="left")
    fact_weather = meteo[[
        'ville_id', 'date_id', 'temp', 'temp_min', 'temp_max',
        'humidity', 'pressure', 'wind_speed',
        'rain_1h', 'snow_1h', 'clouds', 'weather_main'
    ]]
    
    fact_path = os.path.join(output_dir, "fact_weather.csv")
    fact_weather.to_csv(fact_path, index=False)

    logging.info("✅ Transformation terminée avec succès (modèle en étoile)")
    
    return {
        'dim_ville': dim_ville_path,
        'dim_date': dim_date_path,
        'fact_weather': fact_path
    }
