import sqlite3
import pandas as pd
import os
import logging
from config.cities_config import MONTH_NAMES

def load_to_database(star_schema_dir, db_path):
    """Charge les données dans SQLite"""
    logging.info(f"Loading star schema from {star_schema_dir} to {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Chargement des dimensions
    dim_ville = pd.read_csv(os.path.join(star_schema_dir, 'dim_ville.csv'))
    dim_date = pd.read_csv(os.path.join(star_schema_dir, 'dim_date.csv'))
    fact_weather = pd.read_csv(os.path.join(star_schema_dir, 'fact_weather.csv'))
    
    # Création des tables
    dim_ville.to_sql('dim_ville', conn, if_exists='replace', index=False)
    dim_date.to_sql('dim_date', conn, if_exists='replace', index=False)
    fact_weather.to_sql('fact_weather', conn, if_exists='replace', index=False)
    
    # Création des index
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_ville ON fact_weather(ville_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_weather(date_id)")
    
    conn.close()
    logging.info("Data successfully loaded into database")
    return db_path

def generate_recommendations(input_path, output_dir):
    """Génère les recommandations par ville/mois"""
    logging.info(f"Generating recommendations from {input_path}")
    
    df = pd.read_csv(input_path)
    df['date_extraction'] = pd.to_datetime(df['date_extraction'])
    df['month'] = df['date_extraction'].dt.month
    
    recommendations = df.groupby(['ville', 'month']).agg({
        'temp_avg': 'mean',
        'precip': 'mean',
        'wind_speed': 'mean',
        'comfort_score': 'mean'
    }).reset_index()
    
    recommendations['rank'] = recommendations.groupby('ville')['comfort_score'].rank(ascending=False)
    recommendations['month_name'] = recommendations['month'].map(MONTH_NAMES)
    
    # Sauvegarde
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, 'recommendations.csv')
    recommendations.to_csv(full_path, index=False)
    
    logging.info(f"Recommendations saved to {full_path}")
    return full_path