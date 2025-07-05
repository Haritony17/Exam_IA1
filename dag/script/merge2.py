import pandas as pd
import os
from datetime import datetime
import logging
from typing import List

def merge_files(date: str) -> str:
    """Enhanced merge function with data validation"""
    input_dir = f"data/raw/{date}"
    output_file = "data/processed/meteo_global.csv"
    
    # Validate input directory
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Input directory {input_dir} does not exist")
    
    # Read all city files
    city_files = [
        f for f in os.listdir(input_dir) 
        if f.startswith('meteo_') and f.endswith('.csv')
    ]
    
    if not city_files:
        raise ValueError(f"No city data files found in {input_dir}")
    
    # Load and validate each file
    dfs: List[pd.DataFrame] = []
    for file in city_files:
        try:
            df = pd.read_csv(f"{input_dir}/{file}")
            required_columns = ['ville', 'date_extraction', 'temp', 'humidity']
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"Missing required columns in {file}")
            dfs.append(df)
        except Exception as e:
            logging.warning(f"Skipping invalid file {file}: {str(e)}")
            continue
    
    if not dfs:
        raise ValueError("No valid city data to merge")
    
    # Concatenate and deduplicate
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Data quality checks
    merged_df = merged_df[
        (merged_df['temp'].between(-50, 60)) &
        (merged_df['humidity'].between(0, 100))
    ].drop_duplicates(
        subset=['ville', 'date_extraction'],
        keep='last'
    )
    
    # Save merged data
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Append to existing file or create new
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        final_df = pd.concat([existing_df, merged_df], ignore_index=True)
    else:
        final_df = merged_df
    
    final_df.to_csv(output_file, index=False)
    logging.info(f"Merged data saved to {output_file} with {len(final_df)} records")
    return output_file