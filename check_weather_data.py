#!/usr/bin/env python3

import pandas as pd
import sys
from pathlib import Path

def check_weather_data(file_path):
    """Check downloaded weather data"""
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading file: {e}")
        return
    
    # Check if dataframe is empty
    if df.empty:
        print("Warning: The CSV file is empty")
        return
    
    # Check required columns
    required_columns = ['timestamp', 'location_name', 'parameter', 'value', 'unit']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        return
    
    print(f"Weather Data Summary")
    print("=" * 50)
    print(f"File: {Path(file_path).name}")
    print(f"Total measurements: {len(df):,}")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Locations: {df['location_name'].nunique()}")
    print(f"Parameters: {df['parameter'].nunique()}")
    print()
    
    print("Locations:")
    for loc in sorted(df['location_name'].unique()):
        count = len(df[df['location_name'] == loc])
        print(f"  - {loc}: {count:,} measurements")
    print()
    
    print("Parameters:")
    for param in sorted(df['parameter'].unique()):
        param_df = df[df['parameter'] == param]
        count = len(param_df)
        if count > 0:
            avg_value = param_df['value'].mean()
            unit = param_df['unit'].iloc[0]
            print(f"  - {param}: {count:,} measurements, avg: {avg_value:.2f} {unit}")
        else:
            print(f"  - {param}: 0 measurements")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_weather_data(sys.argv[1])
    else:
        # Check the most recent file
        try:
            data_dir = Path("data/nasapower/processed")
            if not data_dir.exists():
                print(f"Directory {data_dir} does not exist")
                return
            
            # Match the actual file naming pattern
            files = list(data_dir.glob("jp_nasapower_weather_*.csv"))
            if files:
                latest = max(files, key=lambda p: p.stat().st_mtime)
                check_weather_data(latest)
            else:
                print("No weather data files found")
        except PermissionError:
            print("Permission denied accessing the data directory")
        except Exception as e:
            print(f"Error accessing files: {e}")