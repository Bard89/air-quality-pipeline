#!/usr/bin/env python3

import pandas as pd
import sys
from pathlib import Path

def check_weather_data(file_path):
    """Check downloaded weather data"""
    df = pd.read_csv(file_path)
    
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
        count = len(df[df['parameter'] == param])
        avg_value = df[df['parameter'] == param]['value'].mean()
        unit = df[df['parameter'] == param]['unit'].iloc[0]
        print(f"  - {param}: {count:,} measurements, avg: {avg_value:.2f} {unit}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_weather_data(sys.argv[1])
    else:
        # Check the most recent file
        files = list(Path("data/nasapower/processed").glob("jp_nasapower_weather_parallel_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            check_weather_data(latest)
        else:
            print("No weather data files found")