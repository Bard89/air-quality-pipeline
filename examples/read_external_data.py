#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from datetime import datetime
from src.utils.data_reader import DataReader
from src.infrastructure.data_reference import ExternalDataManager


def main():
    print("=== External Data Reader Example ===\n")
    
    reader = DataReader()
    manager = ExternalDataManager()
    
    print(f"Reading data from: {manager.external_data_path}\n")
    
    try:
        print("1. Reading Japan OpenAQ data (last available)...")
        openaq_data = reader.read_openaq(
            country='JP',
            parameters=['pm25', 'pm10']
        )
        print(f"   Loaded {len(openaq_data)} air quality measurements")
        print(f"   Parameters: {openaq_data['parameter'].unique()}")
        print(f"   Date range: {openaq_data['datetime'].min()} to {openaq_data['datetime'].max()}")
    except FileNotFoundError as e:
        print(f"   Error: {e}")
    
    print()
    
    try:
        print("2. Reading Japan weather data (Jan 2023)...")
        weather_data = reader.read_weather(
            source='openmeteo',
            country='JP',
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2023, 1, 31),
            parameters=['temperature', 'humidity']
        )
        print(f"   Loaded {len(weather_data)} weather measurements")
        print(f"   Parameters: {weather_data['parameter'].unique()[:5]}")
    except FileNotFoundError as e:
        print(f"   Error: {e}")
    
    print()
    
    try:
        print("3. Reading Japan elevation data...")
        elevation_data = reader.read_elevation('JP')
        print(f"   Loaded {len(elevation_data)} elevation points")
        print(f"   Columns: {list(elevation_data.columns)}")
    except FileNotFoundError as e:
        print(f"   Error: {e}")
    
    print()
    
    try:
        print("4. Available data sources summary:")
        for source in ['openaq', 'openmeteo', 'nasapower', 'firms', 'era5', 'terrain']:
            files = manager.list_files(source)
            if files:
                print(f"   {source}: {len(files)} files")
                latest = files[-1]
                size_mb = latest.stat().st_size / (1024 * 1024)
                print(f"      Latest: {latest.name} ({size_mb:.2f} MB)")
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n✅ External data reading demonstration complete!")


if __name__ == "__main__":
    main()