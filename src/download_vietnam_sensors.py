import os
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

from src.openaq.client import OpenAQClient
from src.openaq.location_finder import LocationFinder
from src.openaq.data_downloader import DataDownloader
from src.core.data_storage import DataStorage


def load_country_mapping():
    with open('config/country_mapping.json', 'r') as f:
        return json.load(f)


def main():
    load_dotenv()
    api_key = os.getenv('OPENAQ_API_KEY')
    if not api_key:
        print("Error: OPENAQ_API_KEY not found")
        return
    
    storage = DataStorage()
    client = OpenAQClient(api_key, storage)
    finder = LocationFinder(client)
    downloader = DataDownloader(client)
    
    country_mapping = load_country_mapping()
    
    print("Finding all locations in Vietnam...")
    locations = finder.find_locations_in_country('VN', country_mapping)
    print(f"Found {len(locations)} locations")
    
    client.save_response({'results': locations}, 'vietnam_all_locations')
    
    print("\nFinding active PM2.5 sensors...")
    active_sensors = finder.find_active_sensors(locations, parameter='pm25', min_date='2024-01-01')
    print(f"Found {len(active_sensors)} active PM2.5 sensors")
    
    print("\nActive sensors by city:")
    cities = {}
    for sensor in active_sensors:
        city = sensor['city'] or 'Unknown'
        if city not in cities:
            cities[city] = []
        cities[city].append(sensor)
    
    for city, sensors in sorted(cities.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"\n{city}: {len(sensors)} sensors")
        for s in sensors[:3]:
            print(f"  - {s['location_name']} ({s['latitude']:.4f}, {s['longitude']:.4f})")
    
    hanoi_sensors = cities.get('Hanoi', [])[:5]
    
    if hanoi_sensors:
        print(f"\nDownloading data for {len(hanoi_sensors)} Hanoi sensors...")
        
        start_date = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 6, 30, 23, 59, 59, tzinfo=timezone.utc)
        
        df = downloader.download_multiple_sensors(hanoi_sensors, start_date, end_date)
        
        if not df.empty:
            output_path = storage.get_processed_dir('openaq') / 'vietnam_sensors_june_2024.csv'
            df.to_csv(output_path, index=False)
            
            print(f"\nSaved {len(df)} measurements to {output_path}")
            print(f"Unique sensors: {df['sensor_id'].nunique()}")
            print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
            
            sensor_summary = df.groupby(['sensor_id', 'location_name', 'latitude', 'longitude'])['value'].agg(['count', 'mean'])
            print("\nSensor summary:")
            print(sensor_summary)


if __name__ == "__main__":
    main()