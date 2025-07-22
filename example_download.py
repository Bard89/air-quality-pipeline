#!/usr/bin/env python3
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from src.openaq.client import OpenAQClient
from src.openaq.location_finder import LocationFinder
from src.openaq.data_downloader import DataDownloader
from src.core.data_storage import DataStorage


def main():
    load_dotenv()
    api_key = os.getenv('OPENAQ_API_KEY')
    
    if not api_key:
        print("Please set OPENAQ_API_KEY in .env file")
        print("Get your key at: https://explore.openaq.org/register")
        return
    
    storage = DataStorage()
    client = OpenAQClient(api_key, storage)
    finder = LocationFinder(client)
    downloader = DataDownloader(client)
    
    print("Fetching countries...")
    countries = client.get_countries()
    country_list = [(c['code'], c['name']) for c in countries['results'][:10]]
    print("Available countries:", country_list)
    
    country_code = input("\nEnter country code (e.g., VN for Vietnam): ").upper()
    
    country_id = next((c['id'] for c in countries['results'] if c['code'] == country_code), None)
    if not country_id:
        print(f"Country {country_code} not found")
        return
    
    print(f"\nFinding PM2.5 sensors in {country_code}...")
    locations = finder.find_locations_in_country(country_code, {country_code: {'id': country_id}})
    sensors = finder.find_active_sensors(locations, parameter='pm25', min_date='2024-01-01')
    
    print(f"Found {len(sensors)} active PM2.5 sensors")
    
    if not sensors:
        print("No active sensors found")
        return
    
    for i, sensor in enumerate(sensors[:5]):
        print(f"{i+1}. {sensor['location_name']} ({sensor['latitude']:.4f}, {sensor['longitude']:.4f})")
    
    num_sensors = int(input(f"\nHow many sensors to download (1-{min(5, len(sensors))}): "))
    selected_sensors = sensors[:num_sensors]
    
    days = int(input("How many days of data to download: "))
    
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    
    print(f"\nDownloading {days} days of data for {num_sensors} sensors...")
    df = downloader.download_multiple_sensors(selected_sensors, start_date, end_date)
    
    if not df.empty:
        output_file = f"data/openaq/processed/{country_code}_pm25_data.csv"
        df.to_csv(output_file, index=False)
        print(f"\nSaved {len(df)} measurements to {output_file}")
        print(f"Average PM2.5: {df['value'].mean():.2f} µg/m³")


if __name__ == "__main__":
    main()