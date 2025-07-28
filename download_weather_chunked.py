#!/usr/bin/env python3

import subprocess
import sys
from datetime import datetime
import time

def download_chunk(start_date, end_date, max_locations=10, max_concurrent=10):
    """Download weather data for a specific date range"""
    print(f"\nDownloading {start_date} to {end_date}...")
    
    cmd = [
        "python", "download_weather_parallel.py",
        "--source", "nasapower",
        "--country", "JP",
        "--max-locations", str(max_locations),
        "--max-concurrent", str(max_concurrent),
        "--start", start_date,
        "--end", end_date,
        "--no-analyze"
    ]
    
    start_time = time.time()
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed/60:.1f} minutes")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed: {e.stderr}")
        return False
    except KeyboardInterrupt:
        print("\nDownload interrupted")
        sys.exit(1)

def main():
    print("Japan Weather Data Download for 2024 (All Parameters)")
    print("====================================================")
    print("This will download ALL weather parameters for Japan throughout 2024")
    print("Parameters: temperature, humidity, pressure, windspeed, winddirection,")
    print("            precipitation, solar_radiation, visibility, cloud_cover, dew_point")
    print("")
    
    # Download settings
    locations = 10  # Number of locations
    concurrent = 10  # Concurrent requests
    
    print(f"Settings: {locations} locations, {concurrent} concurrent requests")
    response = input("Continue? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled")
        return
    
    # Download month by month for 2024
    year = 2024
    months = [
        ("01", 31), ("02", 29), ("03", 31), ("04", 30),
        ("05", 31), ("06", 30), ("07", 31), ("08", 31),
        ("09", 30), ("10", 31), ("11", 30), ("12", 31)
    ]
    
    successful = 0
    failed = 0
    total_start = time.time()
    
    for month, days in months:
        # Download in 15-day chunks for better reliability
        for day_start in [1, 16]:
            day_end = min(day_start + 14, days)
            start_date = f"{year}-{month}-{day_start:02d}"
            end_date = f"{year}-{month}-{day_end:02d}"
            
            if download_chunk(start_date, end_date, locations, concurrent):
                successful += 1
            else:
                failed += 1
                # Retry once on failure
                print("Retrying...")
                time.sleep(5)
                if download_chunk(start_date, end_date, locations, concurrent):
                    successful += 1
                    failed -= 1
    
    total_elapsed = time.time() - total_start
    
    print("\n" + "="*60)
    print(f"Download Complete!")
    print(f"Total time: {total_elapsed/60:.1f} minutes ({total_elapsed/3600:.1f} hours)")
    print(f"Successful chunks: {successful}")
    print(f"Failed chunks: {failed}")
    print(f"Data saved to: data/nasapower/processed/")
    print("="*60)

if __name__ == "__main__":
    main()