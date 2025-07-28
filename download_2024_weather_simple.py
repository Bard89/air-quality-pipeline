#!/usr/bin/env python3

import subprocess
import sys
from datetime import datetime, timedelta

def download_month(year, month, locations=5, params="temperature,humidity,precipitation"):
    """Download weather data for a specific month"""
    start = f"{year}-{month:02d}-01"
    
    # Calculate last day of month
    if month == 12:
        end = f"{year}-12-31"
    else:
        next_month = datetime(year, month + 1, 1) - timedelta(days=1)
        end = next_month.strftime("%Y-%m-%d")
    
    print(f"\nDownloading {start} to {end}...")
    
    cmd = [
        "python", "download_weather_incremental.py",
        "--source", "nasapower",
        "--country", "JP",
        "--max-locations", str(locations),
        "--max-concurrent", "5",
        "--parameters", params,
        "--start", start,
        "--end", end,
        "--no-analyze"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print(f"✓ Completed {year}-{month:02d}")
        return True
    except subprocess.CalledProcessError:
        print(f"✗ Failed {year}-{month:02d}")
        return False
    except KeyboardInterrupt:
        print("\nDownload interrupted")
        sys.exit(1)

def main():
    print("Japan Weather Data Download for 2024")
    print("====================================")
    print("This will download temperature, humidity, and precipitation data")
    print("for 5 major Japanese cities throughout 2024.")
    print("")
    
    # Ask for confirmation
    response = input("Continue? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled")
        return
    
    year = 2024
    successful = 0
    failed = 0
    
    for month in range(1, 13):
        if download_month(year, month):
            successful += 1
        else:
            failed += 1
    
    print("\n" + "="*50)
    print(f"Download Summary:")
    print(f"Successful: {successful} months")
    print(f"Failed: {failed} months")
    print(f"Data saved to: data/nasapower/processed/")
    print("="*50)

if __name__ == "__main__":
    main()