#!/usr/bin/env python3

import asyncio
from datetime import datetime
from src.plugins import get_registry

async def compare_weather_sources():
    registry = get_registry()
    registry.auto_discover()
    
    print("Granular Weather Data Sources Comparison for Japan")
    print("=" * 80)
    print()
    
    sources_info = {
        'jma': {
            'name': 'JMA (Japan Meteorological Agency)',
            'features': [
                '✓ 1,300+ AMeDAS weather stations across Japan',
                '✓ 10-minute measurement intervals',
                '✓ Highest spatial density for Japan',
                '✓ Real-time data (last 3 days only)',
                '✓ Free access via API',
                '✓ Parameters: temp, humidity, pressure, wind, precip, solar, visibility'
            ]
        },
        'openmeteo': {
            'name': 'Open-Meteo',
            'features': [
                '✓ 0.1° resolution (~11km grid)',
                '✓ Hourly measurements',
                '✓ Historical data from 1940',
                '✓ No API key required',
                '✓ No rate limits',
                '✓ Based on ERA5 reanalysis data',
                '✓ 500+ grid points covering Japan'
            ]
        },
        'era5': {
            'name': 'ERA5 (ECMWF)',
            'features': [
                '✓ 0.25° resolution (~31km grid)',
                '✓ Hourly measurements',
                '✓ Most comprehensive parameters',
                '✓ Multiple atmospheric levels',
                '✓ Free but requires CDS API key',
                '✓ ~150 grid points for Japan',
                '✓ 5-day update latency'
            ]
        },
        'nasapower': {
            'name': 'NASA POWER',
            'features': [
                '✗ 0.5° resolution (~50km grid)',
                '✗ Daily/hourly data',
                '✗ Only 24 grid points for Japan',
                '✗ No API key required',
                '✗ Good for quick prototyping',
                '✗ Less granular than other sources'
            ]
        }
    }
    
    for source_id, info in sources_info.items():
        print(f"{info['name']}")
        print("-" * len(info['name']))
        for feature in info['features']:
            print(f"  {feature}")
        print()
        
        if source_id in registry.list_plugins():
            DataSourceClass = registry.get(source_id)
            ds = DataSourceClass()
            try:
                locations = await ds.get_locations(country='JP', limit=5)
                print(f"  Sample locations:")
                for loc in locations:
                    if source_id == 'jma' and loc.metadata.get('data_source') == 'amedas':
                        print(f"    - {loc.name} (Station ID: {loc.metadata.get('station_id', 'N/A')})")
                    else:
                        print(f"    - {loc.name}")
            finally:
                await ds.close()
        else:
            print(f"  Plugin not available")
        print()
    
    print("Recommendation for Most Granular Data:")
    print("=" * 80)
    print("1. JMA AMeDAS: Best for Japan-specific high-frequency data (10-minute intervals)")
    print("2. Open-Meteo: Best balance of resolution, ease of use, and no limits")
    print("3. ERA5: Best for comprehensive atmospheric analysis")
    print()
    print("Commands to download 2024 data:")
    print("-" * 80)
    print("# JMA AMeDAS (1,300+ stations, 10-minute data - recent only):")
    print("python download_weather_incremental.py --source jma --country JP --start [recent-date] --end [today] --max-locations 100")
    print()
    print("# Open-Meteo (0.1° grid, hourly - recommended for historical):")
    print("python download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-12-31 --max-locations 100")
    print()
    print("# For NASA POWER (slower but reliable):")
    print("python download_weather_incremental.py --source nasapower --country JP --start 2024-01-01 --end 2024-12-31")

if __name__ == "__main__":
    asyncio.run(compare_weather_sources())