#!/usr/bin/env python3

import argparse
import asyncio
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging
import time

from src.plugins import get_registry
from src.domain.models import ParameterType, Location
import json
import csv
from src.utils.data_analyzer import analyze_dataset

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Note: Install tqdm for progress bars: pip install tqdm")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


WEATHER_PARAMETERS = {
    'temperature': ParameterType.TEMPERATURE,
    'humidity': ParameterType.HUMIDITY,
    'pressure': ParameterType.PRESSURE,
    'windspeed': ParameterType.WIND_SPEED,
    'winddirection': ParameterType.WIND_DIRECTION,
    'precipitation': ParameterType.PRECIPITATION,
    'solar_radiation': ParameterType.SOLAR_RADIATION,
    'visibility': ParameterType.VISIBILITY,
    'cloud_cover': ParameterType.CLOUD_COVER,
    'dew_point': ParameterType.DEW_POINT
}


WEATHER_SOURCES = {
    'jma': 'Japan Meteorological Agency (AMeDAS stations + JRA-55 reanalysis)',
    'era5': 'ECMWF ERA5 reanalysis (0.25° resolution, requires CDS API key)',
    'nasapower': 'NASA POWER satellite data (0.5° resolution, no API key required)'
}


async def download_weather_data(
    source: str,
    country: str = "JP",
    parameters: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    max_locations: Optional[int] = None,
    analyze: bool = True,
    output_dir: Optional[Path] = None
) -> Optional[Path]:
    registry = get_registry()
    registry.auto_discover()
    
    if source not in registry.list_plugins():
        raise ValueError(f"Unknown data source: {source}. Available: {list(registry.list_plugins().keys())}")
    
    DataSourceClass = registry.get(source)
    datasource = DataSourceClass()
    
    csv_file = None
    try:
        param_types = []
        if parameters:
            for param in parameters:
                if param.lower() in WEATHER_PARAMETERS:
                    param_types.append(WEATHER_PARAMETERS[param.lower()])
                else:
                    logger.warning(f"Unknown parameter: {param}")
        else:
            param_types = list(WEATHER_PARAMETERS.values())
        
        locations = await datasource.get_locations(country=country, limit=max_locations)
        logger.info(f"Found {len(locations)} locations for {country}")
        
        if not locations:
            logger.error(f"No locations found for country: {country}")
            return None
        
        output_dir = output_dir or Path(f"data/{source}/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename with date range
        if start_date and end_date:
            date_start = start_date.strftime("%Y%m%d")
            date_end = end_date.strftime("%Y%m%d")
            filename = f"{country.lower()}_{source}_weather_{date_start}_to_{date_end}.csv"
        else:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"{country.lower()}_{source}_weather_{timestamp}.csv"
        
        output_file = output_dir / filename
        
        checkpoint_file = Path(f"data/{source}/checkpoints") / f"checkpoint_{country}_{source}_weather.json"
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        
        checkpoint_data = {}
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
        
        # Define headers once
        headers = [
            'timestamp', 'value', 'sensor_id', 'location_id', 'location_name',
            'latitude', 'longitude', 'parameter', 'unit', 'city', 'country',
            'data_source', 'level', 'quality_flag'
        ]
        
        start_location_idx = 0
        if checkpoint_data and checkpoint_data.get('output_file') == str(output_file):
            start_location_idx = checkpoint_data.get('current_location_index', 0)
            logger.info(f"Resuming from checkpoint (location {start_location_idx}/{len(locations)})")
        
        if start_location_idx == 0:
            csv_file = open(output_file, 'w', newline='', encoding='utf-8')
            csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
            csv_writer.writeheader()
        else:
            csv_file = open(output_file, 'a', newline='', encoding='utf-8')
            csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        
        total_measurements = checkpoint_data.get('total_measurements', 0) if checkpoint_data else 0
        start_time = time.time()
        
        # Create progress bar for locations
        location_progress = None
        if TQDM_AVAILABLE:
            location_progress = tqdm(
                enumerate(locations[start_location_idx:], start=start_location_idx),
                total=len(locations) - start_location_idx,
                desc="Locations",
                unit="loc",
                initial=0
            )
            location_iterator = location_progress
        else:
            location_iterator = enumerate(locations[start_location_idx:], start=start_location_idx)
        
        for idx, location in location_iterator:
            if not TQDM_AVAILABLE:
                elapsed = time.time() - start_time
                speed = total_measurements / elapsed if elapsed > 0 else 0
                logger.info(f"Processing location {idx + 1}/{len(locations)}: {location.name} | Speed: {speed:.1f} measurements/sec")
            
            try:
                sensors = await datasource.get_sensors(location, parameters=param_types)
                
                # Create progress bar for sensors
                sensor_progress = None
                if TQDM_AVAILABLE:
                    sensor_progress = tqdm(
                        sensors,
                        desc=f"  {location.name[:20]}",
                        unit="param",
                        leave=False
                    )
                    sensor_iterator = sensor_progress
                else:
                    sensor_iterator = sensors
                
                for sensor in sensor_iterator:
                    if not TQDM_AVAILABLE:
                        logger.info(f"  Fetching {sensor.parameter.value} data...")
                    
                    measurement_count = 0
                    fetch_start = time.time()
                    
                    async for measurements in datasource.get_measurements(
                        sensor,
                        start_date=start_date,
                        end_date=end_date
                    ):
                        rows = []
                        for measurement in measurements:
                            row = {
                                'timestamp': measurement.timestamp.isoformat(),
                                'value': float(measurement.value),
                                'sensor_id': sensor.id,
                                'location_id': location.id,
                                'location_name': location.name,
                                'latitude': float(location.coordinates.latitude),
                                'longitude': float(location.coordinates.longitude),
                                'parameter': sensor.parameter.value,
                                'unit': sensor.unit.value,
                                'city': location.city or '',
                                'country': location.country or '',
                                'data_source': source,
                                'level': sensor.metadata.get('level', 'surface'),
                                'quality_flag': measurement.quality_flag or ''
                            }
                            rows.append(row)
                            measurement_count += 1
                        
                        if rows:
                            csv_writer.writerows(rows)
                            csv_file.flush()
                    
                    fetch_time = time.time() - fetch_start
                    if measurement_count > 0:
                        fetch_speed = measurement_count / fetch_time if fetch_time > 0 else 0
                        if TQDM_AVAILABLE and sensor_progress:
                            sensor_progress.set_postfix({
                                'measurements': measurement_count,
                                'speed': f'{fetch_speed:.0f}/s'
                            })
                        else:
                            logger.info(f"    Retrieved {measurement_count} measurements in {fetch_time:.1f}s ({fetch_speed:.0f}/s)")
                    
                    total_measurements += measurement_count
                
                if TQDM_AVAILABLE and sensor_progress:
                    sensor_progress.close()
                
                # Update progress statistics
                elapsed = time.time() - start_time
                locations_done = idx + 1 - start_location_idx
                if locations_done > 0 and elapsed > 0:
                    avg_time_per_location = elapsed / locations_done
                    remaining_locations = len(locations) - idx - 1
                    eta_seconds = avg_time_per_location * remaining_locations
                    eta = datetime.now() + timedelta(seconds=eta_seconds)
                    
                    if TQDM_AVAILABLE and location_progress:
                        location_progress.set_postfix({
                            'measurements': f'{total_measurements:,}',
                            'ETA': eta.strftime('%H:%M:%S')
                        })
                
                checkpoint_data = {
                    'output_file': str(output_file),
                    'current_location_index': idx + 1,
                    'total_locations': len(locations),
                    'total_measurements': total_measurements,
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }
                with open(checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
                
            except Exception as e:
                logger.error(f"Error processing location {location.name}: {e}")
                continue
        
        if TQDM_AVAILABLE and location_progress:
            location_progress.close()
        
        # Calculate final statistics
        total_time = time.time() - start_time
        avg_speed = total_measurements / total_time if total_time > 0 else 0
        
        logger.info("=" * 80)
        logger.info(f"Download complete!")
        logger.info(f"Total measurements: {total_measurements:,}")
        logger.info(f"Total time: {total_time/60:.1f} minutes")
        logger.info(f"Average speed: {avg_speed:.1f} measurements/second")
        logger.info(f"Data saved to: {output_file}")
        logger.info("=" * 80)
        
        if analyze and total_measurements > 0:
            logger.info("Analyzing dataset...")
            analyze_dataset(str(output_file))
        
        return output_file
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise
    finally:
        if csv_file:
            csv_file.close()
        await datasource.close()


def main():
    parser = argparse.ArgumentParser(
        description='Download weather data for Japan from various sources'
    )
    parser.add_argument(
        '--source', '-s',
        choices=list(WEATHER_SOURCES.keys()),
        default='nasapower',
        help='Weather data source (default: nasapower - no API key required)'
    )
    parser.add_argument(
        '--country', '-c',
        default='JP',
        help='Country code (default: JP)'
    )
    parser.add_argument(
        '--parameters', '-p',
        help='Comma-separated list of parameters (e.g., temperature,humidity,windspeed)'
    )
    parser.add_argument(
        '--start', '--start-date',
        type=lambda s: datetime.fromisoformat(s),
        help='Start date (ISO format: YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end', '--end-date',
        type=lambda s: datetime.fromisoformat(s),
        help='End date (ISO format: YYYY-MM-DD)'
    )
    parser.add_argument(
        '--max-locations',
        type=int,
        help='Maximum number of locations to process'
    )
    parser.add_argument(
        '--no-analyze',
        action='store_true',
        help='Skip analysis after download'
    )
    parser.add_argument(
        '--list-sources',
        action='store_true',
        help='List available weather data sources'
    )
    parser.add_argument(
        '--list-parameters',
        action='store_true',
        help='List available weather parameters'
    )
    
    args = parser.parse_args()
    
    if args.list_sources:
        print("\nAvailable Weather Data Sources:")
        print("=" * 80)
        for source, description in WEATHER_SOURCES.items():
            print(f"{source:12} - {description}")
        print("\nRecommended for quick start: nasapower (no API key required)")
        print("Best resolution: era5 (requires CDS API key)")
        return
    
    if args.list_parameters:
        print("\nAvailable Weather Parameters:")
        print("=" * 80)
        for param in sorted(WEATHER_PARAMETERS.keys()):
            print(f"  {param}")
        return
    
    parameters = None
    if args.parameters:
        parameters = [p.strip() for p in args.parameters.split(',')]
    
    try:
        asyncio.run(download_weather_data(
            source=args.source,
            country=args.country,
            parameters=parameters,
            start_date=args.start,
            end_date=args.end,
            max_locations=args.max_locations,
            analyze=not args.no_analyze
        ))
    except KeyboardInterrupt:
        logger.info("\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Download failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()