#!/usr/bin/env python3

import argparse
import asyncio
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging
import time
import json
import csv
import aiohttp

from src.plugins import get_registry
from src.domain.models import ParameterType, Location
from src.utils.data_analyzer import analyze_dataset

try:
    from tqdm.asyncio import tqdm as atqdm
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


async def download_location_data(
    datasource,
    location: Location,
    parameters: List[ParameterType],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    source: str,
    semaphore: asyncio.Semaphore,
    progress_callback=None
) -> List[Dict[str, Any]]:
    """Download data for a single location with all parameters"""
    async with semaphore:
        all_rows = []
        
        try:
            sensors = await datasource.get_sensors(location, parameters=parameters)
            
            # Split large date ranges into monthly chunks
            current_start = start_date or datetime.now(timezone.utc) - timedelta(days=30)
            final_end = end_date or datetime.now(timezone.utc)
            
            while current_start < final_end:
                # Calculate chunk end (max 1 month)
                next_month = current_start.replace(day=1) + timedelta(days=32)
                next_month = next_month.replace(day=1)
                chunk_end = min(next_month - timedelta(days=1), final_end)
                
                for sensor in sensors:
                    try:
                        async for measurements in datasource.get_measurements(
                            sensor,
                            start_date=current_start,
                            end_date=chunk_end
                        ):
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
                                all_rows.append(row)
                                
                    except Exception as e:
                        logger.warning(f"Error fetching {sensor.parameter.value} for {location.name} ({current_start} to {chunk_end}): {e}")
                        continue
                
                if progress_callback:
                    progress_callback(location.name, current_start, chunk_end)
                    
                current_start = chunk_end + timedelta(days=1)
                    
        except Exception as e:
            logger.error(f"Error processing location {location.name}: {e}")
            
        return all_rows


async def download_weather_data_parallel(
    source: str,
    country: str = "JP",
    parameters: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    max_locations: Optional[int] = None,
    max_concurrent: int = 5,
    analyze: bool = True,
    output_dir: Optional[Path] = None
) -> Path:
    """Download weather data in parallel"""
    registry = get_registry()
    registry.auto_discover()
    
    if source not in registry.list_plugins():
        raise ValueError(f"Unknown data source: {source}. Available: {list(registry.list_plugins().keys())}")
    
    DataSourceClass = registry.get(source)
    
    # Create multiple datasource instances for parallel requests
    datasources = [DataSourceClass() for _ in range(max_concurrent)]
    
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
        
        # Get locations using the first datasource
        locations = await datasources[0].get_locations(country=country, limit=max_locations)
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
        
        # Write headers
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            headers = [
                'timestamp', 'value', 'sensor_id', 'location_id', 'location_name',
                'latitude', 'longitude', 'parameter', 'unit', 'city', 'country',
                'data_source', 'level', 'quality_flag'
            ]
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        # Create tasks for all locations
        start_time = time.time()
        tasks = []
        
        # Calculate total months to process
        total_months = 1
        if start_date and end_date:
            total_months = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1
            logger.info(f"Date range spans {total_months} months")
        
        logger.info(f"Starting parallel download with {max_concurrent} concurrent requests...")
        logger.info(f"Processing {len(locations)} locations x {len(param_types)} parameters")
        
        # Progress tracking
        completed_chunks = 0
        total_chunks = len(locations) * total_months
        
        def progress_callback(location_name, chunk_start, chunk_end):
            nonlocal completed_chunks
            completed_chunks += 1
            logger.debug(f"Completed {location_name} for {chunk_start.strftime('%Y-%m')}")
        
        for i, location in enumerate(locations):
            # Use round-robin to distribute locations across datasource instances
            ds = datasources[i % len(datasources)]
            task = download_location_data(
                ds, location, param_types, start_date, end_date, source, semaphore, progress_callback
            )
            tasks.append((location.name, task))
        
        # Process tasks with progress bar
        if TQDM_AVAILABLE:
            results = []
            with tqdm(total=len(tasks), desc="Downloading locations", unit="loc") as pbar:
                for name, task in tasks:
                    pbar.set_description(f"Processing {name[:20]}")
                    try:
                        result = await task
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Failed to download {name}: {e}")
                        results.append([])
                    pbar.update(1)
        else:
            results = []
            for i, (name, task) in enumerate(tasks):
                logger.info(f"Processing location {i+1}/{len(tasks)}: {name}")
                try:
                    result = await task
                    results.append(result)
                except Exception as e:
                    logger.error(f"Failed to download {name}: {e}")
                    results.append([])
        
        # Write all results to CSV
        total_measurements = 0
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            for location_rows in results:
                if location_rows:
                    writer.writerows(location_rows)
                    total_measurements += len(location_rows)
        
        # Calculate statistics
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
        
    finally:
        # Close all datasource connections
        for ds in datasources:
            if ds is not None:
                await ds.close()


def main():
    parser = argparse.ArgumentParser(
        description='Download weather data for Japan in parallel (much faster!)'
    )
    parser.add_argument(
        '--source', '-s',
        default='nasapower',
        help='Weather data source (default: nasapower)'
    )
    parser.add_argument(
        '--country', '-c',
        default='JP',
        help='Country code (default: JP)'
    )
    parser.add_argument(
        '--parameters', '-p',
        help='Comma-separated list of parameters'
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
        default=10,
        help='Maximum number of locations (default: 10)'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=5,
        help='Maximum concurrent downloads (default: 5)'
    )
    parser.add_argument(
        '--no-analyze',
        action='store_true',
        help='Skip analysis after download'
    )
    
    args = parser.parse_args()
    
    parameters = None
    if args.parameters:
        parameters = [p.strip() for p in args.parameters.split(',')]
    
    try:
        asyncio.run(download_weather_data_parallel(
            source=args.source,
            country=args.country,
            parameters=parameters,
            start_date=args.start,
            end_date=args.end,
            max_locations=args.max_locations,
            max_concurrent=args.max_concurrent,
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