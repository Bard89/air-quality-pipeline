#!/usr/bin/env python3
"""
Streaming weather data download - saves data progressively to avoid memory issues
"""

import asyncio
import csv
import io
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging
import time
import aiofiles
from contextlib import asynccontextmanager

from src.plugins import get_registry
from src.domain.models import Location, ParameterType

try:
    from tqdm.asyncio import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StreamingCSVWriter:
    """Thread-safe CSV writer that streams data to disk"""
    
    def __init__(self, output_file: Path, headers: List[str]):
        self.output_file = output_file
        self.headers = headers
        self.lock = asyncio.Lock()
        self.row_count = 0
        
    async def initialize(self):
        """Write headers to file"""
        # Use StringIO and csv.writer for proper header escaping
        output = io.StringIO()
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(self.headers)
        header_content = output.getvalue()
        output.close()
        
        async with aiofiles.open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            await f.write(header_content)
    
    async def write_measurements(self, measurements: List[Dict[str, Any]]):
        """Write measurements to file with lock"""
        if not measurements:
            return
            
        async with self.lock:
            # Use StringIO and csv.DictWriter for proper CSV escaping
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=self.headers, lineterminator='\n')
            writer.writerows(measurements)
            csv_content = output.getvalue()
            output.close()
            
            async with aiofiles.open(self.output_file, 'a', encoding='utf-8') as f:
                await f.write(csv_content)
                self.row_count += len(measurements)


async def download_location_data_streaming(
    datasource,
    location: Location,
    param_types: List[ParameterType],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    source: str,
    csv_writer: StreamingCSVWriter,
    semaphore: asyncio.Semaphore,
    progress_callback = None
) -> int:
    """Download data for a single location and stream to disk"""
    
    async with semaphore:
        measurement_count = 0
        
        try:
            # Get sensors for this location
            sensors = await datasource.get_sensors(location, parameters=param_types)
            
            # Process data in monthly chunks
            current_start = start_date or datetime.now(timezone.utc) - timedelta(days=30)
            final_end = end_date or datetime.now(timezone.utc)
            
            while current_start <= final_end:
                chunk_end = min(
                    current_start.replace(day=1) + timedelta(days=32),
                    final_end
                ).replace(day=1) - timedelta(days=1)
                
                if chunk_end > final_end:
                    chunk_end = final_end
                
                # Process each sensor
                for sensor in sensors:
                    try:
                        batch_rows = []
                        
                        # Stream measurements
                        async for measurements in datasource.get_measurements(
                            sensor, current_start, chunk_end
                        ):
                            for measurement in measurements:
                                row = {
                                    'timestamp': measurement.timestamp.isoformat(),
                                    'value': str(measurement.value),
                                    'sensor_id': sensor.id,
                                    'location_id': location.id,
                                    'location_name': location.name,
                                    'latitude': str(location.coordinates.latitude),
                                    'longitude': str(location.coordinates.longitude),
                                    'parameter': sensor.parameter.value,
                                    'unit': sensor.unit.value,
                                    'city': location.city or '',
                                    'country': location.country or '',
                                    'data_source': source,
                                    'level': sensor.metadata.get('level', 'surface'),
                                    'quality_flag': measurement.quality_flag or ''
                                }
                                batch_rows.append(row)
                                
                                # Write in batches of 1000 rows
                                if len(batch_rows) >= 1000:
                                    await csv_writer.write_measurements(batch_rows)
                                    measurement_count += len(batch_rows)
                                    batch_rows = []
                        
                        # Write remaining rows
                        if batch_rows:
                            await csv_writer.write_measurements(batch_rows)
                            measurement_count += len(batch_rows)
                            
                    except Exception as e:
                        logger.warning(f"Error fetching {sensor.parameter.value} for {location.name}: {e}")
                        continue
                
                if progress_callback:
                    progress_callback(location.name, current_start, chunk_end)
                    
                current_start = chunk_end + timedelta(days=1)
                    
        except Exception as e:
            logger.error(f"Error processing location {location.name}: {e}")
            
        return measurement_count


async def download_weather_data_streaming(
    source: str,
    country: str = "JP",
    parameters: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    max_locations: Optional[int] = None,
    max_concurrent: int = 5,
    output_dir: Optional[Path] = None
) -> Path:
    """Download weather data with streaming to avoid memory issues"""
    
    registry = get_registry()
    registry.auto_discover()
    
    if source not in registry.list_plugins():
        raise ValueError(f"Unknown data source: {source}")
    
    # Create datasource instances
    datasources = []
    plugins = registry.list_plugins()
    if source not in plugins:
        raise ValueError(f"Plugin {source} not found")
    
    for _ in range(max_concurrent):
        datasources.append(plugins[source]())
    
    try:
        # Get locations
        locations = await datasources[0].get_locations(country=country, limit=max_locations)
        if not locations:
            logger.error(f"No locations found for country: {country}")
            return None
        
        # Setup output
        output_dir = output_dir or Path(f"data/{source}/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        date_start = start_date.strftime("%Y%m%d") if start_date else "latest"
        date_end = end_date.strftime("%Y%m%d") if end_date else "latest"
        filename = f"{country.lower()}_{source}_weather_{date_start}_to_{date_end}.csv"
        output_file = output_dir / filename
    
        # Setup CSV writer
        headers = [
            'timestamp', 'value', 'sensor_id', 'location_id', 'location_name',
            'latitude', 'longitude', 'parameter', 'unit', 'city', 'country',
            'data_source', 'level', 'quality_flag'
        ]
        csv_writer = StreamingCSVWriter(output_file, headers)
        await csv_writer.initialize()
        
        # Parameter types
        param_map = {
            'temperature': ParameterType.TEMPERATURE,
            'humidity': ParameterType.HUMIDITY,
            'pressure': ParameterType.PRESSURE,
            'wind_speed': ParameterType.WIND_SPEED,
            'wind_direction': ParameterType.WIND_DIRECTION,
            'precipitation': ParameterType.PRECIPITATION,
            'solar_radiation': ParameterType.SOLAR_RADIATION,
            'visibility': ParameterType.VISIBILITY,
            'cloud_cover': ParameterType.CLOUD_COVER,
            'dew_point': ParameterType.DEW_POINT
        }
        
        if parameters:
            param_types = [param_map[p] for p in parameters if p in param_map]
        else:
            param_types = list(param_map.values())
        
        # Setup progress tracking
        semaphore = asyncio.Semaphore(max_concurrent)
        start_time = time.time()
        
        logger.info(f"Starting streaming download for {len(locations)} locations")
        logger.info(f"Data will be saved progressively to {output_file}")
        
        # Create tasks
        tasks = []
        for i, location in enumerate(locations):
            ds = datasources[i % len(datasources)]
            task = download_location_data_streaming(
                ds, location, param_types, start_date, end_date, 
                source, csv_writer, semaphore
            )
            tasks.append((location.name, task))
        
        # Process with progress bar
        total_measurements = 0
        
        if TQDM_AVAILABLE:
            with tqdm(total=len(tasks), desc="Downloading", unit="loc") as pbar:
                for name, task in tasks:
                    pbar.set_description(f"Processing {name[:20]}")
                    try:
                        count = await task
                        total_measurements += count
                        pbar.set_postfix(measurements=f"{total_measurements:,}")
                    except Exception as e:
                        logger.error(f"Failed to download {name}: {e}")
                    pbar.update(1)
        else:
            for i, (name, task) in enumerate(tasks):
                logger.info(f"Processing {i+1}/{len(tasks)}: {name}")
                try:
                    count = await task
                    total_measurements += count
                    logger.info(f"  Downloaded {count:,} measurements (Total: {total_measurements:,})")
                except Exception as e:
                    logger.error(f"Failed to download {name}: {e}")
        
        # Print summary
        total_time = time.time() - start_time
        avg_speed = total_measurements / total_time if total_time > 0 else 0
        
        logger.info("=" * 80)
        logger.info(f"Download complete!")
        logger.info(f"Total measurements: {total_measurements:,}")
        logger.info(f"Total time: {total_time/60:.1f} minutes")
        logger.info(f"Average speed: {avg_speed:.1f} measurements/second")
        logger.info(f"Data saved to: {output_file}")
        logger.info("=" * 80)
        
        return output_file
        
    finally:
        # Cleanup - always close datasources
        for ds in datasources:
            if hasattr(ds, 'close'):
                try:
                    await ds.close()
                except Exception as e:
                    logger.error(f"Error closing datasource: {e}")


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Download weather data with streaming")
    parser.add_argument("--source", default="openmeteo", help="Data source")
    parser.add_argument("--country", default="JP", help="Country code")
    parser.add_argument("--parameters", help="Comma-separated list of parameters")
    parser.add_argument("--start", "--start-date", type=lambda x: datetime.fromisoformat(x))
    parser.add_argument("--end", "--end-date", type=lambda x: datetime.fromisoformat(x))
    parser.add_argument("--max-locations", type=int, help="Maximum number of locations")
    parser.add_argument("--max-concurrent", type=int, default=5)
    
    args = parser.parse_args()
    
    parameters = args.parameters.split(',') if args.parameters else None
    
    try:
        await download_weather_data_streaming(
            source=args.source,
            country=args.country,
            parameters=parameters,
            start_date=args.start,
            end_date=args.end,
            max_locations=args.max_locations,
            max_concurrent=args.max_concurrent
        )
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())