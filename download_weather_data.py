#!/usr/bin/env python3

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging

from src.plugins import get_registry
from src.domain.models import ParameterType, Location
from src.core.data_storage import DataStorage
from src.core.checkpoint_manager import CheckpointManager
from src.utils.data_analyzer import analyze_dataset


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


WEATHER_PARAMETERS = {
    'temperature': ParameterType.TEMPERATURE,
    'humidity': ParameterType.HUMIDITY,
    'pressure': ParameterType.PRESSURE,
    'windspeed': ParameterType.WINDSPEED,
    'winddirection': ParameterType.WINDDIRECTION,
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
) -> Path:
    registry = get_registry()
    registry.auto_discover()
    
    if source not in registry.list_plugins():
        raise ValueError(f"Unknown data source: {source}. Available: {list(registry.list_plugins().keys())}")
    
    DataSourceClass = registry.get(source)
    datasource = DataSourceClass()
    
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
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"{country.lower()}_{source}_weather_{timestamp}.csv"
        
        checkpoint_file = Path(f"data/{source}/checkpoints") / f"checkpoint_{country}_{source}_weather.json"
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        
        checkpoint_manager = CheckpointManager(checkpoint_file)
        checkpoint_data = checkpoint_manager.load()
        
        start_location_idx = 0
        if checkpoint_data and checkpoint_data.get('output_file') == str(output_file):
            start_location_idx = checkpoint_data.get('current_location_index', 0)
            logger.info(f"Resuming from checkpoint (location {start_location_idx}/{len(locations)})")
        
        storage = DataStorage(output_file)
        
        if start_location_idx == 0:
            headers = [
                'timestamp', 'value', 'sensor_id', 'location_id', 'location_name',
                'latitude', 'longitude', 'parameter', 'unit', 'city', 'country',
                'data_source', 'level', 'quality_flag'
            ]
            storage.write_headers(headers)
        
        total_measurements = 0
        
        for idx, location in enumerate(locations[start_location_idx:], start=start_location_idx):
            logger.info(f"Processing location {idx + 1}/{len(locations)}: {location.name}")
            
            try:
                sensors = await datasource.get_sensors(location, parameters=param_types)
                
                for sensor in sensors:
                    logger.info(f"  Fetching {sensor.parameter.value} data...")
                    
                    measurement_count = 0
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
                            storage.append_measurements(rows)
                        
                    logger.info(f"    Retrieved {measurement_count} measurements")
                    total_measurements += measurement_count
                
                checkpoint_manager.save({
                    'output_file': str(output_file),
                    'current_location_index': idx + 1,
                    'total_locations': len(locations),
                    'total_measurements': total_measurements,
                    'last_updated': datetime.now(timezone.utc).isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error processing location {location.name}: {e}")
                continue
        
        logger.info(f"Download complete! Total measurements: {total_measurements}")
        logger.info(f"Data saved to: {output_file}")
        
        if analyze and total_measurements > 0:
            logger.info("Analyzing dataset...")
            analyze_dataset(str(output_file))
        
        await datasource.close()
        return output_file
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        await datasource.close()
        raise


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