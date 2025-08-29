#!/usr/bin/env python3
"""Download CAMS PM2.5 data matching OpenMeteo weather grid.

This script downloads PM2.5 and PM10 data from CAMS (Copernicus Atmosphere Monitoring Service)
for the same geographic area covered by OpenMeteo weather data.

Usage:
    python scripts/download_cams_pm25.py --start 2024-01-01 --end 2024-01-31
    python scripts/download_cams_pm25.py --start 2024-01-01 --end 2024-01-31 --forecast
    python scripts/download_cams_pm25.py --locations Tokyo Osaka --start 2024-01-01
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import logging
import os
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.plugins.cams.datasource import CAMSDataSource
from src.domain.models import ParameterType

# Load environment variables
load_dotenv()

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Note: Install tqdm for progress bars: pip install tqdm")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def download_cams_data(
    start_date: datetime,
    end_date: datetime,
    parameters: List[str] = None,
    use_forecast: bool = False,
    specific_locations: List[str] = None,
    output_dir: Optional[Path] = None,
    api_uid: Optional[str] = None,
    api_key: Optional[str] = None
):
    """Download CAMS data for specified period and parameters.
    
    Args:
        start_date: Start date for download
        end_date: End date for download
        parameters: List of parameters to download (pm25, pm10, no2, so2, o3, co)
        use_forecast: If True, download forecast data instead of reanalysis
        specific_locations: List of city names to download data for (None = all grid)
        output_dir: Output directory for data files
        api_uid: CDS API UID (if not in .env)
        api_key: CDS API key (if not in .env)
    """
    # Default parameters
    if parameters is None:
        parameters = ['pm25', 'pm10']
    
    # Map parameter names to ParameterType
    param_map = {
        'pm25': ParameterType.PM25,
        'pm10': ParameterType.PM10,
        'no2': ParameterType.NO2,
        'so2': ParameterType.SO2,
        'o3': ParameterType.O3,
        'co': ParameterType.CO
    }
    
    param_types = [param_map[p] for p in parameters if p in param_map]
    
    if not param_types:
        logger.error(f"No valid parameters specified. Available: {list(param_map.keys())}")
        return
    
    # Initialize data source (will use env variables if uid/key not provided)
    datasource = CAMSDataSource(
        api_uid=api_uid,
        api_key=api_key,
        data_dir=str(output_dir or "data/cams")
    )
    
    try:
        # Get locations
        logger.info("Getting CAMS grid locations...")
        all_locations = await datasource.get_locations(limit=None)
        
        # Filter locations if specific ones requested
        if specific_locations:
            locations = [
                loc for loc in all_locations 
                if any(city.lower() in loc.name.lower() for city in specific_locations)
            ]
            if not locations:
                logger.warning(f"No locations found matching: {specific_locations}")
                logger.info("Available city locations:")
                city_locs = [loc for loc in all_locations if loc.metadata.get('type') == 'city_center']
                for loc in city_locs[:20]:
                    logger.info(f"  - {loc.name}")
                return
        else:
            # Use major cities by default
            locations = [
                loc for loc in all_locations 
                if loc.metadata.get('type') == 'city_center'
            ][:10]  # Top 10 cities
        
        logger.info(f"Downloading data for {len(locations)} locations")
        logger.info(f"Parameters: {parameters}")
        logger.info(f"Period: {start_date.date()} to {end_date.date()}")
        logger.info(f"Data type: {'Forecast' if use_forecast else 'Reanalysis'}")
        
        # Create output directory
        output_dir = output_dir or Path(f"data/cams/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare CSV file
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = output_dir / f"cams_pm25_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{timestamp_str}.csv"
        
        # Write header
        with open(output_file, 'w') as f:
            f.write("timestamp,location_id,location_name,latitude,longitude,parameter,value,unit,data_type\n")
        
        total_measurements = 0
        
        # Download data for each location and parameter
        for location in tqdm(locations, desc="Locations") if TQDM_AVAILABLE else locations:
            logger.info(f"Processing {location.name}...")
            
            # Get sensors for this location
            sensors = await datasource.get_sensors(location, parameters=param_types)
            
            for sensor in sensors:
                # Get measurements
                measurement_count = 0
                async for measurements in datasource.get_measurements(
                    sensor,
                    start_date=start_date,
                    end_date=end_date
                ):
                    # Write measurements to CSV
                    with open(output_file, 'a') as f:
                        for m in measurements:
                            f.write(f"{m.timestamp.isoformat()},"
                                  f"{location.id},"
                                  f'"{location.name}",'
                                  f"{float(location.coordinates.latitude)},"
                                  f"{float(location.coordinates.longitude)},"
                                  f"{sensor.parameter.value},"
                                  f"{float(m.value)},"
                                  f"{sensor.unit.value},"
                                  f"{'forecast' if use_forecast else 'reanalysis'}\n")
                            measurement_count += 1
                    
                total_measurements += measurement_count
                
                if measurement_count > 0:
                    logger.info(f"  {sensor.parameter.value}: {measurement_count} measurements")
        
        logger.info(f"Download complete!")
        logger.info(f"Total measurements: {total_measurements}")
        logger.info(f"Output file: {output_file}")
        
        # Provide basic statistics
        if total_measurements > 0:
            import pandas as pd
            df = pd.read_csv(output_file)
            logger.info("\nData Summary:")
            logger.info(f"  Locations: {df['location_id'].nunique()}")
            logger.info(f"  Parameters: {df['parameter'].unique().tolist()}")
            logger.info(f"  Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            # PM2.5 statistics if available
            if 'pm25' in df['parameter'].values:
                pm25_data = df[df['parameter'] == 'pm25']
                logger.info(f"\nPM2.5 Statistics:")
                logger.info(f"  Mean: {pm25_data['value'].mean():.1f} µg/m³")
                logger.info(f"  Median: {pm25_data['value'].median():.1f} µg/m³")
                logger.info(f"  Max: {pm25_data['value'].max():.1f} µg/m³")
                logger.info(f"  Min: {pm25_data['value'].min():.1f} µg/m³")
        
    except Exception as e:
        logger.error(f"Error downloading CAMS data: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Download CAMS PM2.5 data')
    
    # Date arguments
    parser.add_argument('--start', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, 
                       help='End date (YYYY-MM-DD). Default: same as start')
    
    # Data selection
    parser.add_argument('--parameters', nargs='+', 
                       default=['pm25', 'pm10'],
                       choices=['pm25', 'pm10', 'no2', 'so2', 'o3', 'co'],
                       help='Parameters to download')
    parser.add_argument('--locations', nargs='+',
                       help='Specific city names to download (e.g., Tokyo Osaka)')
    parser.add_argument('--forecast', action='store_true',
                       help='Download forecast data instead of reanalysis')
    
    # Output options
    parser.add_argument('--output', type=str,
                       help='Output directory (default: data/cams/processed)')
    
    # API configuration (optional if using .env)
    parser.add_argument('--api-uid', type=str,
                       help='CDS API UID (optional if set in .env as CAMS_API_UID)')
    parser.add_argument('--api-key', type=str,
                       help='CDS API key (optional if set in .env as CAMS_API_KEY)')
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d') if args.end else start_date
    
    if end_date < start_date:
        logger.error("End date must be after start date")
        sys.exit(1)
    
    # Check for API credentials
    has_env_creds = os.getenv('CAMS_API_UID') and os.getenv('CAMS_API_KEY')
    has_arg_creds = args.api_uid and args.api_key
    has_cdsapirc = os.path.exists(os.path.expanduser('~/.cdsapirc'))
    
    if not (has_env_creds or has_arg_creds or has_cdsapirc):
        logger.error("No CAMS API credentials found")
        logger.info("Please do one of the following:")
        logger.info("  1. Set CAMS_API_UID and CAMS_API_KEY in your .env file")
        logger.info("  2. Provide --api-uid YOUR_UID --api-key YOUR_KEY")
        logger.info("  3. Create ~/.cdsapirc file with your credentials")
        logger.info("Get your credentials from: https://ads.atmosphere.copernicus.eu/api-how-to")
        sys.exit(1)
    
    # Run download
    asyncio.run(download_cams_data(
        start_date=start_date,
        end_date=end_date,
        parameters=args.parameters,
        use_forecast=args.forecast,
        specific_locations=args.locations,
        output_dir=Path(args.output) if args.output else None,
        api_uid=args.api_uid,
        api_key=args.api_key
    ))


if __name__ == '__main__':
    main()