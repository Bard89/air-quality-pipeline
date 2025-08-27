#!/usr/bin/env python3
"""Process CAMS NetCDF files to CSV format with H3 aggregation.

This script converts CAMS NetCDF files containing PM2.5 and other atmospheric
composition data into CSV format, with optional H3 hexagonal aggregation.

Usage:
    python scripts/process_cams_to_csv.py --start 2024-01-01 --end 2024-01-31
    python scripts/process_cams_to_csv.py --start 2024-01-01 --end 2024-01-31 --no-aggregate
    python scripts/process_cams_to_csv.py --input data/cams/raw/cams_eac4_20240101.nc
"""

import argparse
import sys
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import xarray as xr
import numpy as np
import h3

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.processors.cams_processor import CAMSProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_single_file(nc_path: Path, output_dir: Path, aggregate: bool = True) -> pd.DataFrame:
    """Process a single NetCDF file to CSV.
    
    Args:
        nc_path: Path to NetCDF file
        output_dir: Output directory for CSV
        aggregate: Whether to aggregate to H3 hexagons
        
    Returns:
        Processed DataFrame
    """
    logger.info(f"Processing {nc_path}")
    
    try:
        # Open NetCDF
        ds = xr.open_dataset(nc_path)
        
        # Rename dimensions
        if 'longitude' in ds.dims:
            ds = ds.rename({'longitude': 'lon'})
        if 'latitude' in ds.dims:
            ds = ds.rename({'latitude': 'lat'})
        if 'valid_time' in ds.dims:
            ds = ds.rename({'valid_time': 'time'})
            
        # Convert to DataFrame
        records = []
        
        # Get coordinates
        lats = ds.lat.values
        lons = ds.lon.values
        times = ds.time.values
        
        # Process PM2.5 (main variable)
        if 'pm2p5' in ds.variables:
            pm25_data = ds['pm2p5'].values
            
            # Convert kg/m³ to µg/m³
            if 'kg' in str(ds['pm2p5'].attrs.get('units', '')):
                pm25_data = pm25_data * 1e9
                
            # Create records for each point
            for t_idx, time in enumerate(times):
                for lat_idx, lat in enumerate(lats):
                    for lon_idx, lon in enumerate(lons):
                        value = pm25_data[t_idx, lat_idx, lon_idx]
                        
                        if not np.isnan(value):
                            record = {
                                'timestamp': pd.Timestamp(time),
                                'latitude': lat,
                                'longitude': lon,
                                'pm25_ugm3': value
                            }
                            
                            # Add other variables if present
                            for var in ['pm10', 'no2', 'so2', 'co', 'o3']:
                                if var in ds.variables:
                                    var_value = ds[var].values[t_idx, lat_idx, lon_idx]
                                    if 'kg' in str(ds[var].attrs.get('units', '')):
                                        var_value = var_value * 1e9
                                    record[f'{var}_ugm3'] = var_value
                                    
                            records.append(record)
                            
        df = pd.DataFrame(records)
        
        if df.empty:
            logger.warning("No valid data found in NetCDF file")
            return df
            
        logger.info(f"Extracted {len(df)} data points")
        
        # Add H3 index
        if aggregate:
            logger.info("Adding H3 hexagon indices...")
            df['h3_index_res8'] = df.apply(
                lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], 8),
                axis=1
            )
            
            # Aggregate to hexagons
            logger.info("Aggregating to H3 hexagons...")
            
            # Round timestamps to hour
            df['timestamp_hour'] = df['timestamp'].dt.floor('h')
            
            # Group and aggregate
            agg_dict = {
                'pm25_ugm3': ['mean', 'std', 'min', 'max', 'count'],
                'latitude': 'mean',
                'longitude': 'mean'
            }
            
            # Add other pollutants if present
            for col in df.columns:
                if col.endswith('_ugm3') and col != 'pm25_ugm3':
                    agg_dict[col] = ['mean', 'std', 'min', 'max']
                    
            grouped = df.groupby(['h3_index_res8', 'timestamp_hour']).agg(agg_dict)
            
            # Flatten column names
            grouped.columns = ['_'.join(col).strip() if col[1] else col[0] 
                              for col in grouped.columns.values]
            grouped = grouped.reset_index()
            grouped = grouped.rename(columns={'timestamp_hour': 'timestamp'})
            
            # Add hexagon center coordinates
            grouped['h3_lat_res8'] = grouped['h3_index_res8'].apply(
                lambda x: h3.cell_to_latlng(x)[0]
            )
            grouped['h3_lon_res8'] = grouped['h3_index_res8'].apply(
                lambda x: h3.cell_to_latlng(x)[1]
            )
            
            df = grouped
            logger.info(f"Aggregated to {len(df)} hexagon-hours")
            
        # Save to CSV
        output_file = output_dir / f"{nc_path.stem}.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Saved to {output_file}")
        
        # Print statistics
        print("\n" + "="*60)
        print(f"Summary for {nc_path.name}")
        print("="*60)
        print(f"Records: {len(df)}")
        print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        if 'pm25_ugm3_mean' in df.columns:
            col = 'pm25_ugm3_mean'
        elif 'pm25_ugm3' in df.columns:
            col = 'pm25_ugm3'
        else:
            col = None
            
        if col:
            print(f"\nPM2.5 Statistics (µg/m³):")
            print(f"  Mean: {df[col].mean():.1f}")
            print(f"  Median: {df[col].median():.1f}")
            print(f"  Min: {df[col].min():.1f}")
            print(f"  Max: {df[col].max():.1f}")
            
            # Check WHO guidelines
            who_24hr = 15
            exceed = (df[col] > who_24hr).sum()
            print(f"\nWHO 24-hr guideline ({who_24hr} µg/m³) exceedances: {exceed}/{len(df)} ({exceed/len(df)*100:.1f}%)")
            
        print("="*60 + "\n")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing {nc_path}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description='Process CAMS NetCDF files to CSV')
    
    # Input options
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input', type=str,
                      help='Process a single NetCDF file')
    group.add_argument('--start', type=str,
                      help='Start date (YYYY-MM-DD) for batch processing')
    
    parser.add_argument('--end', type=str,
                       help='End date (YYYY-MM-DD) for batch processing')
    
    # Processing options
    parser.add_argument('--no-aggregate', action='store_true',
                       help='Skip H3 hexagonal aggregation')
    parser.add_argument('--country', type=str, default='JP',
                       help='Country code (default: JP)')
    parser.add_argument('--output-dir', type=str,
                       help='Output directory (default: data/cams/processed)')
    
    args = parser.parse_args()
    
    # Set output directory
    output_dir = Path(args.output_dir) if args.output_dir else Path('data/cams/processed')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.input:
        # Process single file
        nc_path = Path(args.input)
        if not nc_path.exists():
            logger.error(f"File not found: {nc_path}")
            sys.exit(1)
            
        process_single_file(nc_path, output_dir, aggregate=not args.no_aggregate)
        
    else:
        # Batch processing
        if not args.end:
            args.end = args.start
            
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        
        # Use the processor for batch processing
        processor = CAMSProcessor(country=args.country)
        df = processor.process(
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            logger.warning("No data processed")
        else:
            logger.info(f"Successfully processed {len(df)} records")


if __name__ == '__main__':
    main()