#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import h3
from typing import Optional, List, Dict
import warnings

warnings.filterwarnings('ignore')

sys.path.append(str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

H3_RESOLUTION = 8

def load_processed_data(
    processed_dir: Path,
    country: str,
    source: str,
    start_date: datetime,
    end_date: datetime
) -> Optional[pd.DataFrame]:
    pattern = f"{country.lower()}_{source}_processed_*.csv"
    files = list(processed_dir.glob(pattern))
    
    if not files:
        logger.warning(f"No processed files found for {source}")
        return None
    
    relevant_dfs = []
    for file_path in files:
        try:
            df = pd.read_csv(file_path, parse_dates=['timestamp'] if 'timestamp' in pd.read_csv(file_path, nrows=1).columns else None)
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
                df = df[mask]
            
            if not df.empty:
                relevant_dfs.append(df)
                logger.info(f"Loaded {len(df)} records from {file_path.name}")
        except Exception as e:
            logger.warning(f"Could not load {file_path}: {e}")
    
    if not relevant_dfs:
        return None
    
    combined = pd.concat(relevant_dfs, ignore_index=True)
    logger.info(f"Total {source} records: {len(combined)}")
    
    return combined

def create_time_hexagon_grid(
    country: str,
    start_date: datetime,
    end_date: datetime,
    h3_resolution: int = H3_RESOLUTION
) -> pd.DataFrame:
    logger.info("Creating time-hexagon grid...")
    
    country_bounds = {
        'JP': {'lat_min': 24.0, 'lat_max': 46.0, 'lon_min': 122.0, 'lon_max': 146.0},
        'IN': {'lat_min': 8.0, 'lat_max': 37.0, 'lon_min': 68.0, 'lon_max': 97.0},
        'KR': {'lat_min': 33.0, 'lat_max': 39.0, 'lon_min': 124.0, 'lon_max': 132.0},
        'CN': {'lat_min': 18.0, 'lat_max': 54.0, 'lon_min': 73.0, 'lon_max': 135.0}
    }
    
    bounds = country_bounds.get(country, country_bounds['JP'])
    
    lat_step = 0.5
    lon_step = 0.5
    
    hexagons = set()
    for lat in np.arange(bounds['lat_min'], bounds['lat_max'], lat_step):
        for lon in np.arange(bounds['lon_min'], bounds['lon_max'], lon_step):
            hex_id = h3.latlng_to_cell(lat, lon, h3_resolution)
            hexagons.add(hex_id)
    
    logger.info(f"Generated {len(hexagons)} unique hexagons for {country}")
    
    hours = pd.date_range(start=start_date, end=end_date, freq='h')
    logger.info(f"Generated {len(hours)} hourly timestamps")
    
    grid_data = []
    for hour in hours:
        for hex_id in hexagons:
            lat, lon = h3.cell_to_latlng(hex_id)
            grid_data.append({
                'timestamp': hour,
                f'h3_index_res{h3_resolution}': hex_id,
                f'h3_lat_res{h3_resolution}': lat,
                f'h3_lon_res{h3_resolution}': lon,
                'country': country
            })
            
            if len(grid_data) % 100000 == 0:
                logger.info(f"Created {len(grid_data):,} grid points...")
    
    grid_df = pd.DataFrame(grid_data)
    logger.info(f"Created grid with {len(grid_df):,} time-hexagon combinations")
    
    return grid_df

def merge_data_sources(
    grid_df: pd.DataFrame,
    data_sources: Dict[str, pd.DataFrame],
    h3_resolution: int = H3_RESOLUTION
) -> pd.DataFrame:
    logger.info("Merging data sources...")
    
    h3_col = f'h3_index_res{h3_resolution}'
    result = grid_df.copy()
    
    for source_name, source_df in data_sources.items():
        if source_df is None or source_df.empty:
            logger.warning(f"Skipping empty source: {source_name}")
            continue
        
        logger.info(f"Merging {source_name} with {len(source_df)} records")
        
        if source_name == 'terrain':
            merge_cols = [h3_col]
            drop_cols = [f'h3_lat_res{h3_resolution}', f'h3_lon_res{h3_resolution}', 
                        'country', 'data_source', 'is_static']
        else:
            if 'timestamp' not in source_df.columns:
                logger.warning(f"{source_name} has no timestamp column, skipping")
                continue
            merge_cols = ['timestamp', h3_col]
            drop_cols = [f'h3_lat_res{h3_resolution}', f'h3_lon_res{h3_resolution}', 
                        'country', 'data_source']
        
        for col in drop_cols:
            if col in source_df.columns:
                source_df = source_df.drop(columns=[col])
        
        source_df = source_df.add_prefix(f"{source_name}_")
        for col in merge_cols:
            source_df = source_df.rename(columns={f"{source_name}_{col}": col})
        
        result = result.merge(source_df, on=merge_cols, how='left')
        
        logger.info(f"After merging {source_name}: {len(result)} records, "
                   f"{len(result.columns)} columns")
    
    return result

def fill_missing_values_smart(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Filling missing values...")
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    numeric_cols = [col for col in numeric_cols if not col.startswith('h3_')]
    
    for col in numeric_cols:
        null_count = df[col].isna().sum()
        if null_count > 0:
            null_pct = (null_count / len(df)) * 100
            
            if null_pct < 10:
                df[col] = df[col].fillna(method='ffill', limit=3)
                df[col] = df[col].fillna(method='bfill', limit=3)
            elif null_pct < 50:
                df[col] = df[col].fillna(df[col].mean())
            
            remaining_nulls = df[col].isna().sum()
            if remaining_nulls > 0:
                df[col] = df[col].fillna(0)
            
            logger.debug(f"  {col}: {null_count} -> {remaining_nulls} nulls")
    
    return df

def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding derived features...")
    
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        df['is_daytime'] = df['hour'].between(6, 18).astype(int)
    
    if 'openmeteo_temperature_c_mean' in df.columns and 'openmeteo_humidity_pct_mean' in df.columns:
        temp = df['openmeteo_temperature_c_mean']
        humidity = df['openmeteo_humidity_pct_mean']
        df['heat_index'] = temp + 0.5 * (humidity - 50)
    
    if 'openaq_pm25_ugm3_mean' in df.columns and 'openaq_pm10_ugm3_mean' in df.columns:
        df['pm_ratio'] = df['openaq_pm25_ugm3_mean'] / (df['openaq_pm10_ugm3_mean'] + 1)
    
    if 'firms_fire_count' in df.columns:
        df['has_fire'] = (df['firms_fire_count'] > 0).astype(int)
    
    if 'jartic_avg_speed_kmh' in df.columns and 'jartic_avg_congestion_level' in df.columns:
        df['traffic_index'] = (100 - df['jartic_avg_speed_kmh']) * df['jartic_avg_congestion_level']
    
    return df

def create_unified_dataset(
    country: str,
    start_date: datetime,
    end_date: datetime,
    processed_dir: Path,
    output_path: Optional[Path] = None,
    sources: Optional[List[str]] = None
) -> pd.DataFrame:
    
    if sources is None:
        sources = ['openaq', 'openmeteo', 'nasapower', 'era5', 'firms', 'jartic', 'terrain']
    
    logger.info(f"\n{'='*80}")
    logger.info(f"CREATING UNIFIED DATASET FOR {country}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Sources: {', '.join(sources)}")
    logger.info(f"H3 Resolution: {H3_RESOLUTION}")
    logger.info(f"{'='*80}\n")
    
    grid_df = create_time_hexagon_grid(country, start_date, end_date, H3_RESOLUTION)
    
    data_sources = {}
    for source in sources:
        logger.info(f"\nLoading {source} data...")
        df = load_processed_data(processed_dir, country, source, start_date, end_date)
        if df is not None:
            data_sources[source] = df
        else:
            logger.warning(f"No data loaded for {source}")
    
    unified_df = merge_data_sources(grid_df, data_sources, H3_RESOLUTION)
    
    unified_df = fill_missing_values_smart(unified_df)
    
    unified_df = add_derived_features(unified_df)
    
    unified_df = unified_df.sort_values(['timestamp', f'h3_index_res{H3_RESOLUTION}'])
    
    if output_path:
        unified_df.to_csv(output_path, index=False)
        logger.info(f"\nSaved unified dataset to: {output_path}")
    
    logger.info(f"\n{'='*60}")
    logger.info("UNIFIED DATASET SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Shape: {unified_df.shape[0]:,} rows × {unified_df.shape[1]} columns")
    logger.info(f"Memory usage: {unified_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    if 'timestamp' in unified_df.columns:
        logger.info(f"Time range: {unified_df['timestamp'].min()} to {unified_df['timestamp'].max()}")
    
    logger.info(f"Unique hexagons: {unified_df[f'h3_index_res{H3_RESOLUTION}'].nunique():,}")
    
    logger.info("\nColumn groups:")
    for prefix in ['openaq', 'openmeteo', 'nasapower', 'era5', 'firms', 'jartic', 'terrain']:
        cols = [col for col in unified_df.columns if col.startswith(prefix)]
        if cols:
            logger.info(f"  {prefix}: {len(cols)} columns")
    
    numeric_cols = unified_df.select_dtypes(include=[np.number]).columns
    null_stats = unified_df[numeric_cols].isna().sum()
    high_null_cols = null_stats[null_stats > len(unified_df) * 0.5]
    if not high_null_cols.empty:
        logger.warning(f"\nColumns with >50% missing values:")
        for col, nulls in high_null_cols.items():
            logger.warning(f"  {col}: {(nulls/len(unified_df)*100):.1f}% missing")
    
    logger.info(f"\n{'='*60}\n")
    
    return unified_df

def main():
    parser = argparse.ArgumentParser(
        description='Create unified environmental dataset with H3 hexagonal grid',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create unified dataset for Japan for January 2023
  %(prog)s --country JP --start 2023-01-01 --end 2023-01-31
  
  # Create dataset with specific sources only
  %(prog)s --country JP --start 2023-01-01 --end 2023-01-31 --sources openaq openmeteo era5
  
  # Specify custom output path
  %(prog)s --country JP --start 2023-01-01 --end 2023-01-31 --output unified_jp_202301.csv
        """
    )
    
    parser.add_argument('--country', '-c', type=str, required=True,
                       help='Country code (e.g., JP, IN, KR)')
    parser.add_argument('--start', '-s', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--sources', nargs='+',
                       default=['openaq', 'openmeteo', 'nasapower', 'era5', 'firms', 'jartic', 'terrain'],
                       help='Data sources to include')
    parser.add_argument('--processed-dir', '-p', type=str, default='data/processed',
                       help='Directory containing processed source files')
    parser.add_argument('--output', '-o', type=str,
                       help='Output file path for unified dataset')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d')
    
    if start_date > end_date:
        parser.error("Start date must be before end date")
    
    processed_dir = Path(args.processed_dir)
    if not processed_dir.exists():
        parser.error(f"Processed directory not found: {processed_dir}")
    
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path('data') / 'unified' / (
            f"unified_{args.country.lower()}_"
            f"{start_date.strftime('%Y%m%d')}_to_"
            f"{end_date.strftime('%Y%m%d')}.csv"
        )
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    unified_df = create_unified_dataset(
        country=args.country.upper(),
        start_date=start_date,
        end_date=end_date,
        processed_dir=processed_dir,
        output_path=output_path,
        sources=args.sources
    )
    
    logger.info(f"Unified dataset created successfully: {output_path}")

if __name__ == "__main__":
    main()