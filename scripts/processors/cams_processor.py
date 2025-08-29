"""CAMS data processor - converts NetCDF files to CSV with H3 aggregation."""

import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
import xarray as xr
import h3

from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class CAMSProcessor(BaseProcessor):
    """Processor for CAMS PM2.5 and atmospheric composition data."""
    
    def __init__(self, country: str = "JP"):
        """Initialize CAMS processor.
        
        Args:
            country: Country code (default: JP)
        """
        super().__init__()
        self.country = country
        self.source = "cams"
        
        # CAMS specific parameters
        self.variables_map = {
            'pm2p5': 'pm25_ugm3',
            'pm10': 'pm10_ugm3',
            'no2': 'no2_ugm3',
            'so2': 'so2_ugm3',
            'co': 'co_ppm',
            'o3': 'o3_ugm3'
        }
        
    def get_source_name(self) -> str:
        """Get the data source name."""
        return "cams"
        
    def get_raw_data_paths(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Path]:
        """Get paths to raw NetCDF files for date range.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of NetCDF file paths
        """
        raw_dir = Path(f"data/cams/raw")
        if not raw_dir.exists():
            return []
            
        # Get all NC files in date range
        nc_files = []
        for nc_file in sorted(raw_dir.glob("*.nc")):
            # Extract date from filename (e.g., cams_eac4_20240101.nc)
            try:
                date_str = nc_file.stem.split('_')[-1]
                file_date = datetime.strptime(date_str, '%Y%m%d')
                
                if start_date.date() <= file_date.date() <= end_date.date():
                    nc_files.append(nc_file)
            except:
                continue
                
        return nc_files
        
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        """Process a single raw NetCDF file.
        
        Args:
            file_path: Path to NetCDF file
            
        Returns:
            Processed DataFrame
        """
        return self.process_file(file_path)
        
    def process_file(self, nc_path: Path) -> pd.DataFrame:
        """Process a single NetCDF file to DataFrame.
        
        Args:
            nc_path: Path to NetCDF file
            
        Returns:
            DataFrame with processed data
        """
        logger.info(f"Processing {nc_path.name}")
        
        try:
            # Open NetCDF file
            ds = xr.open_dataset(nc_path)
            
            # Rename dimensions for consistency
            if 'longitude' in ds.dims:
                ds = ds.rename({'longitude': 'lon'})
            if 'latitude' in ds.dims:
                ds = ds.rename({'latitude': 'lat'})
            if 'valid_time' in ds.dims:
                ds = ds.rename({'valid_time': 'time'})
                
            # Process each variable
            dfs = []
            
            for var_name, output_name in self.variables_map.items():
                if var_name not in ds.variables:
                    continue
                    
                logger.debug(f"  Processing variable: {var_name}")
                
                # Get data array
                data = ds[var_name]
                
                # Convert units if needed (kg/m³ to µg/m³)
                if 'kg' in str(data.attrs.get('units', '')):
                    data = data * 1e9
                    
                # Convert to DataFrame
                df = data.to_dataframe().reset_index()
                
                # Rename columns
                df = df.rename(columns={
                    'lat': 'latitude',
                    'lon': 'longitude',
                    var_name: output_name
                })
                
                # Keep only non-null values
                df = df[df[output_name].notna()].copy()
                
                # Add timestamp if it's called 'time'
                if 'time' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['time'])
                    df = df.drop('time', axis=1)
                    
                dfs.append(df)
                
            # Combine all variables
            if not dfs:
                logger.warning(f"No valid data found in {nc_path}")
                return pd.DataFrame()
                
            # Merge all variables on location and time
            result = dfs[0]
            for df in dfs[1:]:
                merge_cols = ['timestamp', 'latitude', 'longitude']
                result = result.merge(df, on=merge_cols, how='outer')
                
            # Add metadata
            result['data_source'] = 'cams'
            result['country'] = self.country
            
            # Add H3 index for spatial aggregation
            if not result.empty:
                result['h3_index_res8'] = result.apply(
                    lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], 8),
                    axis=1
                )
                
            logger.info(f"  Extracted {len(result)} records from {nc_path.name}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing {nc_path}: {e}")
            return pd.DataFrame()
            
    def process(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        output_file: Optional[str] = None
    ) -> pd.DataFrame:
        """Process CAMS NetCDF files to CSV.
        
        Args:
            start_date: Start date for processing
            end_date: End date for processing
            output_file: Output CSV file path
            
        Returns:
            Processed DataFrame
        """
        # Set default dates if not provided
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
            
        logger.info(f"Processing CAMS data from {start_date.date()} to {end_date.date()}")
        
        # Find NetCDF files in raw directory
        raw_dir = Path(f"data/cams/raw")
        if not raw_dir.exists():
            logger.error(f"Directory not found: {raw_dir}")
            return pd.DataFrame()
            
        # Get all NC files in date range
        nc_files = []
        for nc_file in sorted(raw_dir.glob("*.nc")):
            # Extract date from filename (e.g., cams_eac4_20240101.nc)
            try:
                date_str = nc_file.stem.split('_')[-1]
                file_date = datetime.strptime(date_str, '%Y%m%d')
                
                if start_date.date() <= file_date.date() <= end_date.date():
                    nc_files.append(nc_file)
            except:
                continue
                
        if not nc_files:
            logger.warning(f"No NetCDF files found for date range in {raw_dir}")
            return pd.DataFrame()
            
        logger.info(f"Found {len(nc_files)} NetCDF files to process")
        
        # Process each file
        all_data = []
        for nc_file in nc_files:
            df = self.process_file(nc_file)
            if not df.empty:
                all_data.append(df)
                
        if not all_data:
            logger.warning("No data extracted from NetCDF files")
            return pd.DataFrame()
            
        # Combine all data
        result = pd.concat(all_data, ignore_index=True)
        
        # Sort by timestamp and location
        if 'timestamp' in result.columns:
            result = result.sort_values(['timestamp', 'latitude', 'longitude'])
            
        # Aggregate to H3 hexagons with hourly resolution
        if 'h3_index_res8' in result.columns and 'timestamp' in result.columns:
            logger.info("Aggregating to H3 hexagons...")
            
            # Round timestamps to hour
            result['timestamp_hour'] = pd.to_datetime(result['timestamp']).dt.floor('h')
            
            # Group by hexagon and hour
            agg_dict = {}
            for col in result.columns:
                if col.endswith('_ugm3') or col.endswith('_ppm'):
                    agg_dict[col] = ['mean', 'std', 'min', 'max', 'count']
                    
            if agg_dict:
                grouped = result.groupby(['h3_index_res8', 'timestamp_hour']).agg(agg_dict)
                
                # Flatten column names
                grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
                grouped = grouped.reset_index()
                
                # Add hexagon center coordinates
                grouped['h3_lat_res8'] = grouped['h3_index_res8'].apply(
                    lambda x: h3.cell_to_latlng(x)[0]
                )
                grouped['h3_lon_res8'] = grouped['h3_index_res8'].apply(
                    lambda x: h3.cell_to_latlng(x)[1]
                )
                
                # Rename timestamp column
                grouped = grouped.rename(columns={'timestamp_hour': 'timestamp'})
                
                result = grouped
                
        # Save to CSV
        if output_file:
            output_path = Path(output_file)
        else:
            output_dir = Path(f"data/cams/processed")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            date_range_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            output_path = output_dir / f"{self.country.lower()}_cams_{date_range_str}_{timestamp_str}.csv"
            
        logger.info(f"Saving to {output_path}")
        result.to_csv(output_path, index=False)
        logger.info(f"Saved {len(result)} records to {output_path}")
        
        # Print summary
        self.print_summary(result)
        
        return result
        
    def print_summary(self, df: pd.DataFrame):
        """Print summary statistics of processed data."""
        logger.info("\n" + "="*60)
        logger.info("CAMS Data Processing Summary")
        logger.info("="*60)
        
        if df.empty:
            logger.info("No data processed")
            return
            
        # Basic info
        logger.info(f"Total records: {len(df):,}")
        
        if 'timestamp' in df.columns:
            logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
        if 'h3_index_res8' in df.columns:
            logger.info(f"Unique H3 hexagons: {df['h3_index_res8'].nunique()}")
            
        # PM2.5 statistics
        pm25_cols = [col for col in df.columns if 'pm25' in col and 'mean' in col]
        if pm25_cols:
            pm25_col = pm25_cols[0]
            logger.info(f"\nPM2.5 Statistics (µg/m³):")
            logger.info(f"  Mean: {df[pm25_col].mean():.1f}")
            logger.info(f"  Median: {df[pm25_col].median():.1f}")
            logger.info(f"  Min: {df[pm25_col].min():.1f}")
            logger.info(f"  Max: {df[pm25_col].max():.1f}")
            logger.info(f"  Std: {df[pm25_col].std():.1f}")
            
            # WHO guideline exceedances
            who_annual = 5  # WHO annual guideline
            who_24hr = 15   # WHO 24-hr guideline
            exceed_annual = (df[pm25_col] > who_annual).sum()
            exceed_24hr = (df[pm25_col] > who_24hr).sum()
            
            logger.info(f"\nWHO Guideline Exceedances:")
            logger.info(f"  > {who_annual} µg/m³ (annual): {exceed_annual} ({exceed_annual/len(df)*100:.1f}%)")
            logger.info(f"  > {who_24hr} µg/m³ (24-hr): {exceed_24hr} ({exceed_24hr/len(df)*100:.1f}%)")
            
        # Geographic coverage
        if 'h3_lat_res8' in df.columns and 'h3_lon_res8' in df.columns:
            logger.info(f"\nGeographic Coverage:")
            logger.info(f"  Latitude range: {df['h3_lat_res8'].min():.2f} to {df['h3_lat_res8'].max():.2f}")
            logger.info(f"  Longitude range: {df['h3_lon_res8'].min():.2f} to {df['h3_lon_res8'].max():.2f}")
            
        logger.info("="*60)