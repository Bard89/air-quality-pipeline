from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
import numpy as np
import h3
from datetime import datetime, timedelta
import logging
import chardet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseProcessor(ABC):
    
    H3_RESOLUTION_FINE = 8
    H3_RESOLUTION_COARSE = 7
    
    def __init__(self, country: str = 'JP', data_dir: Optional[Path] = None):
        self.country = country.upper()
        self.data_dir = data_dir or Path('data')
        self.processed_dir = self.data_dir / 'processed'
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    @abstractmethod
    def get_source_name(self) -> str:
        pass
    
    @abstractmethod
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        pass
    
    @abstractmethod
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        pass
    
    def detect_encoding(self, file_path: Path) -> str:
        with open(file_path, 'rb') as f:
            raw_data = f.read(100000)
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            
        if confidence < 0.7:
            logger.warning(f"Low confidence ({confidence:.2f}) for encoding detection: {encoding}")
            if self.country == 'JP':
                encoding = 'shift-jis'
                logger.info("Falling back to Shift-JIS for Japanese data")
        
        return encoding
    
    def add_h3_index(self, df: pd.DataFrame, resolution: int = None) -> pd.DataFrame:
        if resolution is None:
            resolution = self.H3_RESOLUTION_FINE
            
        if 'latitude' not in df.columns or 'longitude' not in df.columns:
            raise ValueError("DataFrame must have latitude and longitude columns")
        
        df[f'h3_index_res{resolution}'] = df.apply(
            lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], resolution),
            axis=1
        )
        
        hex_centers = df[f'h3_index_res{resolution}'].apply(h3.cell_to_latlng)
        df[f'h3_lat_res{resolution}'] = hex_centers.apply(lambda x: x[0])
        df[f'h3_lon_res{resolution}'] = hex_centers.apply(lambda x: x[1])
        
        return df
    
    def standardize_timestamps(self, df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        if timestamp_col not in df.columns:
            if 'datetime' in df.columns:
                timestamp_col = 'datetime'
            elif 'time' in df.columns:
                timestamp_col = 'time'
            else:
                raise ValueError(f"No timestamp column found. Available: {df.columns.tolist()}")
        
        df['timestamp'] = pd.to_datetime(df[timestamp_col], utc=True)
        
        df['timestamp'] = df['timestamp'].dt.floor('h')
        
        if timestamp_col != 'timestamp' and timestamp_col in df.columns:
            df = df.drop(columns=[timestamp_col])
        
        return df
    
    def aggregate_to_hexagon_hour(self, df: pd.DataFrame, value_columns: List[str], 
                                  h3_resolution: int = None) -> pd.DataFrame:
        if h3_resolution is None:
            h3_resolution = self.H3_RESOLUTION_FINE
        
        h3_col = f'h3_index_res{h3_resolution}'
        
        if h3_col not in df.columns:
            df = self.add_h3_index(df, h3_resolution)
        
        agg_dict = {}
        for col in value_columns:
            if col in df.columns:
                if df[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                    agg_dict[col] = ['mean', 'std', 'min', 'max', 'count']
                else:
                    agg_dict[col] = ['first', 'count']
        
        agg_dict[f'h3_lat_res{h3_resolution}'] = 'first'
        agg_dict[f'h3_lon_res{h3_resolution}'] = 'first'
        
        grouped = df.groupby(['timestamp', h3_col]).agg(agg_dict)
        
        grouped.columns = ['_'.join(col).strip() if col[1] else col[0] 
                          for col in grouped.columns.values]
        
        grouped = grouped.reset_index()
        
        for col in grouped.columns:
            if col.endswith('_first'):
                grouped = grouped.rename(columns={col: col.replace('_first', '')})
        
        return grouped
    
    def fill_missing_values(self, df: pd.DataFrame, method: str = 'forward') -> pd.DataFrame:
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if method == 'forward':
            df[numeric_cols] = df[numeric_cols].fillna(method='ffill', limit=3)
        elif method == 'interpolate':
            df[numeric_cols] = df[numeric_cols].interpolate(method='linear', limit=3)
        elif method == 'mean':
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
        
        return df
    
    def process_date_range(self, start_date: datetime, end_date: datetime,
                          output_file: Optional[Path] = None) -> pd.DataFrame:
        logger.info(f"Processing {self.get_source_name()} data from {start_date} to {end_date}")
        
        file_paths = self.get_raw_data_paths(start_date, end_date)
        logger.info(f"Found {len(file_paths)} files to process")
        
        if not file_paths:
            logger.warning("No files found for the specified date range")
            return pd.DataFrame()
        
        all_data = []
        max_iterations = 1000
        iteration_count = 0
        
        for file_path in file_paths:
            if iteration_count >= max_iterations:
                logger.error(f"Max iterations ({max_iterations}) reached. Stopping to prevent infinite loop.")
                break
            
            try:
                logger.info(f"Processing file {iteration_count + 1}/{len(file_paths)}: {file_path}")
                df = self.process_raw_file(file_path)
                
                if not df.empty:
                    all_data.append(df)
                    logger.info(f"Processed {len(df)} records from {file_path.name}")
                
                iteration_count += 1
                
                if iteration_count % 10 == 0:
                    logger.info(f"Progress: {iteration_count}/{len(file_paths)} files processed")
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        if not all_data:
            logger.warning("No data was successfully processed")
            return pd.DataFrame()
        
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} total records")
        
        sort_cols = []
        if 'timestamp' in combined_df.columns:
            sort_cols.append('timestamp')
        if 'latitude' in combined_df.columns and 'longitude' in combined_df.columns:
            sort_cols.extend(['latitude', 'longitude'])
        elif f'h3_index_res{self.H3_RESOLUTION_FINE}' in combined_df.columns:
            sort_cols.append(f'h3_index_res{self.H3_RESOLUTION_FINE}')
        
        if sort_cols:
            combined_df = combined_df.sort_values(sort_cols)
        
        if output_file:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            combined_df.to_csv(output_file, index=False)
            logger.info(f"Saved processed data to {output_file}")
        
        return combined_df
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        issues = []
        
        if 'timestamp' not in df.columns:
            issues.append("Missing timestamp column")
        
        if 'latitude' not in df.columns or 'longitude' not in df.columns:
            issues.append("Missing latitude/longitude columns")
        
        if len(df) == 0:
            issues.append("DataFrame is empty")
        
        if 'timestamp' in df.columns:
            null_timestamps = df['timestamp'].isna().sum()
            if null_timestamps > 0:
                issues.append(f"{null_timestamps} null timestamps found")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            inf_count = np.isinf(df[col]).sum()
            if inf_count > 0:
                issues.append(f"{inf_count} infinite values in {col}")
        
        is_valid = len(issues) == 0
        return is_valid, issues