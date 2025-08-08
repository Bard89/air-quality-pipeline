from pathlib import Path
from typing import List, Optional
import pandas as pd
from datetime import datetime
import logging
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class ERA5Processor(BaseProcessor):
    
    def get_source_name(self) -> str:
        return 'ERA5'
    
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        era5_dir = self.data_dir / 'era5' / 'processed'
        
        if not era5_dir.exists():
            logger.warning(f"ERA5 directory not found: {era5_dir}")
            return []
        
        all_files = list(era5_dir.glob("era5_pbl_*.csv"))
        
        relevant_files = []
        for file_path in all_files:
            try:
                filename = file_path.stem
                parts = filename.split('_')
                if len(parts) >= 4:
                    year_str = parts[2]
                    month_str = parts[3]
                    
                    file_year = int(year_str)
                    file_month = int(month_str)
                    
                    file_start = datetime(file_year, file_month, 1)
                    if file_month == 12:
                        file_end = datetime(file_year + 1, 1, 1)
                    else:
                        file_end = datetime(file_year, file_month + 1, 1)
                    
                    if not (file_end < start_date or file_start > end_date):
                        relevant_files.append(file_path)
                        logger.info(f"Including file: {file_path.name}")
            except Exception as e:
                logger.warning(f"Could not parse date from filename {file_path}: {e}")
        
        return sorted(relevant_files)
    
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        logger.info(f"Processing ERA5 file: {file_path}")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['timestamp'])
        except UnicodeDecodeError:
            encoding = self.detect_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding, parse_dates=['timestamp'])
        
        df = self.standardize_timestamps(df)
        
        df = df.rename(columns={'pbl_height_m': 'pbl_height_meters'})
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            df = self.add_h3_index(df)
        else:
            logger.error(f"Missing latitude/longitude columns. Available: {df.columns.tolist()}")
            return pd.DataFrame()
        
        aggregated_df = self.aggregate_to_hexagon_hour(df, ['pbl_height_meters'])
        
        aggregated_df['data_source'] = 'era5'
        aggregated_df['country'] = self.country
        
        is_valid, issues = self.validate_data(aggregated_df)
        if not is_valid:
            logger.warning(f"Data validation issues: {issues}")
        
        return aggregated_df