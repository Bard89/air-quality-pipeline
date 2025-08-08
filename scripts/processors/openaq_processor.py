from pathlib import Path
from typing import List, Optional
import pandas as pd
from datetime import datetime
import logging
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class OpenAQProcessor(BaseProcessor):
    
    PARAMETER_MAPPING = {
        'pm25': 'pm25_ugm3',
        'pm10': 'pm10_ugm3',
        'no2': 'no2_ugm3',
        'so2': 'so2_ugm3',
        'o3': 'o3_ugm3',
        'co': 'co_ppm',
        'bc': 'bc_ugm3'
    }
    
    def get_source_name(self) -> str:
        return 'OpenAQ'
    
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        openaq_dir = self.data_dir / 'openaq' / 'processed'
        
        if not openaq_dir.exists():
            logger.warning(f"OpenAQ directory not found: {openaq_dir}")
            return []
        
        pattern = f"{self.country.lower()}_airquality_*.csv"
        all_files = list(openaq_dir.glob(pattern))
        
        relevant_files = []
        for file_path in all_files:
            try:
                df_sample = pd.read_csv(file_path, nrows=5, parse_dates=['datetime'])
                if 'datetime' in df_sample.columns:
                    file_start = pd.to_datetime(df_sample['datetime'].min()).tz_localize(None)
                    file_end = pd.to_datetime(df_sample['datetime'].max()).tz_localize(None)
                    
                    file_df_full = pd.read_csv(file_path, parse_dates=['datetime'], nrows=1000000)
                    file_end = pd.to_datetime(file_df_full['datetime'].max()).tz_localize(None)
                    
                    if not (file_end < start_date or file_start > end_date):
                        relevant_files.append(file_path)
                        logger.info(f"Including file: {file_path.name} (covers {file_start} to {file_end})")
            except Exception as e:
                logger.warning(f"Could not check date range for {file_path}: {e}")
        
        return sorted(relevant_files)
    
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        logger.info(f"Processing OpenAQ file: {file_path}")
        
        encoding = self.detect_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding, parse_dates=['datetime'])
        
        df = self.standardize_timestamps(df, 'datetime')
        
        df = df.rename(columns={
            'parameter': 'parameter_raw',
            'value': 'value_raw',
            'unit': 'unit_raw'
        })
        
        df['parameter'] = df['parameter_raw'].map(
            lambda x: self.PARAMETER_MAPPING.get(x, f"{x}_unknown")
        )
        
        pivot_df = df.pivot_table(
            index=['timestamp', 'location_id', 'location_name', 'latitude', 'longitude', 'city', 'country'],
            columns='parameter',
            values='value_raw',
            aggfunc='mean'
        ).reset_index()
        
        pivot_df = self.add_h3_index(pivot_df)
        
        value_columns = [col for col in pivot_df.columns if col in self.PARAMETER_MAPPING.values()]
        aggregated_df = self.aggregate_to_hexagon_hour(pivot_df, value_columns)
        
        aggregated_df['data_source'] = 'openaq'
        aggregated_df['country'] = self.country
        
        is_valid, issues = self.validate_data(aggregated_df)
        if not is_valid:
            logger.warning(f"Data validation issues: {issues}")
        
        return aggregated_df