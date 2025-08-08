from pathlib import Path
from typing import List, Optional
import pandas as pd
from datetime import datetime
import logging
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class NASAPowerProcessor(BaseProcessor):
    
    PARAMETER_MAPPING = {
        'temperature': 'nasa_temperature_c',
        'humidity': 'nasa_humidity_pct',
        'pressure': 'nasa_pressure_hpa',
        'wind_speed': 'nasa_wind_speed_ms',
        'wind_direction': 'nasa_wind_direction_deg',
        'precipitation': 'nasa_precipitation_mm',
        'solar_radiation': 'nasa_solar_radiation_wm2',
        'cloud_cover': 'nasa_cloud_cover_pct',
        'dew_point': 'nasa_dew_point_c'
    }
    
    def get_source_name(self) -> str:
        return 'NASA_POWER'
    
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        nasapower_dir = self.data_dir / 'nasapower' / 'processed'
        
        if not nasapower_dir.exists():
            logger.warning(f"NASA POWER directory not found: {nasapower_dir}")
            return []
        
        pattern = f"{self.country.lower()}_nasapower_weather_*.csv"
        all_files = list(nasapower_dir.glob(pattern))
        
        relevant_files = []
        for file_path in all_files:
            try:
                filename = file_path.stem
                parts = filename.split('_')
                if len(parts) >= 6:
                    date_start_str = parts[3]
                    date_end_str = parts[5]
                    
                    file_start = datetime.strptime(date_start_str, '%Y%m%d')
                    file_end = datetime.strptime(date_end_str, '%Y%m%d')
                    
                    if not (file_end < start_date or file_start > end_date):
                        relevant_files.append(file_path)
                        logger.info(f"Including file: {file_path.name}")
            except Exception as e:
                logger.warning(f"Could not parse date from filename {file_path}: {e}")
        
        return sorted(relevant_files)
    
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        logger.info(f"Processing NASA POWER file: {file_path}")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['timestamp'])
        except UnicodeDecodeError:
            encoding = self.detect_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding, parse_dates=['timestamp'])
        
        df = self.standardize_timestamps(df)
        
        df['parameter'] = df['parameter'].map(
            lambda x: self.PARAMETER_MAPPING.get(x, f"nasa_{x}_unknown")
        )
        
        pivot_df = df.pivot_table(
            index=['timestamp', 'location_id', 'location_name', 'latitude', 'longitude', 'city', 'country'],
            columns='parameter',
            values='value',
            aggfunc='mean'
        ).reset_index()
        
        pivot_df = self.add_h3_index(pivot_df)
        
        value_columns = [col for col in pivot_df.columns if col in self.PARAMETER_MAPPING.values()]
        aggregated_df = self.aggregate_to_hexagon_hour(pivot_df, value_columns)
        
        aggregated_df['data_source'] = 'nasapower'
        aggregated_df['country'] = self.country
        
        is_valid, issues = self.validate_data(aggregated_df)
        if not is_valid:
            logger.warning(f"Data validation issues: {issues}")
        
        return aggregated_df