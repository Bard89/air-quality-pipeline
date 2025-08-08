from pathlib import Path
from typing import List, Optional
import pandas as pd
from datetime import datetime
import logging
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class OpenMeteoProcessor(BaseProcessor):
    
    PARAMETER_MAPPING = {
        'temperature': 'temperature_c',
        'humidity': 'humidity_pct',
        'pressure': 'pressure_hpa',
        'wind_speed': 'wind_speed_ms',
        'wind_direction': 'wind_direction_deg',
        'precipitation': 'precipitation_mm',
        'solar_radiation': 'solar_radiation_wm2',
        'cloud_cover': 'cloud_cover_pct',
        'dew_point': 'dew_point_c',
        'apparent_temperature': 'apparent_temp_c',
        'surface_pressure': 'surface_pressure_hpa',
        'visibility': 'visibility_m'
    }
    
    def get_source_name(self) -> str:
        return 'OpenMeteo'
    
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        openmeteo_dir = self.data_dir / 'openmeteo' / 'processed'
        
        if not openmeteo_dir.exists():
            logger.warning(f"OpenMeteo directory not found: {openmeteo_dir}")
            return []
        
        pattern = f"{self.country.lower()}_openmeteo_weather_*.csv"
        all_files = list(openmeteo_dir.glob(pattern))
        
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
        logger.info(f"Processing OpenMeteo file: {file_path}")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['timestamp'])
        except UnicodeDecodeError:
            encoding = self.detect_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding, parse_dates=['timestamp'])
        
        df = self.standardize_timestamps(df)
        
        df['parameter'] = df['parameter'].map(
            lambda x: self.PARAMETER_MAPPING.get(x, f"{x}_unknown")
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
        
        aggregated_df['data_source'] = 'openmeteo'
        aggregated_df['country'] = self.country
        
        is_valid, issues = self.validate_data(aggregated_df)
        if not is_valid:
            logger.warning(f"Data validation issues: {issues}")
        
        return aggregated_df