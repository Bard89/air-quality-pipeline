from pathlib import Path
from typing import List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class FIRMSProcessor(BaseProcessor):
    
    def get_source_name(self) -> str:
        return 'FIRMS'
    
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        firms_dir = self.data_dir / 'firms' / 'processed'
        
        if not firms_dir.exists():
            logger.warning(f"FIRMS directory not found: {firms_dir}")
            return []
        
        pattern = f"firms_{self.country.lower()}_fires_*.csv"
        all_files = list(firms_dir.glob(pattern))
        
        relevant_files = []
        for file_path in all_files:
            try:
                filename = file_path.stem
                parts = filename.split('_')
                if len(parts) >= 5:
                    date_start_str = parts[3]
                    date_end_str = parts[4]
                    
                    file_start = datetime.strptime(date_start_str, '%Y%m%d')
                    file_end = datetime.strptime(date_end_str, '%Y%m%d')
                    
                    if not (file_end < start_date or file_start > end_date):
                        relevant_files.append(file_path)
                        logger.info(f"Including file: {file_path.name}")
            except Exception as e:
                logger.warning(f"Could not parse date from filename {file_path}: {e}")
        
        return sorted(relevant_files)
    
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        logger.info(f"Processing FIRMS file: {file_path}")
        
        encoding = self.detect_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding, parse_dates=['timestamp'])
        
        if df.empty:
            logger.warning(f"FIRMS file is empty: {file_path}")
            return pd.DataFrame()
        
        df = self.standardize_timestamps(df)
        
        df = self.add_h3_index(df)
        h3_col = f'h3_index_res{self.H3_RESOLUTION_FINE}'
        
        aggregated = df.groupby(['timestamp', h3_col]).agg({
            'fire_radiative_power': ['count', 'sum', 'max', 'mean'],
            'fire_brightness': 'max',
            'fire_confidence': 'mean',
            'scan_area_km2': 'mean',
            f'h3_lat_res{self.H3_RESOLUTION_FINE}': 'first',
            f'h3_lon_res{self.H3_RESOLUTION_FINE}': 'first'
        }).reset_index()
        
        aggregated.columns = ['_'.join(col).strip() if col[1] else col[0] 
                             for col in aggregated.columns.values]
        
        aggregated = aggregated.rename(columns={
            'fire_radiative_power_count': 'fire_count',
            'fire_radiative_power_sum': 'total_frp_mw',
            'fire_radiative_power_max': 'max_frp_mw',
            'fire_radiative_power_mean': 'avg_frp_mw',
            'fire_brightness_max': 'max_brightness_k',
            'fire_confidence_mean': 'avg_confidence_pct',
            'scan_area_km2_mean': 'avg_scan_area_km2',
            f'h3_lat_res{self.H3_RESOLUTION_FINE}_first': f'h3_lat_res{self.H3_RESOLUTION_FINE}',
            f'h3_lon_res{self.H3_RESOLUTION_FINE}_first': f'h3_lon_res{self.H3_RESOLUTION_FINE}'
        })
        
        for each_hexagon in df[h3_col].unique():
            hex_fires = df[df[h3_col] == each_hexagon]
            if len(hex_fires) > 0:
                fire_lat = hex_fires['latitude'].mean()
                fire_lon = hex_fires['longitude'].mean()
                
                distances = []
                for idx, row in df.iterrows():
                    if row[h3_col] != each_hexagon:
                        dist = np.sqrt((row['latitude'] - fire_lat)**2 + 
                                     (row['longitude'] - fire_lon)**2) * 111
                        distances.append(dist)
                
                if distances:
                    min_distance = min(distances)
                else:
                    min_distance = 0
                
                mask = aggregated[h3_col] == each_hexagon
                aggregated.loc[mask, 'distance_to_nearest_fire_km'] = min_distance
        
        aggregated['data_source'] = 'firms'
        aggregated['country'] = self.country
        
        is_valid, issues = self.validate_data(aggregated)
        if not is_valid:
            logger.warning(f"Data validation issues: {issues}")
        
        return aggregated