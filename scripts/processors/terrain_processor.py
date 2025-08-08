from pathlib import Path
from typing import List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class TerrainProcessor(BaseProcessor):
    
    def get_source_name(self) -> str:
        return 'Terrain'
    
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        terrain_dir = self.data_dir / 'terrain' / 'processed'
        
        if not terrain_dir.exists():
            logger.warning(f"Terrain directory not found: {terrain_dir}")
            return []
        
        pattern = f"{self.country.lower()}_elevation_grid.csv"
        elevation_file = terrain_dir / pattern
        
        if elevation_file.exists():
            return [elevation_file]
        
        pattern_alt = f"{self.country}_elevation_grid.csv"
        elevation_file_alt = terrain_dir / pattern_alt
        
        if elevation_file_alt.exists():
            return [elevation_file_alt]
        
        logger.warning(f"No elevation data found for country: {self.country}")
        return []
    
    def calculate_slope_aspect(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(['latitude', 'longitude'])
        
        df['elevation_diff_lat'] = df.groupby('longitude')['elevation_m'].diff()
        df['elevation_diff_lon'] = df.groupby('latitude')['elevation_m'].diff()
        
        df['lat_diff'] = df.groupby('longitude')['latitude'].diff() * 111000
        df['lon_diff'] = df.groupby('latitude')['longitude'].diff() * 111000
        
        df['slope_lat'] = np.arctan2(df['elevation_diff_lat'], df['lat_diff']) * 180 / np.pi
        df['slope_lon'] = np.arctan2(df['elevation_diff_lon'], df['lon_diff']) * 180 / np.pi
        
        df['slope_degrees'] = np.sqrt(df['slope_lat']**2 + df['slope_lon']**2)
        
        df['aspect_degrees'] = np.arctan2(df['elevation_diff_lon'], df['elevation_diff_lat']) * 180 / np.pi
        df['aspect_degrees'] = (df['aspect_degrees'] + 360) % 360
        
        df = df.drop(columns=['elevation_diff_lat', 'elevation_diff_lon', 
                              'lat_diff', 'lon_diff', 'slope_lat', 'slope_lon'])
        
        df['slope_degrees'] = df['slope_degrees'].fillna(0)
        df['aspect_degrees'] = df['aspect_degrees'].fillna(0)
        
        return df
    
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        logger.info(f"Processing Terrain file: {file_path}")
        
        encoding = self.detect_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding)
        
        df = self.calculate_slope_aspect(df)
        
        df = self.add_h3_index(df)
        h3_col = f'h3_index_res{self.H3_RESOLUTION_FINE}'
        
        aggregated = df.groupby(h3_col).agg({
            'elevation_m': ['mean', 'std', 'min', 'max'],
            'slope_degrees': 'mean',
            'aspect_degrees': 'mean',
            f'h3_lat_res{self.H3_RESOLUTION_FINE}': 'first',
            f'h3_lon_res{self.H3_RESOLUTION_FINE}': 'first'
        }).reset_index()
        
        aggregated.columns = ['_'.join(col).strip() if col[1] else col[0] 
                             for col in aggregated.columns.values]
        
        aggregated = aggregated.rename(columns={
            'elevation_m_mean': 'elevation_m',
            'elevation_m_std': 'elevation_std_m',
            'elevation_m_min': 'min_elevation_m',
            'elevation_m_max': 'max_elevation_m',
            'slope_degrees_mean': 'slope_degrees',
            'aspect_degrees_mean': 'aspect_degrees',
            f'h3_lat_res{self.H3_RESOLUTION_FINE}_first': f'h3_lat_res{self.H3_RESOLUTION_FINE}',
            f'h3_lon_res{self.H3_RESOLUTION_FINE}_first': f'h3_lon_res{self.H3_RESOLUTION_FINE}'
        })
        
        aggregated['data_source'] = 'terrain'
        aggregated['country'] = self.country
        
        aggregated['is_static'] = True
        
        return aggregated