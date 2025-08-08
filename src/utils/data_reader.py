from typing import Optional, List, Union
import pandas as pd
from pathlib import Path
from datetime import datetime
from ..infrastructure.data_reference import ExternalDataManager


class DataReader:
    def __init__(self, external_data_manager: Optional[ExternalDataManager] = None):
        self.manager = external_data_manager or ExternalDataManager()
    
    def read_openaq(
        self,
        country: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        parameters: Optional[List[str]] = None
    ) -> pd.DataFrame:
        latest_file = self.manager.get_latest_file('openaq', country=country)
        
        if not latest_file:
            raise FileNotFoundError(f"No OpenAQ data found for {country}")
        
        df = pd.read_csv(latest_file, parse_dates=['datetime'])
        
        if start_date:
            df = df[df['datetime'] >= start_date]
        if end_date:
            df = df[df['datetime'] <= end_date]
        if parameters and 'parameter' in df.columns:
            df = df[df['parameter'].isin(parameters)]
        
        return df
    
    def read_weather(
        self,
        source: str,
        country: str,
        start_date: datetime,
        end_date: datetime,
        parameters: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if source not in ['openmeteo', 'nasapower']:
            raise ValueError(f"Invalid weather source: {source}")
        
        files = self.manager.get_date_range_files(
            source, start_date, end_date, country
        )
        
        if not files:
            raise FileNotFoundError(
                f"No {source} data found for {country} "
                f"between {start_date} and {end_date}"
            )
        
        dfs = []
        for file in files:
            df = pd.read_csv(file)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            elif 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            dfs.append(df)
        
        result = pd.concat(dfs, ignore_index=True)
        
        date_col = 'timestamp' if 'timestamp' in result.columns else 'datetime'
        result = result[(result[date_col] >= start_date) & 
                       (result[date_col] <= end_date)]
        
        if parameters and 'parameter' in result.columns:
            result = result[result['parameter'].isin(parameters)]
        
        return result
    
    def read_elevation(self, country: str) -> pd.DataFrame:
        file_path = self.manager.get_processed_path('terrain') / f"{country.upper()}_elevation_grid.csv"
        
        if not file_path.exists():
            file_path = self.manager.get_processed_path('terrain') / f"{country.lower()}_elevation_grid.csv"
        
        if not file_path.exists():
            raise FileNotFoundError(f"No elevation data found for {country}")
        
        return pd.read_csv(file_path)
    
    def read_fires(self, country: str) -> pd.DataFrame:
        files = self.manager.list_files('firms', f"firms_{country.lower()}_*")
        
        if not files:
            raise FileNotFoundError(f"No fire data found for {country}")
        
        latest_file = files[-1]
        return pd.read_csv(latest_file, parse_dates=['acq_date'])
    
    def read_era5_pbl(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        files = self.manager.list_files('era5', 'era5_pbl_*.csv')
        
        relevant_files = []
        for file in files:
            parts = file.stem.split('_')
            if len(parts) >= 4:
                try:
                    year = int(parts[2])
                    month = int(parts[3])
                    file_date = datetime(year, month, 1)
                    
                    if (start_date.replace(day=1) <= file_date <= 
                        end_date.replace(day=1)):
                        relevant_files.append(file)
                except (ValueError, IndexError):
                    continue
        
        if not relevant_files:
            raise FileNotFoundError(
                f"No ERA5 PBL data found between {start_date} and {end_date}"
            )
        
        dfs = []
        for file in relevant_files:
            df = pd.read_csv(file)
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
            dfs.append(df)
        
        result = pd.concat(dfs, ignore_index=True)
        
        if 'time' in result.columns:
            result = result[(result['time'] >= start_date) & 
                          (result['time'] <= end_date)]
        
        return result