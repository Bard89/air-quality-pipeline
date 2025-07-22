from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import pandas as pd
from src.openaq.client import OpenAQClient


class DataDownloader:
    def __init__(self, client: OpenAQClient):
        self.client = client
    
    def download_sensor_data(self, sensor_id: int, start_date: datetime, 
                           end_date: datetime, chunk_days: int = 3) -> List[Dict]:
        all_measurements = []
        current_date = start_date
        
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
            
            try:
                response = self.client.get_sensor_measurements(
                    sensor_id=sensor_id,
                    date_from=current_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    date_to=chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    limit=1000
                )
                
                measurements = response.get('results', [])
                all_measurements.extend(measurements)
                
            except Exception as e:
                print(f"Error downloading {current_date.date()} to {chunk_end.date()}: {e}")
            
            current_date = chunk_end
        
        return all_measurements
    
    def measurements_to_dataframe(self, measurements: List[Dict], sensor_info: Dict) -> pd.DataFrame:
        data = []
        
        for m in measurements:
            period = m.get('period', {})
            datetime_from = period.get('datetimeFrom', {})
            
            if datetime_from.get('utc') and m.get('value') is not None:
                data.append({
                    'datetime': datetime_from.get('utc'),
                    'value': float(m.get('value')),
                    'sensor_id': sensor_info['sensor_id'],
                    'location_id': sensor_info['location_id'],
                    'location_name': sensor_info['location_name'],
                    'city': sensor_info['city'],
                    'country': sensor_info['country'],
                    'latitude': sensor_info['latitude'],
                    'longitude': sensor_info['longitude'],
                    'parameter': sensor_info['parameter'],
                    'unit': sensor_info['unit']
                })
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.drop_duplicates(subset=['datetime'])
        df = df.sort_values('datetime')
        
        return df
    
    def download_multiple_sensors(self, sensors: List[Dict], start_date: datetime, 
                                 end_date: datetime) -> pd.DataFrame:
        all_data = []
        
        for i, sensor in enumerate(sensors):
            print(f"\nDownloading sensor {i+1}/{len(sensors)}: {sensor['location_name']} ({sensor['sensor_id']})")
            print(f"Location: {sensor['latitude']}, {sensor['longitude']}")
            
            measurements = self.download_sensor_data(sensor['sensor_id'], start_date, end_date)
            
            if measurements:
                df = self.measurements_to_dataframe(measurements, sensor)
                all_data.append(df)
                print(f"Downloaded {len(df)} measurements")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            return combined_df
        
        return pd.DataFrame()