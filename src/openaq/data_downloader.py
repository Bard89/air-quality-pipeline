import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from src.openaq.client import OpenAQClient


class DataDownloader:
    def __init__(self, client: OpenAQClient):
        self.client = client
        self.chunk_days = 90
        self.batch_size = 10
        self.page_limit = 1000
    
    def download_sensor_data(self, sensor_id: int, start_date: datetime, 
                           end_date: datetime) -> List[Dict]:
        all_measurements = []
        current_date = start_date
        total_days = (end_date - start_date).days
        chunks_needed = (total_days + self.chunk_days - 1) // self.chunk_days
        chunk_num = 0
        
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=self.chunk_days), end_date)
            chunk_num += 1
            
            print(f"  Chunk {chunk_num}/{chunks_needed}: {current_date.date()} to {chunk_end.date()}", end='', flush=True)
            
            try:
                response = self.client.get_sensor_measurements(
                    sensor_id=sensor_id,
                    date_from=current_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    date_to=chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    limit=self.page_limit
                )
                
                measurements = response.get('results', [])
                all_measurements.extend(measurements)
                print(f" - {len(measurements)} measurements")
                
            except Exception as e:
                print(f" - ERROR: {e}")
            
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
    
    def _process_batch_measurements(self, measurements: List[Dict]) -> List[Dict]:
        processed_data = []
        
        for m in measurements:
            period = m.get('period', {})
            datetime_from = period.get('datetimeFrom', {})
            location = m.get('location', {})
            parameter = m.get('parameter', {})
            coords = m.get('coordinates', {})
            
            if datetime_from.get('utc') and m.get('value') is not None:
                processed_data.append({
                    'datetime': datetime_from.get('utc'),
                    'value': float(m.get('value')),
                    'sensor_id': m.get('sensor', {}).get('id'),
                    'location_id': location.get('id'),
                    'location_name': location.get('name'),
                    'city': location.get('locality'),
                    'country': location.get('country'),
                    'latitude': coords.get('latitude'),
                    'longitude': coords.get('longitude'),
                    'parameter': parameter.get('name'),
                    'unit': parameter.get('units')
                })
        
        return processed_data
    
    def _download_batch_chunk(self, batch_ids: List[int], current_date: datetime, 
                            chunk_end: datetime, parameters: Optional[List[str]]) -> List[Dict]:
        all_data = []
        page = 1
        total_measurements = 0
        
        while True:
            try:
                response = self.client.get_measurements(
                    date_from=current_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    date_to=chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    location_ids=batch_ids,
                    parameters=parameters,
                    limit=self.page_limit,
                    page=page
                )
                
                measurements = response.get('results', [])
                if not measurements:
                    break
                
                processed_data = self._process_batch_measurements(measurements)
                all_data.extend(processed_data)
                total_measurements += len(measurements)
                
                meta = response.get('meta', {})
                if meta.get('found', 0) <= page * self.page_limit:
                    break
                
                page += 1
                
            except Exception as e:
                print(f"Error in batch request: {e}")
                break
        
        if total_measurements > 0:
            batch_num = len(all_data) // self.batch_size + 1
            print(f"  Batch {batch_num}: {total_measurements} measurements from {len(batch_ids)} locations")
        
        return all_data
    
    def download_batch_measurements(self, location_ids: List[int], start_date: datetime,
                                   end_date: datetime, parameters: Optional[List[str]] = None) -> pd.DataFrame:
        all_data = []
        current_date = start_date
        
        print(f"Batch downloading from {len(location_ids)} locations")
        print(f"Date range: {(end_date - start_date).days} days")
        
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=self.chunk_days), end_date)
            print(f"\nPeriod: {current_date.date()} to {chunk_end.date()}")
            
            for i in range(0, len(location_ids), self.batch_size):
                batch_ids = location_ids[i:i+self.batch_size]
                batch_data = self._download_batch_chunk(batch_ids, current_date, chunk_end, parameters)
                all_data.extend(batch_data)
            
            current_date = chunk_end
        
        if all_data:
            df = pd.DataFrame(all_data)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.drop_duplicates(subset=['datetime', 'sensor_id', 'parameter'])
            df = df.sort_values(['datetime', 'location_id'])
            return df
        
        return pd.DataFrame()
    
    def _group_sensors_by_parameter(self, sensors: List[Dict]) -> Dict[str, List[Dict]]:
        param_groups = {}
        for sensor in sensors:
            param = sensor['parameter']
            if param not in param_groups:
                param_groups[param] = []
            param_groups[param].append(sensor)
        return param_groups
    
    def _calculate_progress(self, param_groups: Dict[str, List[Dict]], current_param: str, 
                          current_index: int, start_time: float, total_sensors: int) -> None:
        sensors_done = sum(
            len(pg) for pg_name, pg in param_groups.items() 
            if pg_name < current_param or (pg_name == current_param and current_index > 0)
        )
        sensors_done += current_index + 1
        
        if sensors_done > 0:
            avg_time = (time.time() - start_time) / sensors_done
            remaining = total_sensors - sensors_done
            eta = remaining * avg_time
            print(f"Progress: {sensors_done}/{total_sensors} sensors | ETA: {eta/60:.1f} minutes")
    
    def download_multiple_sensors(self, sensors: List[Dict], start_date: datetime, 
                                 end_date: datetime) -> pd.DataFrame:
        all_data = []
        start_time = time.time()
        total_measurements = 0
        
        param_groups = self._group_sensors_by_parameter(sensors)
        
        print(f"\nDownloading from {len(sensors)} sensors across {len(param_groups)} parameters")
        print(f"Date range: {(end_date - start_date).days} days")
        print("-" * 60)
        
        for param, param_sensors in param_groups.items():
            print(f"\n{param.upper()} ({len(param_sensors)} sensors):")
            
            for i, sensor in enumerate(param_sensors):
                sensor_start = time.time()
                print(f"\nSensor {i+1}/{len(param_sensors)}: {sensor['location_name']} (ID: {sensor['sensor_id']})")
                print(f"Parameter: {sensor['parameter']} | Location: {sensor['latitude']:.4f}, {sensor['longitude']:.4f}")
                
                measurements = self.download_sensor_data(sensor['sensor_id'], start_date, end_date)
                
                if measurements:
                    df = self.measurements_to_dataframe(measurements, sensor)
                    all_data.append(df)
                    total_measurements += len(df)
                    
                    elapsed = time.time() - sensor_start
                    print(f"✓ {len(df)} measurements in {elapsed:.1f}s")
                else:
                    print("✗ No data available")
                
                self._calculate_progress(param_groups, param, i, start_time, len(sensors))
        
        print(f"\n{'='*60}")
        print(f"Download completed in {(time.time() - start_time)/60:.1f} minutes")
        print(f"Total measurements: {total_measurements:,}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            return combined_df
        
        return pd.DataFrame()