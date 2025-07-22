from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
import time
import json
from pathlib import Path
from src.openaq.client import OpenAQClient


class IncrementalDownloader:
    def __init__(self, client: OpenAQClient):
        self.client = client
        self.checkpoint_file = Path('data/openaq/checkpoints/download_progress.json')
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    
    def save_checkpoint(self, country_code: str, location_index: int, total_locations: int,
                       completed_locations: List[int], output_file: str):
        checkpoint = {
            'country_code': country_code,
            'location_index': location_index,
            'total_locations': total_locations,
            'completed_locations': completed_locations,
            'output_file': output_file,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    
    def load_checkpoint(self):
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return None
    
    def append_to_csv(self, df: pd.DataFrame, output_path: Path):
        if output_path.exists():
            df.to_csv(output_path, mode='a', header=False, index=False)
        else:
            df.to_csv(output_path, index=False)
    
    def download_location_sensors(self, location: Dict, start_date: datetime, 
                                end_date: datetime, parameters: Optional[List[str]] = None) -> pd.DataFrame:
        all_data = []
        location_id = location['id']
        location_name = location.get('name', 'Unknown')
        coords = location.get('coordinates', {})
        city = location.get('locality')
        
        sensors = []
        for sensor in location.get('sensors', []):
            param_name = sensor.get('parameter', {}).get('name')
            if parameters is None or param_name in parameters:
                sensors.append(sensor)
        
        for sensor in sensors:
            sensor_id = sensor.get('id')
            if not sensor_id:
                continue
                
            current_date = start_date
            chunk_days = 90
            
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
                    
                    for m in measurements:
                        period = m.get('period', {})
                        datetime_from = period.get('datetimeFrom', {})
                        parameter = m.get('parameter', {})
                        
                        if datetime_from.get('utc') and m.get('value') is not None:
                            # Parse measurement datetime and check if it's within our date range
                            meas_datetime = pd.to_datetime(datetime_from.get('utc'))
                            if meas_datetime < start_date or meas_datetime > end_date:
                                continue  # Skip measurements outside our date range
                            
                            all_data.append({
                                'datetime': datetime_from.get('utc'),
                                'value': float(m.get('value')),
                                'sensor_id': sensor_id,
                                'location_id': location_id,
                                'location_name': location_name,
                                'city': city,
                                'country': location.get('country', {}).get('code'),
                                'latitude': coords.get('latitude'),
                                'longitude': coords.get('longitude'),
                                'parameter': parameter.get('name'),
                                'unit': parameter.get('units')
                            })
                    
                except Exception as e:
                    print(f"\n  Error sensor {sensor_id}: {str(e)[:50]}")
                
                current_date = chunk_end
                time.sleep(1.05)
        
        if all_data:
            df = pd.DataFrame(all_data)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.drop_duplicates(subset=['datetime', 'sensor_id', 'parameter'])
            df = df.sort_values('datetime')
            return df
        
        return pd.DataFrame()
    
    def download_country_incremental(self, country_code: str, country_id: int,
                                   start_date: datetime, end_date: datetime,
                                   parameters: Optional[List[str]] = None,
                                   max_locations: Optional[int] = None,
                                   resume: bool = True) -> str:
        
        # Check for checkpoint
        checkpoint = None
        completed_locations = []
        start_index = 0
        
        if resume and self.checkpoint_file.exists():
            checkpoint = self.load_checkpoint()
            if checkpoint and checkpoint['country_code'] == country_code:
                print(f"\nResuming from checkpoint (location {checkpoint['location_index']}/{checkpoint['total_locations']})")
                completed_locations = checkpoint['completed_locations']
                start_index = checkpoint['location_index']
                output_path = Path(checkpoint['output_file'])
            else:
                checkpoint = None
        
        if not checkpoint:
            # Start fresh
            filename = f"{country_code.lower()}_airquality_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            output_path = Path(f'data/openaq/processed/{filename}')
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Clear any existing file
            if output_path.exists():
                output_path.unlink()
        
        # Get all locations
        print(f"\nFetching all locations in {country_code}...")
        all_locations = []
        page = 1
        
        while True:
            try:
                response = self.client.get_locations(country_ids=[country_id], page=page, limit=100)
                locations = response.get('results', [])
                if not locations:
                    break
                all_locations.extend(locations)
                if len(locations) < 100:
                    break
                page += 1
            except:
                break
        
        print(f"Found {len(all_locations)} locations")
        
        # Filter to active locations
        active_locations = []
        for loc in all_locations:
            if loc.get('datetimeLast'):
                last_date = loc['datetimeLast'].get('utc', '')
                if last_date and last_date >= start_date.strftime('%Y-%m-%d'):
                    if loc['id'] not in completed_locations:
                        active_locations.append(loc)
        
        # Sort by sensor count
        active_locations.sort(key=lambda x: len(x.get('sensors', [])), reverse=True)
        
        if max_locations and len(active_locations) > max_locations:
            active_locations = active_locations[:max_locations]
        
        total_locations = len(active_locations) + len(completed_locations)
        print(f"Active locations to download: {len(active_locations)}")
        print(f"Already completed: {len(completed_locations)}")
        
        # Calculate estimates
        avg_sensors_per_location = sum(len(loc.get('sensors', [])) for loc in active_locations) / len(active_locations) if active_locations else 0
        days = (end_date - start_date).days
        chunks_per_sensor = (days + 89) // 90
        estimated_requests = int(len(active_locations) * avg_sensors_per_location * chunks_per_sensor)
        estimated_time = estimated_requests * 1.1 / 60
        
        print(f"\nEstimated API requests: {estimated_requests:,}")
        print(f"Estimated time: {estimated_time:.1f} minutes")
        print(f"\nData will be saved incrementally to: {output_path}")
        print("You can safely interrupt and resume later")
        print("-" * 60)
        
        # Download measurements
        start_time = time.time()
        total_measurements = 0
        
        for i, location in enumerate(active_locations[start_index:], start=start_index):
            loc_id = location['id']
            loc_name = location.get('name', 'Unknown')
            sensor_count = len(location.get('sensors', []))
            
            print(f"\nLocation {len(completed_locations)+1}/{total_locations}: {loc_name}")
            print(f"  ID: {loc_id} | Sensors: {sensor_count}")
            
            # Download data for this location
            location_start = time.time()
            df = self.download_location_sensors(location, start_date, end_date, parameters)
            
            if not df.empty:
                # Save immediately
                self.append_to_csv(df, output_path)
                total_measurements += len(df)
                print(f"  ✓ Saved {len(df):,} measurements in {time.time()-location_start:.1f}s")
            else:
                print(f"  ✗ No data found")
            
            # Update checkpoint
            completed_locations.append(loc_id)
            self.save_checkpoint(country_code, i+1, total_locations, completed_locations, str(output_path))
            
            # Progress update
            elapsed = time.time() - start_time
            if elapsed > 0 and (i - start_index + 1) > 0:
                rate = (i - start_index + 1) / elapsed
                remaining = len(active_locations) - i - 1
                eta = remaining / rate if rate > 0 else 0
                print(f"  Progress: {len(completed_locations)}/{total_locations} | "
                      f"Total: {total_measurements:,} measurements | ETA: {eta/60:.1f} min")
        
        # Cleanup checkpoint on completion
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        
        print(f"\n{'='*60}")
        print(f"DOWNLOAD COMPLETE")
        print(f"Total time: {(time.time() - start_time)/60:.1f} minutes")
        print(f"Total measurements: {total_measurements:,}")
        print(f"Output file: {output_path}")
        print(f"{'='*60}")
        
        return str(output_path)