from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import json
import time

import pandas as pd

from src.openaq.client import OpenAQClient
class IncrementalDownloaderAll:

    def __init__(self, client: OpenAQClient):
        self.client = client
        self.checkpoint_dir = Path('data/openaq/checkpoints')
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = None

    def save_checkpoint(self, country_code: str, location_index: int, total_locations: int,
                       completed_locations: List[int], output_file: str,
                       current_location_id: Optional[int] = None,
                       current_sensor_index: Optional[int] = None):
        checkpoint = {
            'country_code': country_code,
            'location_index': location_index,
            'total_locations': total_locations,
            'completed_locations': completed_locations,
            'output_file': output_file,
            'current_location_id': current_location_id,
            'current_sensor_index': current_sensor_index,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, indent=2)

    def load_checkpoint(self):
        if self.checkpoint_file and self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def append_to_csv(self, df: pd.DataFrame, output_path: Path):
        if output_path.exists():
            df.to_csv(output_path, mode='a', header=False, index=False)
        else:
            df.to_csv(output_path, index=False)

    def fetch_all_sensor_data(self, sensor_id: int, max_pages: int = 1000) -> List[Dict]:
        all_data = []
        page = 1
        total_fetched = 0
        sensor_start_time = time.time()

        print("      Fetching ALL available data...")
        
        # API has a hard limit at page 16 (max 16,000 measurements per sensor)
        max_pages = min(max_pages, 16)

        while page <= max_pages:
            try:
                params = {
                    'limit': 1000,
                    'page': page
                }

                response = self.client.api.get(f'/sensors/{sensor_id}/measurements', params)
                measurements = response.get('results', [])

                if not measurements:
                    print(f"\n      No more data after page {page-1}")
                    break

                page_total = len(measurements)
                total_fetched += page_total

                for m in measurements:
                    period = m.get('period', {})
                    datetime_from = period.get('datetimeFrom', {})

                    if datetime_from.get('utc') and m.get('value') is not None:
                        all_data.append({
                            'datetime': datetime_from.get('utc'),
                            'value': m.get('value'),
                            'period': period,
                            'parameter': m.get('parameter', {})
                        })

                if page % 5 == 0:
                    elapsed = time.time() - sensor_start_time
                    print(f"\r      Page {page}: {total_fetched} measurements fetched ({elapsed:.1f}s)", end='', flush=True)

                if page_total < 1000:
                    print("\n      Reached end of data")
                    break

                page += 1

            except Exception as e:
                error_msg = str(e)[:100]
                    
                if ('408' in error_msg or 'timeout' in error_msg.lower()) and page > 1:
                    print(f"\n      Timeout on page {page}, stopping...")
                    if page >= 16:
                        print(f"      Note: API has a hard limit at page 16")
                else:
                    print(f"\n      Error on page {page}: {error_msg}")
                break

        print(f"\n      Total: {total_fetched} measurements fetched")
        return all_data

    def download_location_sensors_all(self, location: Dict, output_path: Path,
                                    parameters: Optional[List[str]] = None,
                                    start_sensor_index: int = 0) -> int:
        location_id = location['id']
        location_name = location.get('name', 'Unknown')
        coords = location.get('coordinates', {})
        city = location.get('locality')
        country_code = location.get('country', {}).get('code')
        total_measurements = 0

        sensors = []
        for sensor in location.get('sensors', []):
            param_name = sensor.get('parameter', {}).get('name')
            if parameters is None or param_name in parameters:
                sensors.append(sensor)

        print(f"  Processing {len(sensors)} sensors...")

        for i, sensor in enumerate(sensors[start_sensor_index:], start=start_sensor_index):
            sensor_id = sensor.get('id')
            param_name = sensor.get('parameter', {}).get('name', 'unknown')
            if not sensor_id:
                continue

            print(f"    Sensor {i+1}/{len(sensors)}: {param_name} (ID: {sensor_id})")

            raw_data = self.fetch_all_sensor_data(sensor_id)

            if raw_data:
                sensor_data = []
                for item in raw_data:
                    sensor_data.append({
                        'datetime': item['datetime'],
                        'value': float(item['value']),
                        'sensor_id': sensor_id,
                        'location_id': location_id,
                        'location_name': location_name,
                        'city': city,
                        'country': country_code,
                        'latitude': coords.get('latitude'),
                        'longitude': coords.get('longitude'),
                        'parameter': item['parameter'].get('name'),
                        'unit': item['parameter'].get('units')
                    })

                df = pd.DataFrame(sensor_data)
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df.drop_duplicates(subset=['datetime', 'sensor_id', 'parameter'])
                df = df.sort_values('datetime')

                self.append_to_csv(df, output_path)
                sensor_measurements = len(df)
                total_measurements += sensor_measurements
                print(f"      ✓ Saved {sensor_measurements} measurements")

                if not df.empty:
                    date_min = df['datetime'].min()
                    date_max = df['datetime'].max()
                    print(f"      Date range: {date_min.strftime('%Y-%m-%d %H:%M')} to {date_max.strftime('%Y-%m-%d %H:%M')}")
            else:
                print("      ✗ No data found")

        return total_measurements

    def download_country_all(self, country_code: str, country_id: int,
                           parameters: Optional[List[str]] = None,
                           max_locations: Optional[int] = None,
                           resume: bool = True) -> str:

        checkpoint = None
        completed_locations = []
        start_index = 0
        current_sensor_index = 0
        
        param_str = '_'.join(parameters) if parameters else 'all'
        checkpoint_filename = f"checkpoint_{country_code.lower()}_{param_str}.json"
        self.checkpoint_file = self.checkpoint_dir / checkpoint_filename

        if resume and self.checkpoint_file.exists():
            checkpoint = self.load_checkpoint()
            if checkpoint and checkpoint['country_code'] == country_code:
                print(f"\nResuming from checkpoint (location {checkpoint['location_index']}/{checkpoint['total_locations']})")
                completed_locations = checkpoint['completed_locations']
                start_index = checkpoint['location_index']
                current_sensor_index = checkpoint.get('current_sensor_index', 0)
                output_path = Path(checkpoint['output_file'])
            else:
                checkpoint = None

        if not checkpoint:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{country_code.lower()}_airquality_all_{timestamp}.csv"
            output_path = Path(f'data/openaq/processed/{filename}')
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if output_path.exists():
                output_path.unlink()

            headers = ['datetime', 'value', 'sensor_id', 'location_id', 'location_name',
                      'city', 'country', 'latitude', 'longitude', 'parameter', 'unit']
            pd.DataFrame(columns=headers).to_csv(output_path, index=False)

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
            except Exception:
                break

        print(f"Found {len(all_locations)} locations")

        active_locations = [loc for loc in all_locations if loc['id'] not in completed_locations]
        active_locations.sort(key=lambda x: len(x.get('sensors', [])), reverse=True)

        if max_locations and len(active_locations) > max_locations:
            active_locations = active_locations[:max_locations]

        total_locations = len(active_locations) + len(completed_locations)
        print(f"Locations to download: {len(active_locations)}")
        print(f"Already completed: {len(completed_locations)}")

        print("\nDownloading ALL AVAILABLE DATA (no date filtering)")
        print(f"Data will be saved to: {output_path}")
        print("Data is saved after EACH SENSOR completes")
        print("You can safely interrupt and resume later")

        start_time = time.time()
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 60)
        total_measurements = 0

        for i, location in enumerate(active_locations[start_index:], start=start_index):
            loc_id = location['id']
            loc_name = location.get('name', 'Unknown')
            sensor_count = len(location.get('sensors', []))

            print(f"\nLocation {len(completed_locations)+1}/{total_locations}: {loc_name}")
            print(f"  ID: {loc_id} | Sensors: {sensor_count}")

            location_start = time.time()

            try:
                self.save_checkpoint(country_code, i, total_locations, completed_locations,
                                   str(output_path), loc_id, current_sensor_index)

                loc_measurements = self.download_location_sensors_all(
                    location, output_path, parameters, current_sensor_index
                )

                total_measurements += loc_measurements
                current_sensor_index = 0

                if loc_measurements > 0:
                    print(f"  ✓ Location complete: {loc_measurements:,} measurements in {time.time()-location_start:.1f}s")
                else:
                    print("  ✗ No data found")

            except KeyboardInterrupt:
                print("\n\nInterrupted! All completed sensor data has been saved.")
                print("Resume by running the same command again.")
                raise
            except Exception as e:
                print(f"  ✗ Error processing location: {str(e)}")
                print("  Skipping to next location...")
                current_sensor_index = 0

            completed_locations.append(loc_id)
            self.save_checkpoint(country_code, i+1, total_locations, completed_locations, str(output_path))

            elapsed = time.time() - start_time
            if elapsed > 0 and (i - start_index + 1) > 0:
                rate = (i - start_index + 1) / elapsed
                remaining = len(active_locations) - i - 1
                eta = remaining / rate if rate > 0 else 0

                elapsed_hours = int(elapsed // 3600)
                elapsed_minutes = int((elapsed % 3600) // 60)
                elapsed_seconds = int(elapsed % 60)
                elapsed_str = f"{elapsed_hours}h {elapsed_minutes}m {elapsed_seconds}s" if elapsed_hours > 0 else f"{elapsed_minutes}m {elapsed_seconds}s"

                eta_hours = int(eta // 3600)
                eta_minutes = int((eta % 3600) // 60)
                eta_str = f"{eta_hours}h {eta_minutes}m" if eta_hours > 0 else f"{eta_minutes}m"

                print(f"  Progress: {len(completed_locations)}/{total_locations} | "
                      f"Total: {total_measurements:,} measurements | "
                      f"Elapsed: {elapsed_str} | ETA: {eta_str}")

        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

        total_elapsed = time.time() - start_time
        total_hours = int(total_elapsed // 3600)
        total_minutes = int((total_elapsed % 3600) // 60)
        total_seconds = int(total_elapsed % 60)

        print(f"\n{'='*60}")
        print("DOWNLOAD COMPLETE")
        if total_hours > 0:
            print(f"Total time: {total_hours}h {total_minutes}m {total_seconds}s")
        else:
            print(f"Total time: {total_minutes}m {total_seconds}s")
        print(f"Total measurements: {total_measurements:,}")
        if total_elapsed > 0:
            print(f"Average rate: {total_measurements/total_elapsed:.0f} measurements/second")
        print(f"Output file: {output_path}")
        print(f"{'='*60}")

        return str(output_path)
