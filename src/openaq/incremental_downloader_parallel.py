from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import json
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from src.core.checkpoint_manager import CheckpointManager
from src.openaq.client import OpenAQClient
class IncrementalDownloaderParallel:

    def __init__(self, client: OpenAQClient):
        self.client = client
        self.checkpoint_manager = CheckpointManager(Path('data/openaq/checkpoints'))
        self.checkpoint_file = None  # Keep for compatibility

        self.is_parallel = isinstance(getattr(client.api, '__class__', None).__name__, str) and \
                          'Parallel' in client.api.__class__.__name__

        if self.is_parallel:
            print("Using PARALLEL downloader for maximum speed!")
            print(f"Parallel API client detected with {getattr(client.api, 'num_keys', 'unknown')} keys")

    def _get_sequential_downloader(self):
        from src.openaq.incremental_downloader_all import IncrementalDownloaderAll
        return IncrementalDownloaderAll(self.client)

    def save_checkpoint(self, country_code: str, location_index: int, total_locations: int,
                       completed_locations: List[int], output_file: str,
                       current_location_id: Optional[int] = None):
        self.checkpoint_manager.save_checkpoint(
            country_code, location_index, total_locations,
            completed_locations, output_file, current_location_id
        )

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

    def fetch_sensor_pages_parallel_sync(self, sensor_ids: List[int], max_pages_per_sensor: int = 100, location_name: str = "") -> Dict[int, List]:
        all_requests = []
        request_map = {}
        
        # API has a hard limit at page 16 (max 16,000 measurements per sensor)
        max_pages_per_sensor = min(max_pages_per_sensor, 16)
        
        for sensor_id in sensor_ids:
            # Be very conservative with initial parallel fetching
            # Fetch more pages when we have many keys available
            available_keys = getattr(self.client.api, 'num_keys', 1)
            if available_keys >= 20:
                # With many keys, we can fetch more pages initially
                if len(sensor_ids) >= 6:
                    pages_to_fetch = 2  # 2 pages for 6+ sensors
                elif len(sensor_ids) >= 4:
                    pages_to_fetch = 3  # 3 pages for 4-5 sensors
                else:
                    pages_to_fetch = min(4, max(3, available_keys // (len(sensor_ids) * 3)))
            else:
                # Conservative for fewer keys
                if len(sensor_ids) >= 6:
                    pages_to_fetch = 1
                elif len(sensor_ids) >= 4:
                    pages_to_fetch = 2
                else:
                    pages_to_fetch = min(3, max(2, available_keys // (len(sensor_ids) * 4))) if available_keys > 1 else 2
            
            # Apply max_pages_per_sensor limit if specified
            pages_to_fetch = min(pages_to_fetch, max_pages_per_sensor)
            
            for page in range(1, pages_to_fetch + 1):
                endpoint = f'/sensors/{sensor_id}/measurements'
                params = {'limit': 1000, 'page': page}
                request_index = len(all_requests)
                all_requests.append((endpoint, params))
                request_map[request_index] = (sensor_id, page)

        pages_per_sensor = len(all_requests) // len(sensor_ids) if sensor_ids else 0
        loc_info = f" for {location_name}" if location_name else ""
        print(f"      Fetching {len(all_requests)} pages in parallel ({pages_per_sensor} pages × {len(sensor_ids)} sensors){loc_info}")
        print(f"      Using {getattr(self.client.api, 'num_keys', 1)} API keys available")

        if hasattr(self.client.api, 'get_batch'):
            results = self.client.api.get_batch(all_requests)
            key_sequence = []
            key_usage = {}
            for i, result in enumerate(results):
                if isinstance(result, dict) and '_api_key_display' in result:
                    key_display = result['_api_key_display']
                    key_sequence.append(key_display)
                    key_usage[key_display] = key_usage.get(key_display, 0) + 1
            if key_sequence:
                # Sort keys for display
                sorted_keys = sorted(set(key_sequence))
                key_usage_sorted = sorted(key_usage.items())
                print(f"      Keys used: {sorted_keys} (total: {len(sorted_keys)} unique keys)")
                # Show usage distribution
                usage_summary = ', '.join([f"key{k}: {v}x" for k, v in key_usage_sorted[:10]])
                if len(key_usage_sorted) > 10:
                    usage_summary += f"... ({len(key_usage_sorted)-10} more)"
                print(f"      Usage: {usage_summary}")
        else:
            results = []
            for endpoint, params in all_requests:
                results.append(self.client.api.get(endpoint, params))

        sensor_data = {sensor_id: [] for sensor_id in sensor_ids}

        successful_requests = 0
        failed_requests = 0
        
        for i, result in enumerate(results):
            if 'error' not in result:
                sensor_id, page = request_map[i]
                if '_api_key_index' in result:
                    del result['_api_key_index']
                if '_api_key_display' in result:
                    del result['_api_key_display']
                measurements = result.get('results', [])
                sensor_data[sensor_id].extend(measurements)
                successful_requests += 1
            else:
                failed_requests += 1
        
        if failed_requests > 0:
            print(f"      Warning: {failed_requests}/{len(results)} requests failed (will retry sequentially)")

        return sensor_data

    def fetch_remaining_sensor_data(self, sensor_id: int, start_page: int = 3, max_pages: int = 100) -> List[Dict]:
        all_data = []
        page = start_page
        consecutive_errors = 0
        total_fetched = len(all_data)
        
        # API has a hard limit at page 16 (max 16,000 measurements per sensor)
        max_pages = min(max_pages, 16)

        while page <= max_pages:
            try:
                params = {'limit': 1000, 'page': page}
                response = self.client.api.get(f'/sensors/{sensor_id}/measurements', params)
                measurements = response.get('results', [])

                if not measurements:
                    break

                all_data.extend(measurements)
                total_fetched = len(all_data)
                consecutive_errors = 0

                if page % 5 == 0:
                    print(f"\r      Fetched up to page {page} ({total_fetched} measurements total)", end='', flush=True)

                if len(measurements) < 1000:
                    print(f"\n      Reached end of data at page {page}")
                    break

                page += 1

            except Exception as e:
                error_msg = str(e)[:100]
                
                if ('408' in error_msg or 'timeout' in error_msg.lower()) and consecutive_errors == 0:
                    print(f"\n      Timeout on page {page}, retrying once...")
                    consecutive_errors += 1
                    time.sleep(1)
                    continue
                
                print(f"\n      Error on page {page}: {error_msg}")
                print(f"      Total fetched: {total_fetched} measurements")
                if page >= 16 and ('408' in error_msg or 'timeout' in error_msg.lower()):
                    print(f"      Note: API has a hard limit at page 16")
                break

        return all_data

    def process_location_parallel(self, location: Dict, output_path: Path,
                                parameters: Optional[List[str]] = None, max_requests: Optional[int] = None) -> int:
        location_id = location['id']
        location_name = location.get('name', 'Unknown')
        coords = location.get('coordinates', {})
        city = location.get('locality')
        country_code = location.get('country', {}).get('code')
        total_measurements = 0

        sensors = []
        sensor_info = {}
        for sensor in location.get('sensors', []):
            param_name = sensor.get('parameter', {}).get('name')
            if parameters is None or param_name in parameters:
                sensor_id = sensor.get('id')
                if sensor_id:
                    sensors.append(sensor_id)
                    sensor_info[sensor_id] = param_name

        if not sensors:
            return 0

        print(f"\n  Location: {location_name} (ID: {location_id})")
        if max_requests:
            print(f"  Processing {len(sensors)} sensors in parallel (max {max_requests} requests)...")
        else:
            print(f"  Processing {len(sensors)} sensors in parallel...")
        
        # If max_requests is specified, limit pages per sensor
        if max_requests and max_requests < len(sensors) * 3:
            max_pages_override = max(1, max_requests // len(sensors))
            sensor_data = self.fetch_sensor_pages_parallel_sync(sensors, location_name=location_name, max_pages_per_sensor=max_pages_override)
        else:
            sensor_data = self.fetch_sensor_pages_parallel_sync(sensors, location_name=location_name)

        for sensor_id, measurements in sensor_data.items():
            param_name = sensor_info.get(sensor_id, 'unknown')

            pages_fetched = len(measurements) // 1000
            if pages_fetched > 0 and len(measurements) % 1000 == 0:
                print(f"    Sensor {sensor_id} ({param_name}): {len(measurements)}+ measurements...")
                additional_data = self.fetch_remaining_sensor_data(sensor_id, start_page=pages_fetched + 1)
                measurements.extend(additional_data)

            if measurements:
                sensor_df_data = []
                for m in measurements:
                    period = m.get('period', {})
                    datetime_from = period.get('datetimeFrom', {})
                    parameter = m.get('parameter', {})

                    if datetime_from.get('utc') and m.get('value') is not None:
                        sensor_df_data.append({
                            'datetime': datetime_from.get('utc'),
                            'value': float(m.get('value')),
                            'sensor_id': sensor_id,
                            'location_id': location_id,
                            'location_name': location_name,
                            'city': city,
                            'country': country_code,
                            'latitude': coords.get('latitude'),
                            'longitude': coords.get('longitude'),
                            'parameter': parameter.get('name'),
                            'unit': parameter.get('units')
                        })

                if sensor_df_data:
                    df = pd.DataFrame(sensor_df_data)
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df = df.drop_duplicates(subset=['datetime', 'sensor_id', 'parameter'])
                    df = df.sort_values('datetime')

                    self.append_to_csv(df, output_path)
                    sensor_measurements = len(df)
                    total_measurements += sensor_measurements
                    print(f"    ✓ Sensor {sensor_id} ({param_name}): {sensor_measurements} measurements saved")

        return total_measurements

    def process_locations_batch(self, locations_batch: List[Tuple[int, Dict]], 
                               output_path: Path, parameters: Optional[List[str]] = None) -> Tuple[List[int], int]:
        """Process multiple locations in parallel using thread pool"""
        batch_completed = []
        batch_measurements = 0
        
        def process_single_location(loc_info: Tuple[int, Dict, int]) -> Tuple[int, int]:
            idx, location, max_requests = loc_info
            loc_id = location['id']
            
            try:
                if self.is_parallel and len(location.get('sensors', [])) > 3:
                    loc_measurements = self.process_location_parallel(
                        location, output_path, parameters, max_requests=max_requests
                    )
                else:
                    sequential_downloader = self._get_sequential_downloader()
                    loc_measurements = sequential_downloader.download_location_sensors_all(
                        location, output_path, parameters
                    )
                
                return (loc_id, loc_measurements)
            except Exception as e:
                print(f"  ✗ Error processing location {location.get('name', 'Unknown')}: {str(e)}")
                return (loc_id, 0)
        
        # Calculate optimal number of concurrent locations
        total_sensors = sum(len(loc[1].get('sensors', [])) for loc in locations_batch)
        avg_sensors_per_location = total_sensors / len(locations_batch) if locations_batch else 1
        
        # Estimate how many locations we can process concurrently
        available_keys = getattr(self.client.api, 'num_keys', 1)
        # Calculate how many locations we can process without key reuse
        # With many keys, we need to ensure no key is used twice
        if available_keys >= 10:
            # Calculate requests per location (sensors * pages)
            pages_per_sensor = 3 if available_keys >= 20 else 2
            requests_per_location = int(avg_sensors_per_location * pages_per_sensor)
            
            # Calculate how many full locations we can handle
            full_locations = int(available_keys / requests_per_location)
            remaining_keys = available_keys % requests_per_location
            
            # If we have enough remaining keys for a partial location, add one more
            # Only if remaining keys >= sensors (at least 1 page per sensor)
            if remaining_keys >= avg_sensors_per_location:
                max_concurrent_locations = min(len(locations_batch), full_locations + 1)
            else:
                max_concurrent_locations = min(len(locations_batch), full_locations)
            
            # Ensure at least 1 location
            max_concurrent_locations = max(1, max_concurrent_locations)
        else:
            max_concurrent_locations = max(1, min(
                len(locations_batch),
                int(available_keys / max(avg_sensors_per_location * 3, 1))
            ))
        
        # Calculate key budget for each location
        pages_per_sensor = 3 if available_keys >= 20 else 2
        requests_per_location = int(avg_sensors_per_location * pages_per_sensor)
        
        # Assign key budgets to each location
        location_budgets = []
        keys_used = 0
        for i in range(max_concurrent_locations):
            if i < int(available_keys / requests_per_location):
                # Full budget
                location_budgets.append(requests_per_location)
                keys_used += requests_per_location
            else:
                # Partial budget with remaining keys
                remaining = available_keys - keys_used
                if remaining > 0:
                    location_budgets.append(remaining)
                    keys_used += remaining
        
        print(f"\n  Processing batch of {len(locations_batch)} locations ({max_concurrent_locations} concurrently)...")
        print(f"  Available keys: {available_keys}, Avg sensors/location: {avg_sensors_per_location:.1f}")
        if available_keys >= 10:
            remaining_keys = available_keys % requests_per_location
            print(f"  Requests per location: {requests_per_location} ({pages_per_sensor} pages × {avg_sensors_per_location:.1f} sensors)")
            print(f"  Key allocation: {int(available_keys / requests_per_location)} full locations + {remaining_keys} spare keys")
            if max_concurrent_locations > 1 and location_budgets:
                print(f"  Location budgets: {location_budgets} requests each")
        
        # Show which locations are in this batch
        loc_names = [loc[1].get('name', f'ID:{loc[1]["id"]}') for loc in locations_batch[:5]]
        if len(locations_batch) > 5:
            loc_names.append(f"... and {len(locations_batch)-5} more")
        print(f"  Locations in batch: {', '.join(loc_names)}")
        
        # Create tuples with location info and budget
        locations_with_budget = [
            (idx, loc, location_budgets[i]) 
            for i, (idx, loc) in enumerate(locations_batch[:max_concurrent_locations])
        ]
        
        with ThreadPoolExecutor(max_workers=max_concurrent_locations) as executor:
            results = list(executor.map(process_single_location, locations_with_budget))
        
        for loc_id, measurements in results:
            batch_completed.append(loc_id)
            batch_measurements += measurements
        
        # Print cumulative key usage statistics
        if hasattr(self.client.api, '_print_stats'):
            self.client.api._print_stats()
        
        return batch_completed, batch_measurements

    def download_country_all(self, country_code: str, country_id: int,
                           parameters: Optional[List[str]] = None,
                           max_locations: Optional[int] = None,
                           resume: bool = True) -> str:

        # Use checkpoint manager to handle resume logic
        output_path, checkpoint = self.checkpoint_manager.get_or_create_output_file(country_code, resume)
        
        completed_locations = []
        start_index = 0
        
        if checkpoint:
            print(f"\nResuming from checkpoint (location {checkpoint['location_index']}/{checkpoint['total_locations']})")
            completed_locations = checkpoint['completed_locations']
            start_index = checkpoint['location_index']
        else:
            # New download - create CSV headers
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

        # Sort all locations first
        all_locations.sort(key=lambda x: len(x.get('sensors', [])), reverse=True)
        
        # If we have a start_index, use it to slice the locations
        if start_index > 0:
            active_locations = all_locations[start_index:]
            print(f"Resuming from location index {start_index}")
        else:
            active_locations = [loc for loc in all_locations if loc['id'] not in completed_locations]

        if max_locations and len(active_locations) > max_locations:
            active_locations = active_locations[:max_locations]

        total_locations = len(all_locations)
        already_completed = start_index if start_index > 0 else len(completed_locations)
        print(f"Locations to download: {len(active_locations)}")
        print(f"Already completed: {already_completed}")

        if self.is_parallel:
            print(f"\nPARALLEL MODE ENABLED - Using all {getattr(self.client.api, 'num_keys', 1)} API keys concurrently!")

        print(f"\nDownloading ALL AVAILABLE DATA")
        print(f"Data will be saved to: {output_path}")
        print("Data is saved after each location completes")
        print("-" * 60)

        start_time = time.time()
        total_measurements = 0
        
        # Determine if we should use parallel location processing
        avg_sensors = sum(len(loc.get('sensors', [])) for loc in active_locations) / len(active_locations) if active_locations else 0
        available_keys = getattr(self.client.api, 'num_keys', 1)
        
        print(f"\nParallel mode decision:")
        print(f"  Available API keys: {available_keys}")
        print(f"  Average sensors per location: {avg_sensors:.1f}")
        
        # Use parallel location processing if we have many keys and sensors won't saturate them
        # With 23 keys, we can handle locations with up to ~15 sensors on average
        sensors_per_key_threshold = 0.7  # We want at least 70% of keys to be utilized
        max_avg_sensors = available_keys * sensors_per_key_threshold / 3  # 3 pages per sensor estimate
        
        use_parallel_locations = self.is_parallel and available_keys > 1 and avg_sensors < max_avg_sensors
        
        if use_parallel_locations:
            print(f"  → Using PARALLEL LOCATION PROCESSING")
            print(f"  → Will process multiple locations concurrently to utilize all {available_keys} API keys")
            
            # Process locations in batches
            # More aggressive batching: use more locations per batch when we have many keys
            batch_size = max(5, min(30, int(available_keys / max(avg_sensors * 1.5, 1))))  # 1.5 pages per sensor estimate
            print(f"  → Batch size: {batch_size} locations per batch")
            
            i = 0  # Start from 0, since active_locations is already sliced
            while i < len(active_locations):
                batch_end = min(i + batch_size, len(active_locations))
                batch = [(idx, loc) for idx, loc in enumerate(active_locations[i:batch_end], start=i)]
                
                current_start = start_index + i + 1
                current_end = min(current_start + len(batch) - 1, total_locations)
                print(f"\nProcessing locations {current_start}-{current_end} of {total_locations}")
                for _, loc in batch:
                    print(f"  - {loc.get('name', 'Unknown')} (ID: {loc['id']}, Sensors: {len(loc.get('sensors', []))})")  
                
                batch_start = time.time()
                
                try:
                    self.save_checkpoint(country_code, start_index + i, total_locations, completed_locations,
                                       str(output_path), batch[0][1]['id'])
                    
                    batch_completed, batch_measurements = self.process_locations_batch(
                        batch, output_path, parameters
                    )
                    
                    completed_locations.extend(batch_completed)
                    total_measurements += batch_measurements
                    
                    batch_elapsed = time.time() - batch_start
                    if batch_measurements > 0:
                        rate = batch_measurements / batch_elapsed if batch_elapsed > 0 else 0
                        print(f"\n  ✓ Batch complete: {batch_measurements:,} measurements in {batch_elapsed:.1f}s ({rate:.0f} meas/sec)")
                    
                except KeyboardInterrupt:
                    print("\n\nInterrupted! All completed data has been saved.")
                    print("Resume by running the same command again.")
                    raise
                
                i = batch_end
                # Save actual progress (start_index + current position)
                self.save_checkpoint(country_code, start_index + i, total_locations, completed_locations, str(output_path))
                
                elapsed = time.time() - start_time
                if elapsed > 0:
                    locations_rate = len(completed_locations) / elapsed * 60  # locations per minute
                    remaining = len(active_locations) - i
                    eta = remaining / locations_rate if locations_rate > 0 else 0
                    print(f"  Progress: {len(completed_locations)}/{total_locations} | "
                          f"Total: {total_measurements:,} measurements | ETA: {eta:.1f} min")
        
        else:
            print(f"  → Using SEQUENTIAL location processing (parallel sensors within each location)")
            if self.is_parallel and avg_sensors >= max_avg_sensors:
                print(f"  → Reason: High sensor density ({avg_sensors:.1f} sensors/location) can utilize all keys")
            
            # Original sequential processing for high sensor count locations
            for i, location in enumerate(active_locations[start_index:], start=start_index):
                loc_id = location['id']
                loc_name = location.get('name', 'Unknown')
                sensor_count = len(location.get('sensors', []))

                print(f"\nLocation {len(completed_locations)+1}/{total_locations}: {loc_name}")
                print(f"  ID: {loc_id} | Sensors: {sensor_count}")

                location_start = time.time()

                try:
                    self.save_checkpoint(country_code, i, total_locations, completed_locations,
                                       str(output_path), loc_id)

                    if self.is_parallel and sensor_count > 3:
                        loc_measurements = self.process_location_parallel(
                            location, output_path, parameters
                        )
                    else:
                        sequential_downloader = self._get_sequential_downloader()
                        loc_measurements = sequential_downloader.download_location_sensors_all(
                            location, output_path, parameters
                        )

                    total_measurements += loc_measurements

                    if loc_measurements > 0:
                        elapsed = time.time() - location_start
                        rate = loc_measurements / elapsed if elapsed > 0 else 0
                        print(f"  ✓ Location complete: {loc_measurements:,} measurements in {elapsed:.1f}s ({rate:.0f} meas/sec)")
                    else:
                        print("  ✗ No data found")

                except KeyboardInterrupt:
                    print("\n\nInterrupted! All completed data has been saved.")
                    print("Resume by running the same command again.")
                    raise
                except Exception as e:
                    print(f"  ✗ Error processing location: {str(e)}")
                    print("  Skipping to next location...")

                completed_locations.append(loc_id)
                self.save_checkpoint(country_code, i+1, total_locations, completed_locations, str(output_path))

                elapsed = time.time() - start_time
                if elapsed > 0 and (i - start_index + 1) > 0:
                    rate = (i - start_index + 1) / elapsed
                    remaining = len(active_locations) - i - 1
                    eta = remaining / rate if rate > 0 else 0
                    print(f"  Progress: {len(completed_locations)}/{total_locations} | "
                          f"Total: {total_measurements:,} measurements | ETA: {eta/60:.1f} min")

        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

        print(f"\n{'='*60}")
        print("DOWNLOAD COMPLETE")
        print(f"Total time: {(time.time() - start_time)/60:.1f} minutes")
        print(f"Total measurements: {total_measurements:,}")
        print(f"Output file: {output_path}")
        print(f"{'='*60}")

        return str(output_path)