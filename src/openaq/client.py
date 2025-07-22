from typing import Dict, List, Optional, Union
from src.core.api_client import RateLimitedAPIClient
from src.core.api_client_multi_key import MultiKeyRateLimitedAPIClient
from src.core.api_client_parallel import ParallelAPIClient
from src.core.data_storage import DataStorage


class OpenAQClient:
    def __init__(self, api_keys: Union[str, List[str]], storage: Optional[DataStorage] = None, 
                 parallel: bool = False):
        # Support both single key and multiple keys
        if isinstance(api_keys, str):
            self.api = RateLimitedAPIClient(
                base_url="https://api.openaq.org/v3",
                api_key=api_keys,
                requests_per_minute=60
            )
        else:
            # Use parallel client if requested and multiple keys available
            if parallel and len(api_keys) > 1:
                self.api = ParallelAPIClient(
                    base_url="https://api.openaq.org/v3",
                    api_keys=api_keys,
                    requests_per_minute_per_key=60
                )
            else:
                # Use multi-key client for better rate limits
                self.api = MultiKeyRateLimitedAPIClient(
                    base_url="https://api.openaq.org/v3",
                    api_keys=api_keys,
                    requests_per_minute_per_key=60
                )
        self.storage = storage or DataStorage()
    
    def get_countries(self, limit: int = 200) -> Dict:
        return self.api.get('/countries', {'limit': limit})
    
    def get_locations(self, country_ids: Optional[List[int]] = None, 
                     limit: int = 100, page: int = 1) -> Dict:
        params = {'limit': limit, 'page': page}
        if country_ids:
            params['countries_id'] = ','.join(map(str, country_ids))
        return self.api.get('/locations', params)
    
    def get_location_details(self, location_id: int) -> Dict:
        return self.api.get(f'/locations/{location_id}')
    
    def get_sensor_measurements(self, sensor_id: int, date_from: str, date_to: str,
                               limit: int = 1000, page: int = 1) -> Dict:
        params = {
            'date_from': date_from,
            'date_to': date_to,
            'limit': limit,
            'page': page
        }
        return self.api.get(f'/sensors/{sensor_id}/measurements', params)
    
    def get_measurements(self, date_from: str, date_to: str, location_ids: Optional[List[int]] = None,
                        sensor_ids: Optional[List[int]] = None, parameters: Optional[List[str]] = None,
                        limit: int = 1000, page: int = 1) -> Dict:
        params = {
            'date_from': date_from,
            'date_to': date_to,
            'limit': limit,
            'page': page
        }
        if location_ids:
            params['locations_id'] = ','.join(map(str, location_ids))
        if sensor_ids:
            params['sensors_id'] = ','.join(map(str, sensor_ids))
        if parameters:
            params['parameters_name'] = ','.join(parameters)
        return self.api.get('/measurements', params)
    
    def get_latest_measurements(self, location_id: int) -> Dict:
        return self.api.get(f'/locations/{location_id}/latest')
    
    def save_response(self, data: Dict, identifier: str) -> None:
        if self.storage:
            return self.storage.save_json(data, 'openaq', identifier)