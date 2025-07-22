import requests
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any


class OpenAQClient:
    def __init__(self, config_path: str = "config/openaq_config.json", api_key: Optional[str] = None):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.base_url = self.config['api']['base_url']
        self.session = requests.Session()
        
        # Set API key if provided
        if api_key:
            self.session.headers['X-API-Key'] = api_key
        
        self.last_request_time = 0
        self.request_delay = self.config['api']['rate_limit']['delay_between_requests']
    
    def _rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        self._rate_limit()
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to {url}: {e}")
            raise
    
    def get_countries(self) -> Dict:
        return self._make_request('/countries', {'limit': 200})
    
    def get_locations(self, country_ids: Optional[List[int]] = None, 
                     limit: int = 100, page: int = 1) -> Dict:
        params = {
            'limit': limit,
            'page': page
        }
        
        if country_ids:
            params['countries_id'] = ','.join(map(str, country_ids))
        
        return self._make_request('/locations', params)
    
    def get_sensor_measurements(self, sensor_id: int, 
                               date_from: str, date_to: str,
                               limit: int = 1000, page: int = 1) -> Dict:
        params = {
            'date_from': date_from,
            'date_to': date_to,
            'limit': limit,
            'page': page
        }
        
        return self._make_request(f'/sensors/{sensor_id}/measurements', params)
    
    def get_latest_measurements(self, location_id: int) -> Dict:
        return self._make_request(f'/locations/{location_id}/latest')
    
    def save_raw_response(self, data: Dict, endpoint_type: str, identifier: str):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{endpoint_type}_{identifier}_{timestamp}.json"
        filepath = Path(self.config['data_paths']['raw']) / filename
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        return filepath