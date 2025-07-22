from typing import Dict, Optional
import time

import requests
class RateLimitedAPIClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, requests_per_minute: int = 60):
        self.base_url = base_url
        self.session = requests.Session()
        if api_key:
            self.session.headers['X-API-Key'] = api_key

        self.request_delay = 60.0 / requests_per_minute
        self.last_request_time = 0

    def _rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        self._rate_limit()
        url = f"{self.base_url}{endpoint}"

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
