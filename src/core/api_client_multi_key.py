from typing import Dict, List, Optional
import itertools
import time

import requests
class MultiKeyRateLimitedAPIClient:
    def __init__(self, base_url: str, api_keys: List[str], requests_per_minute_per_key: int = 60):
        if not api_keys:
            raise ValueError("At least one API key must be provided")
        self.base_url = base_url
        self.api_keys = api_keys
        self.num_keys = len(api_keys)
        self.sessions = []
        for key in api_keys:
            session = requests.Session()
            session.headers['X-API-Key'] = key
            self.sessions.append(session)

        self.key_iterator = itertools.cycle(range(self.num_keys))
        self.request_delay = 60.0 / requests_per_minute_per_key
        self.last_request_times = [0] * self.num_keys
        self.effective_delay = self.request_delay / self.num_keys
        self.request_counts = [0] * self.num_keys
        self.total_requests = 0

        print(f"Initialized with {self.num_keys} API keys")
        total_rate = requests_per_minute_per_key * self.num_keys
        print(f"Effective rate: {total_rate:,} requests/minute ({total_rate/60:.1f} req/sec)")

    def _get_next_key_index(self) -> int:
        return next(self.key_iterator)

    def _rate_limit(self, key_index: int):
        current_time = time.time()
        time_since_last = current_time - self.last_request_times[key_index]

        if time_since_last < self.effective_delay:
            sleep_time = self.effective_delay - time_since_last
            time.sleep(sleep_time)

        self.last_request_times[key_index] = time.time()

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        key_index = self._get_next_key_index()
        self._rate_limit(key_index)

        url = f"{self.base_url}{endpoint}"
        session = self.sessions[key_index]

        try:
            response = session.get(url, params=params)
            response.raise_for_status()

            self.request_counts[key_index] += 1
            self.total_requests += 1

            if self.total_requests % 100 == 0:
                self._print_stats()

            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"Rate limit hit on key {key_index + 1}. Waiting longer...")
                time.sleep(5)
                return self.get(endpoint, params)
            raise

    def _print_stats(self):
        print(f"\nAPI Key Usage Stats (Total: {self.total_requests} requests):")
        for i, count in enumerate(self.request_counts):
            percentage = (count / self.total_requests * 100) if self.total_requests > 0 else 0
            print(f"  Key {i + 1}: {count} requests ({percentage:.1f}%)")


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
