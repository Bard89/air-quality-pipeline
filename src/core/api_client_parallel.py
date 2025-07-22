from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import asyncio
import time

import aiohttp
class ParallelAPIClient:
    def __init__(self, base_url: str, api_keys: List[str], requests_per_minute_per_key: int = 60):
        self.base_url = base_url
        self.api_keys = api_keys
        self.num_keys = len(api_keys)
        self.rate_limit_backoff = 5

        self.request_delay = 60.0 / requests_per_minute_per_key
        self.last_request_times = defaultdict(float)
        self.key_locks = {i: asyncio.Lock() for i in range(self.num_keys)}

        self.request_counts = defaultdict(int)
        self.total_requests = 0
        self.error_counts = defaultdict(int)

        print(f"Initialized parallel client with {self.num_keys} API keys")
        print(f"Maximum parallel requests: {self.num_keys}")
        print(f"Theoretical max rate: {requests_per_minute_per_key * self.num_keys} requests/minute")

    async def _get_with_key(self, session: aiohttp.ClientSession, key_index: int,
                           endpoint: str, params: Optional[Dict] = None, retry_attempt: int = 0) -> Tuple[Dict, int]:
        async with self.key_locks[key_index]:
            current_time = time.time()
            time_since_last = current_time - self.last_request_times[key_index]
            if time_since_last < self.request_delay:
                await asyncio.sleep(self.request_delay - time_since_last)

            url = f"{self.base_url}{endpoint}"
            headers = {'X-API-Key': self.api_keys[key_index]}

            try:
                timeout = aiohttp.ClientTimeout(total=30)
                async with session.get(url, params=params, headers=headers, timeout=timeout) as response:
                    self.last_request_times[key_index] = time.time()
                    self.request_counts[key_index] += 1
                    self.total_requests += 1

                    if response.status == 429:
                        self.error_counts[key_index] += 1
                        await asyncio.sleep(self.rate_limit_backoff)
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"Rate limit hit on key {key_index}"
                        )

                    response.raise_for_status()
                    data = await response.json()
                    if isinstance(data, dict):
                        data['_api_key_index'] = key_index
                        data['_api_key_display'] = key_index + 1
                    return data, key_index

            except asyncio.TimeoutError:
                self.error_counts[key_index] += 1
                if retry_attempt == 0 and self.num_keys > 1:
                    next_key_index = (key_index + 1) % self.num_keys
                    print(f"      Timeout on key {key_index + 1}, retrying with key {next_key_index + 1}...")
                    return await self._get_with_key(session, next_key_index, endpoint, params, retry_attempt + 1)
                raise aiohttp.ClientTimeout(f"Request timeout on key {key_index + 1}")
            except Exception:
                self.error_counts[key_index] += 1
                raise

    async def get_single(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        async with aiohttp.ClientSession(connector=connector) as session:
            key_index = min(range(self.num_keys),
                          key=lambda i: self.last_request_times[i])

            result, _ = await self._get_with_key(session, key_index, endpoint, params)
            return result

    async def get_many(self, requests: List[Tuple[str, Optional[Dict]]]) -> List[Dict]:
        results = []

        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            import random
            
            key_assignments = []
            for i in range(len(requests)):
                key_assignments.append(i % self.num_keys)
            
            random.shuffle(key_assignments)
            
            if len(requests) <= 30:
                print(f"      [DEBUG] Key assignment order: {key_assignments}")
            
            for i, (endpoint, params) in enumerate(requests):
                key_index = key_assignments[i]
                task = self._get_with_key(session, key_index, endpoint, params)
                tasks.append(task)

            semaphore = asyncio.Semaphore(self.num_keys * 2)
            
            async def bounded_task(task):
                async with semaphore:
                    return await task
            
            bounded_tasks = [bounded_task(task) for task in tasks]
            responses = await asyncio.gather(*bounded_tasks, return_exceptions=True)

            for response in responses:
                if isinstance(response, Exception):
                    results.append({"error": str(response)})
                else:
                    result, _ = response
                    results.append(result)

        if self.total_requests % 50 == 0:
            self._print_stats()

        return results

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        return asyncio.run(self.get_single(endpoint, params))

    def get_batch(self, requests: List[Tuple[str, Optional[Dict]]]) -> List[Dict]:
        return asyncio.run(self.get_many(requests))

    def _print_stats(self):
        print(f"\n      [PARALLEL MODE] Total requests: {self.total_requests} | Keys active: {sum(1 for c in self.request_counts.values() if c > 0)}/{self.num_keys}")
        sorted_keys = sorted(range(self.num_keys), key=lambda i: self.request_counts[i], reverse=True)[:5]
        key_info = []
        for i in sorted_keys:
            if self.request_counts[i] > 0:
                key_info.append(f"K{i+1}:{self.request_counts[i]}")
        if key_info:
            print(f"      Top keys: {' | '.join(key_info)}")
