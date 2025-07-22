from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import asyncio
import time

import aiohttp
class ParallelAPIClient:
    """Parallel API client that uses multiple keys concurrently for maximum speed"""

    def __init__(self, base_url: str, api_keys: List[str], requests_per_minute_per_key: int = 60):
        self.base_url = base_url
        self.api_keys = api_keys
        self.num_keys = len(api_keys)
        self.rate_limit_backoff = 5  # Configurable backoff time

        # Rate limiting per key
        self.request_delay = 60.0 / requests_per_minute_per_key
        self.last_request_times = defaultdict(float)
        self.key_locks = {i: asyncio.Lock() for i in range(self.num_keys)}

        # Statistics
        self.request_counts = defaultdict(int)
        self.total_requests = 0
        self.error_counts = defaultdict(int)

        print(f"Initialized parallel client with {self.num_keys} API keys")
        print(f"Maximum parallel requests: {self.num_keys}")
        print(f"Theoretical max rate: {requests_per_minute_per_key * self.num_keys} requests/minute")

    async def _get_with_key(self, session: aiohttp.ClientSession, key_index: int,
                           endpoint: str, params: Optional[Dict] = None, retry_attempt: int = 0) -> Tuple[Dict, int]:
        """Make a request with a specific API key"""
        async with self.key_locks[key_index]:
            # Rate limiting for this specific key
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

                    if response.status == 429:  # Rate limit
                        self.error_counts[key_index] += 1
                        await asyncio.sleep(self.rate_limit_backoff)  # Back off
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"Rate limit hit on key {key_index}"
                        )

                    response.raise_for_status()
                    data = await response.json()
                    # Add key info to response for tracking
                    if isinstance(data, dict):
                        data['_api_key_index'] = key_index + 1
                    return data, key_index

            except asyncio.TimeoutError:
                self.error_counts[key_index] += 1
                # If this is the first attempt and we have more keys, try with a different key
                if retry_attempt == 0 and self.num_keys > 1:
                    # Find a different key to retry with
                    next_key_index = (key_index + 1) % self.num_keys
                    print(f"      Timeout on key {key_index + 1}, retrying with key {next_key_index + 1}...")
                    return await self._get_with_key(session, next_key_index, endpoint, params, retry_attempt + 1)
                raise aiohttp.ClientTimeout(f"Request timeout on key {key_index + 1}")
            except Exception:
                self.error_counts[key_index] += 1
                raise

    async def get_single(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make a single request using the least recently used key"""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Find the key that was used longest ago
            key_index = min(range(self.num_keys),
                          key=lambda i: self.last_request_times[i])

            result, _ = await self._get_with_key(session, key_index, endpoint, params)
            return result

    async def get_many(self, requests: List[Tuple[str, Optional[Dict]]]) -> List[Dict]:
        """Make multiple requests in parallel using all available keys"""
        results = []

        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Create tasks for all requests
            tasks = []
            for i, (endpoint, params) in enumerate(requests):
                key_index = i % self.num_keys  # Distribute across keys
                task = self._get_with_key(session, key_index, endpoint, params)
                tasks.append(task)

            # Execute all tasks in parallel with limited concurrency
            semaphore = asyncio.Semaphore(self.num_keys * 2)
            
            async def bounded_task(task):
                async with semaphore:
                    return await task
            
            bounded_tasks = [bounded_task(task) for task in tasks]
            responses = await asyncio.gather(*bounded_tasks, return_exceptions=True)

            # Process results
            for response in responses:
                if isinstance(response, Exception):
                    results.append({"error": str(response)})
                else:
                    result, _ = response
                    results.append(result)

        # Print stats every 50 requests in parallel mode
        if self.total_requests % 50 == 0:
            self._print_stats()

        return results

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Synchronous wrapper for single request"""
        return asyncio.run(self.get_single(endpoint, params))

    def get_batch(self, requests: List[Tuple[str, Optional[Dict]]]) -> List[Dict]:
        """Synchronous wrapper for batch requests"""
        return asyncio.run(self.get_many(requests))

    def _print_stats(self):
        """Print usage statistics"""
        print(f"\n      [PARALLEL MODE] Total requests: {self.total_requests} | Keys active: {sum(1 for c in self.request_counts.values() if c > 0)}/{self.num_keys}")
        # Show top 5 most used keys
        sorted_keys = sorted(range(self.num_keys), key=lambda i: self.request_counts[i], reverse=True)[:5]
        key_info = []
        for i in sorted_keys:
            if self.request_counts[i] > 0:
                key_info.append(f"K{i+1}:{self.request_counts[i]}")
        if key_info:
            print(f"      Top keys: {' | '.join(key_info)}")
