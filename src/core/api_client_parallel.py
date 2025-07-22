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
                           endpoint: str, params: Optional[Dict] = None) -> Tuple[Dict, int]:
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
                    return await response.json(), key_index

            except asyncio.TimeoutError:
                self.error_counts[key_index] += 1
                raise aiohttp.ClientTimeout(f"Request timeout on key {key_index}")
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

        # Print stats every 100 requests
        if self.total_requests % 100 == 0:
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
        print(f"\nParallel API Stats (Total: {self.total_requests} requests):")
        for i in range(self.num_keys):
            count = self.request_counts[i]
            errors = self.error_counts[i]
            percentage = (count / self.total_requests * 100) if self.total_requests > 0 else 0
            print(f"  Key {i + 1}: {count} requests ({percentage:.1f}%), {errors} errors")
