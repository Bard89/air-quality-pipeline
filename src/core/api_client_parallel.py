from collections import defaultdict
from contextlib import nullcontext as asyncio_nullcontext
from typing import Dict, List, Optional, Tuple
import asyncio
import threading
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
        # Don't create locks here - they need to be created in the event loop

        self.request_counts = defaultdict(int)
        self.total_requests = 0
        self.error_counts = defaultdict(int)
        self.batch_count = 0
        self.key_rotation_offset = 0
        self.rotation_lock = threading.Lock()  # Thread-safe rotation
        
        # Global rate limiting - will be created per event loop
        self.max_concurrent = self.num_keys  # Use all available keys concurrently

        print(f"Initialized parallel client with {self.num_keys} API keys")
        print(f"Maximum parallel requests: {self.num_keys}")
        print(f"Theoretical max rate: {requests_per_minute_per_key * self.num_keys} requests/minute")

    async def _get_with_key(self, session: aiohttp.ClientSession, key_index: int,
                           endpoint: str, params: Optional[Dict] = None, retry_attempt: int = 0,
                           semaphore: Optional[asyncio.Semaphore] = None,
                           key_lock: Optional[asyncio.Lock] = None) -> Tuple[Dict, int]:
        async with semaphore if semaphore else asyncio_nullcontext():
            async with key_lock if key_lock else asyncio_nullcontext():
                current_time = time.time()
                time_since_last = current_time - self.last_request_times[key_index]
                # Increase minimum delay between requests per key
                min_delay = max(self.request_delay, 2.0)  # At least 2 seconds between requests per key
                if time_since_last < min_delay:
                    await asyncio.sleep(min_delay - time_since_last)

                url = f"{self.base_url}{endpoint}"
                headers = {'X-API-Key': self.api_keys[key_index]}

                try:
                    timeout = aiohttp.ClientTimeout(total=45)  # Increase timeout to 45 seconds
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
                    # Don't retry on timeout - just fail fast
                    page_info = ""
                    if params and 'page' in params:
                        page_info = f" (page {params['page']})"
                    sensor_info = ""
                    if '/sensors/' in endpoint:
                        sensor_info = f" for {endpoint.split('/')[-2]}"
                    print(f"      Timeout on key {key_index + 1}{page_info}{sensor_info}")
                    raise asyncio.TimeoutError(f"Request timeout on key {key_index + 1}{page_info}")
                except Exception:
                    self.error_counts[key_index] += 1
                    raise

    async def get_single(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        async with aiohttp.ClientSession(connector=connector) as session:
            key_index = min(range(self.num_keys),
                          key=lambda i: self.last_request_times[i])

            # Create locks for single requests
            key_lock = asyncio.Lock()
            semaphore = asyncio.Semaphore(1)
            result, _ = await self._get_with_key(session, key_index, endpoint, params,
                                               semaphore=semaphore, key_lock=key_lock)
            return result

    async def get_many(self, requests: List[Tuple[str, Optional[Dict]]]) -> List[Dict]:
        results = []
        self.batch_count += 1
        
        # Create locks and semaphore in the current event loop
        key_locks = {i: asyncio.Lock() for i in range(self.num_keys)}
        global_semaphore = asyncio.Semaphore(self.max_concurrent)

        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            import random
            
            # Distribute requests across keys using thread-safe rotation
            key_assignments = []
            
            with self.rotation_lock:
                if len(requests) <= self.num_keys:
                    # We have enough keys - use each key only once
                    # Start from rotation offset and take consecutive keys
                    selected_keys = []
                    for i in range(len(requests)):
                        key_idx = (self.key_rotation_offset + i) % self.num_keys
                        selected_keys.append(key_idx)
                    key_assignments = selected_keys
                    # Update rotation offset for next call
                    self.key_rotation_offset = (self.key_rotation_offset + len(requests)) % self.num_keys
                else:
                    # More requests than keys - distribute evenly
                    # Each key gets used at most ceil(requests/keys) times
                    assignments_per_key = (len(requests) + self.num_keys - 1) // self.num_keys
                    for key_idx in range(self.num_keys):
                        for _ in range(min(assignments_per_key, len(requests) - len(key_assignments))):
                            key_assignments.append(key_idx)
                    # Shuffle to distribute load
                    random.shuffle(key_assignments)
            
            if len(requests) <= 30:
                print(f"      [DEBUG] Key assignment order: {key_assignments}")
                # Show which keys are being used in this batch
                unique_keys = sorted(set(key_assignments))
                print(f"      [DEBUG] Keys selected for this batch: {[k+1 for k in unique_keys]} ({len(unique_keys)} keys)")
                # Show rotation info
                with self.rotation_lock:
                    print(f"      [DEBUG] Rotation offset for next batch: {self.key_rotation_offset}")
            
            for i, (endpoint, params) in enumerate(requests):
                key_index = key_assignments[i]
                task = self._get_with_key(session, key_index, endpoint, params, 
                                        semaphore=global_semaphore, key_lock=key_locks[key_index])
                tasks.append(task)
                # Small delay to avoid overwhelming the API
                if i > 0 and i % 5 == 0:
                    await asyncio.sleep(0.2)

            # Gather all tasks
            responses = await asyncio.gather(*tasks, return_exceptions=True)

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
        # Create a new event loop for each batch to avoid conflicts
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.get_many(requests))
        finally:
            loop.close()

    def _print_stats(self):
        active_keys = sum(1 for c in self.request_counts.values() if c > 0)
        print(f"\n      [PARALLEL MODE] Batch #{self.batch_count} | Total requests: {self.total_requests} | Keys used: {active_keys}/{self.num_keys}")
        
        # Show all keys usage, not just top 5
        if active_keys > 0:
            min_usage = min(c for c in self.request_counts.values() if c > 0)
            max_usage = max(self.request_counts.values())
            avg_usage = self.total_requests / active_keys if active_keys > 0 else 0
            
            print(f"      Key usage: min={min_usage}, max={max_usage}, avg={avg_usage:.1f}")
            
            # Show keys that haven't been used yet
            unused_keys = [i+1 for i in range(self.num_keys) if self.request_counts[i] == 0]
            if unused_keys and len(unused_keys) <= 10:
                print(f"      Unused keys: {unused_keys}")
