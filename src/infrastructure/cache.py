from typing import Optional, Any, Dict
import time
import asyncio
from collections import OrderedDict
import json
import hashlib
from ..domain.interfaces import Cache
import logging


logger = logging.getLogger(__name__)


class MemoryCache(Cache):
    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            value, expiry = self._cache[key]
            
            if time.time() > expiry:
                del self._cache[key]
                self._misses += 1
                return None
            
            self._cache.move_to_end(key)
            self._hits += 1
            return value

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = ttl or self.default_ttl
        expiry = time.time() + ttl
        
        async with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            
            self._cache[key] = (value, expiry)
            
            if len(self._cache) > self.max_size:
                self._evict_expired()
                
                if len(self._cache) > self.max_size:
                    oldest_key = next(iter(self._cache))
                    del self._cache[oldest_key]
                    logger.debug(f"Evicted oldest cache entry: {oldest_key}")

    async def delete(self, key: str) -> None:
        async with self._lock:
            if key in self._cache:
                del self._cache[key]

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def _evict_expired(self) -> None:
        current_time = time.time()
        expired_keys = [
            k for k, (_, expiry) in self._cache.items()
            if current_time > expiry
        ]
        
        for key in expired_keys:
            del self._cache[key]

    def get_stats(self) -> Dict[str, Any]:
        total_requests = self._hits + self._misses
        hit_rate = self._hits / total_requests if total_requests > 0 else 0
        
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "total_requests": total_requests
        }


class KeyBuilder:
    @staticmethod
    def build_location_key(country_code: str, parameter: Optional[str] = None) -> str:
        parts = ["locations", country_code]
        if parameter:
            parts.append(parameter)
        return ":".join(parts)

    @staticmethod
    def build_sensor_key(location_id: str) -> str:
        return f"sensors:{location_id}"

    @staticmethod
    def build_measurement_key(sensor_id: str, page: int = 1) -> str:
        return f"measurements:{sensor_id}:page:{page}"

    @staticmethod
    def build_hash_key(data: Dict[str, Any]) -> str:
        json_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(json_str.encode()).hexdigest()


class CachedDataSource:
    def __init__(self, data_source: Any, cache: Cache):
        self.data_source = data_source
        self.cache = cache
        self.key_builder = KeyBuilder()

    async def find_locations(self, country_code: Optional[str] = None, parameter: Optional[str] = None, limit: Optional[int] = None):
        cache_key = self.key_builder.build_location_key(
            country_code or "all",
            parameter
        )
        
        if limit:
            cache_key += f":limit:{limit}"
        
        cached = await self.cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Cache hit for locations: {cache_key}")
            return cached
        
        result = await self.data_source.find_locations(country_code, parameter, limit)
        await self.cache.set(cache_key, result)
        
        return result

    async def get_sensors(self, location):
        cache_key = self.key_builder.build_sensor_key(location.id)
        
        cached = await self.cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Cache hit for sensors: {cache_key}")
            return cached
        
        result = await self.data_source.get_sensors(location)
        await self.cache.set(cache_key, result, ttl=3600)
        
        return result