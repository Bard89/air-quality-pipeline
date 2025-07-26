import pytest
import asyncio
import time
from src.infrastructure.cache import MemoryCache, KeyBuilder


class TestMemoryCache:
    @pytest.mark.asyncio
    async def test_basic_get_set(self):
        cache = MemoryCache(max_size=10)
        
        await cache.set("key1", "value1")
        result = await cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_cache_miss(self):
        cache = MemoryCache()
        result = await cache.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_ttl_expiration(self):
        cache = MemoryCache(default_ttl=1)
        
        await cache.set("key1", "value1", ttl=1)
        assert await cache.get("key1") == "value1"
        
        await asyncio.sleep(1.1)
        assert await cache.get("key1") is None

    @pytest.mark.asyncio
    async def test_max_size_eviction(self):
        cache = MemoryCache(max_size=3)
        
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        await cache.set("key3", "value3")
        await cache.set("key4", "value4")
        
        assert await cache.get("key1") is None
        assert await cache.get("key4") == "value4"

    @pytest.mark.asyncio
    async def test_lru_behavior(self):
        cache = MemoryCache(max_size=3)
        
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        await cache.set("key3", "value3")
        
        await cache.get("key1")
        
        await cache.set("key4", "value4")
        
        assert await cache.get("key2") is None
        assert await cache.get("key1") == "value1"

    @pytest.mark.asyncio
    async def test_cache_stats(self):
        cache = MemoryCache()
        
        await cache.set("key1", "value1")
        await cache.get("key1")
        await cache.get("key2")
        
        stats = cache.get_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.5

    @pytest.mark.asyncio
    async def test_clear_cache(self):
        cache = MemoryCache()
        
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        await cache.clear()
        
        assert await cache.get("key1") is None
        assert await cache.get("key2") is None


class TestKeyBuilder:
    def test_location_key(self):
        key = KeyBuilder.build_location_key("US", "pm25")
        assert key == "locations:US:pm25"

    def test_sensor_key(self):
        key = KeyBuilder.build_sensor_key("loc123")
        assert key == "sensors:loc123"

    def test_measurement_key(self):
        key = KeyBuilder.build_measurement_key("sensor456", page=5)
        assert key == "measurements:sensor456:page:5"

    def test_hash_key_consistency(self):
        data = {"a": 1, "b": 2, "c": 3}
        key1 = KeyBuilder.build_hash_key(data)
        key2 = KeyBuilder.build_hash_key({"c": 3, "a": 1, "b": 2})
        assert key1 == key2