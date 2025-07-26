import pytest
import asyncio
import time
from src.infrastructure.retry import (
    retry, ExponentialBackoff, LinearBackoff, JitteredBackoff,
    CircuitBreaker
)
from src.domain.exceptions import RateLimitException, NetworkException


class TestRetryStrategies:
    def test_exponential_backoff(self):
        strategy = ExponentialBackoff(base_delay=1.0, max_delay=10.0, factor=2.0)
        
        assert strategy.calculate_delay(1) == 1.0
        assert strategy.calculate_delay(2) == 2.0
        assert strategy.calculate_delay(3) == 4.0
        assert strategy.calculate_delay(4) == 8.0
        assert strategy.calculate_delay(5) == 10.0

    def test_linear_backoff(self):
        strategy = LinearBackoff(base_delay=1.0, increment=2.0, max_delay=10.0)
        
        assert strategy.calculate_delay(1) == 1.0
        assert strategy.calculate_delay(2) == 3.0
        assert strategy.calculate_delay(3) == 5.0
        assert strategy.calculate_delay(5) == 9.0
        assert strategy.calculate_delay(10) == 10.0

    def test_jittered_backoff(self):
        base_strategy = ExponentialBackoff(base_delay=1.0)
        strategy = JitteredBackoff(base_strategy, jitter_range=0.1)
        
        delays = [strategy.calculate_delay(2) for _ in range(10)]
        
        assert all(1.8 <= d <= 2.2 for d in delays)
        assert len(set(delays)) > 1


class TestRetryDecorator:
    @pytest.mark.asyncio
    async def test_async_retry_success_on_second_attempt(self):
        attempt_count = 0
        
        @retry(max_attempts=3, retry_on=(ValueError,))
        async def flaky_function():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise ValueError("Temporary error")
            return "success"
        
        result = await flaky_function()
        assert result == "success"
        assert attempt_count == 2

    @pytest.mark.asyncio
    async def test_async_retry_exhaustion(self):
        attempt_count = 0
        
        @retry(max_attempts=3, retry_on=(ValueError,))
        async def always_fails():
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Permanent error")
        
        with pytest.raises(ValueError, match="Permanent error"):
            await always_fails()
        
        assert attempt_count == 3

    def test_sync_retry_success(self):
        attempt_count = 0
        
        @retry(max_attempts=3, retry_on=(ValueError,))
        def flaky_function():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise ValueError("Temporary error")
            return "success"
        
        result = flaky_function()
        assert result == "success"
        assert attempt_count == 2

    @pytest.mark.asyncio
    async def test_rate_limit_retry_after(self):
        @retry(max_attempts=2, retry_on=(RateLimitException,))
        async def rate_limited():
            raise RateLimitException("Rate limited", retry_after=1)
        
        start_time = time.time()
        
        with pytest.raises(RateLimitException):
            await rate_limited()
        
        elapsed = time.time() - start_time
        assert 0.9 < elapsed < 1.5

    @pytest.mark.asyncio
    async def test_on_retry_callback(self):
        retry_log = []
        
        def log_retry(exc, attempt):
            retry_log.append((type(exc).__name__, attempt))
        
        @retry(
            max_attempts=3,
            retry_on=(ValueError,),
            on_retry=log_retry
        )
        async def flaky_function():
            raise ValueError("Error")
        
        with pytest.raises(ValueError):
            await flaky_function()
        
        assert retry_log == [("ValueError", 1), ("ValueError", 2)]


class TestCircuitBreaker:
    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self):
        breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=0.5,
            expected_exception=ValueError
        )
        
        @breaker
        async def failing_function():
            raise ValueError("Service error")
        
        for i in range(3):
            with pytest.raises(ValueError):
                await failing_function()
        
        with pytest.raises(Exception, match="Circuit breaker is open"):
            await failing_function()

    @pytest.mark.asyncio
    async def test_circuit_breaker_recovery(self):
        breaker = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.1,
            expected_exception=ValueError
        )
        
        call_count = 0
        
        @breaker
        async def sometimes_fails():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ValueError("Error")
            return "success"
        
        for i in range(2):
            with pytest.raises(ValueError):
                await sometimes_fails()
        
        await asyncio.sleep(0.15)
        
        result = await sometimes_fails()
        assert result == "success"
        assert breaker.state == "closed"