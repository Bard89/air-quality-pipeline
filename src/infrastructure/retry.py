import asyncio
import time
from typing import Callable, Optional, Type, Tuple, Union, Any
from functools import wraps
import random
import logging
from abc import ABC, abstractmethod
from ..domain.exceptions import RateLimitException, NetworkException, DataSourceException


logger = logging.getLogger(__name__)


class RetryStrategy(ABC):
    @abstractmethod
    def calculate_delay(self, attempt: int) -> float:
        pass


class ExponentialBackoff(RetryStrategy):
    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0, factor: float = 2.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.factor = factor

    def calculate_delay(self, attempt: int) -> float:
        delay = self.base_delay * (self.factor ** (attempt - 1))
        return min(delay, self.max_delay)


class LinearBackoff(RetryStrategy):
    def __init__(self, base_delay: float = 1.0, increment: float = 1.0, max_delay: float = 60.0):
        self.base_delay = base_delay
        self.increment = increment
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int) -> float:
        delay = self.base_delay + (self.increment * (attempt - 1))
        return min(delay, self.max_delay)


class JitteredBackoff(RetryStrategy):
    def __init__(self, strategy: RetryStrategy, jitter_range: float = 0.1):
        self.strategy = strategy
        self.jitter_range = jitter_range

    def calculate_delay(self, attempt: int) -> float:
        base_delay = self.strategy.calculate_delay(attempt)
        jitter = base_delay * self.jitter_range * (2 * random.random() - 1)
        return max(0, base_delay + jitter)


def retry(
    max_attempts: int = 3,
    retry_on: Tuple[Type[Exception], ...] = (Exception,),
    strategy: Optional[RetryStrategy] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    if strategy is None:
        strategy = JitteredBackoff(ExponentialBackoff())

    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                for attempt in range(1, max_attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except retry_on as e:
                        if isinstance(e, RateLimitException) and e.retry_after:
                            delay = e.retry_after
                        else:
                            delay = strategy.calculate_delay(attempt)
                        
                        if attempt < max_attempts:
                            logger.warning(
                                f"Retry {attempt}/{max_attempts} for {func.__name__} "
                                f"after {delay:.1f}s due to: {str(e)}"
                            )
                            
                            if on_retry:
                                on_retry(e, attempt)
                            
                            await asyncio.sleep(delay)
                        else:
                            logger.error(f"Max retries reached for {func.__name__}")
                            raise
            
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                for attempt in range(1, max_attempts + 1):
                    try:
                        return func(*args, **kwargs)
                    except retry_on as e:
                        if isinstance(e, RateLimitException) and e.retry_after:
                            delay = e.retry_after
                        else:
                            delay = strategy.calculate_delay(attempt)
                        
                        if attempt < max_attempts:
                            logger.warning(
                                f"Retry {attempt}/{max_attempts} for {func.__name__} "
                                f"after {delay:.1f}s due to: {str(e)}"
                            )
                            
                            if on_retry:
                                on_retry(e, attempt)
                            
                            time.sleep(delay)
                        else:
                            logger.error(f"Max retries reached for {func.__name__}")
                            raise
            
            return sync_wrapper
    
    return decorator


class CircuitBreaker:
    """
    Circuit breaker pattern implementation for async functions only.
    Prevents cascading failures by opening after threshold failures.
    """
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "closed"
        self._lock = asyncio.Lock()

    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with self._lock:
                if self.state == "open":
                    if time.time() - self.last_failure_time > self.recovery_timeout:
                        self.state = "half-open"
                    else:
                        raise DataSourceException("Circuit breaker is open")

            try:
                result = await func(*args, **kwargs)
                async with self._lock:
                    if self.state == "half-open":
                        self.state = "closed"
                        self.failure_count = 0
                return result
            except self.expected_exception as e:
                async with self._lock:
                    self.failure_count += 1
                    self.last_failure_time = time.time()
                    
                    if self.failure_count >= self.failure_threshold:
                        self.state = "open"
                        logger.error(f"Circuit breaker opened for {func.__name__}")
                
                raise

        return wrapper