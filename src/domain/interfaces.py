from abc import ABC, abstractmethod
from typing import List, Optional, AsyncIterator, Dict, Any, Tuple
from datetime import datetime
from .models import Location, Sensor, Measurement, DataSourceConfig, DownloadJob


class DataSource(ABC):
    @abstractmethod
    async def list_countries(self) -> List[Dict[str, str]]:
        pass

    @abstractmethod
    async def find_locations(
        self, 
        country_code: Optional[str] = None,
        parameter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Location]:
        pass

    @abstractmethod
    async def get_sensors(self, location: Location) -> List[Sensor]:
        pass

    @abstractmethod
    async def stream_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> AsyncIterator[Measurement]:
        pass

    @abstractmethod
    async def get_metadata(self) -> Dict[str, Any]:
        pass


class Storage(ABC):
    @abstractmethod
    async def save_measurement(self, measurement: Measurement) -> None:
        pass

    @abstractmethod
    async def save_measurements_batch(self, measurements: List[Measurement]) -> int:
        pass

    @abstractmethod
    async def get_checkpoint(self, job_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    async def save_checkpoint(self, job_id: str, checkpoint: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class RateLimiter(ABC):
    @abstractmethod
    async def acquire(self, key: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        pass


class Cache(ABC):
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        pass

    @abstractmethod
    async def clear(self) -> None:
        pass


class JobManager(ABC):
    @abstractmethod
    async def create_job(self, job: DownloadJob) -> str:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> Optional[DownloadJob]:
        pass

    @abstractmethod
    async def update_job_status(self, job_id: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        pass

    @abstractmethod
    async def list_jobs(self, status: Optional[str] = None) -> List[DownloadJob]:
        pass


class MetricsCollector(ABC):
    @abstractmethod
    def increment_counter(self, name: str, value: int = 1, tags: Optional[Dict[str, str]] = None) -> None:
        pass

    @abstractmethod
    def record_gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        pass

    @abstractmethod
    def record_histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        pass

    @abstractmethod
    def flush(self) -> None:
        pass