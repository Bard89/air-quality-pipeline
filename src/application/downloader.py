import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import uuid
from ..domain.interfaces import DataSource, Storage, JobManager, MetricsCollector
from ..domain.models import DownloadJob, Location, ParameterType
from ..infrastructure.logging import get_logger, LogContext
from ..infrastructure.retry import retry
from ..infrastructure.metrics import MetricsMiddleware
from ..domain.exceptions import DataSourceException


logger = get_logger(__name__)


class AirQualityDownloader:
    def __init__(
        self,
        data_source: DataSource,
        storage: Storage,
        job_manager: JobManager,
        metrics: MetricsCollector,
        max_concurrent_locations: int = 5,
        max_concurrent_sensors: int = 10
    ):
        self.data_source = data_source
        self.storage = storage
        self.job_manager = job_manager
        self.metrics = metrics
        self.max_concurrent_locations = max_concurrent_locations
        self.max_concurrent_sensors = max_concurrent_sensors
        self.metrics_middleware = MetricsMiddleware(metrics)

    async def download_country(
        self,
        country_code: str,
        parameters: Optional[List[ParameterType]] = None,
        max_locations: Optional[int] = None,
        resume: bool = True
    ) -> str:
        job = DownloadJob(
            id=str(uuid.uuid4()),
            source=await self._get_source_name(),
            country_code=country_code,
            parameters=parameters or [],
            max_locations=max_locations
        )
        
        job_id = await self.job_manager.create_job(job)
        
        with LogContext(job_id=job_id, country=country_code):
            try:
                await self._execute_job(job, resume)
                await self.job_manager.update_job_status(job_id, "completed")
                return job_id
            except Exception as e:
                logger.error(f"Job {job_id} failed", exc_info=True)
                await self.job_manager.update_job_status(
                    job_id, 
                    "failed",
                    {"error": str(e)}
                )
                raise

    async def _execute_job(self, job: DownloadJob, resume: bool) -> None:
        checkpoint = None
        if resume:
            checkpoint = await self.storage.get_checkpoint(job.id)
            if checkpoint:
                logger.info(f"Resuming from checkpoint: location {checkpoint['location_index']}")

        locations = await self._find_locations(job)
        start_index = checkpoint['location_index'] if checkpoint else 0
        completed_ids = set(checkpoint.get('completed_locations', [])) if checkpoint else set()

        location_semaphore = asyncio.Semaphore(self.max_concurrent_locations)
        
        tasks = []
        for i, location in enumerate(locations[start_index:], start=start_index):
            if location.id in completed_ids:
                continue
            
            task = asyncio.create_task(
                self._download_location_with_limit(
                    location, job, i, len(locations), location_semaphore, completed_ids
                )
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        failed_count = sum(1 for r in results if isinstance(r, Exception))
        if failed_count > 0:
            logger.warning(f"Failed to download {failed_count} locations")

    @retry(max_attempts=3, retry_on=(DataSourceException,))
    async def _find_locations(self, job: DownloadJob) -> List[Location]:
        logger.info(f"Finding locations for country {job.country_code}")
        
        locations = await self.data_source.find_locations(
            country_code=job.country_code,
            limit=job.max_locations
        )
        
        logger.info(f"Found {len(locations)} locations")
        self.metrics.record_gauge(
            "locations_found",
            len(locations),
            {"country": job.country_code}
        )
        
        return locations

    async def _download_location_with_limit(
        self,
        location: Location,
        job: DownloadJob,
        index: int,
        total: int,
        semaphore: asyncio.Semaphore,
        completed_ids: set
    ) -> None:
        async with semaphore:
            await self._download_location(location, job, index, total, completed_ids)

    @MetricsMiddleware.track_download("location")
    async def _download_location(
        self,
        location: Location,
        job: DownloadJob,
        index: int,
        total: int,
        completed_ids: set
    ) -> None:
        logger.info(
            f"Downloading location {index + 1}/{total}: {location.name} ({location.id})"
        )
        
        try:
            sensors = await self.data_source.get_sensors(location)
            
            if job.parameters:
                sensors = [s for s in sensors if s.parameter in job.parameters]
            
            if not sensors:
                logger.warning(f"No sensors found for location {location.name}")
                return
            
            sensor_semaphore = asyncio.Semaphore(self.max_concurrent_sensors)
            tasks = []
            
            for sensor in sensors:
                task = asyncio.create_task(
                    self._download_sensor_with_limit(sensor, sensor_semaphore)
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update completed locations list
            completed_ids.add(location.id)
            
            checkpoint = {
                "location_index": index + 1,
                "completed_locations": list(completed_ids),
                "total_locations": total,
                "country_code": job.country_code,
                "current_location_id": location.id
            }
            await self.storage.save_checkpoint(job.id, checkpoint)
            
            self.metrics.increment_counter(
                "locations_completed",
                tags={"country": job.country_code}
            )
            
        except Exception as e:
            logger.error(f"Failed to download location {location.id}: {e}")
            self.metrics.increment_counter(
                "location_errors",
                tags={"country": job.country_code, "error_type": type(e).__name__}
            )
            raise

    async def _download_sensor_with_limit(
        self,
        sensor: Any,
        semaphore: asyncio.Semaphore
    ) -> None:
        async with semaphore:
            await self._download_sensor(sensor)

    @retry(max_attempts=3, retry_on=(DataSourceException,))
    async def _download_sensor(self, sensor: Any) -> None:
        measurement_count = 0
        
        try:
            async for measurement in self.data_source.stream_measurements(sensor):
                await self.storage.save_measurement(measurement)
                measurement_count += 1
                
                if measurement_count % 1000 == 0:
                    logger.debug(f"Downloaded {measurement_count} measurements for sensor {sensor.id}")
            
            self.metrics.record_histogram(
                "sensor_measurements",
                measurement_count,
                tags={"parameter": sensor.parameter.value}
            )
            
        except Exception as e:
            logger.error(f"Failed to download sensor {sensor.id}: {e}")
            raise

    async def _get_source_name(self) -> str:
        metadata = await self.data_source.get_metadata()
        return metadata.get("name", "unknown")