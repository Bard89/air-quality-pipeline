import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

from ...domain.interfaces import DataSource
from ...domain.models import (
    Coordinates,
    Location,
    Measurement,
    MeasurementUnit,
    ParameterType,
    Sensor,
)
from .archive_downloader import JARTICArchiveDownloader
from .data_parser import JARTICDataParser


logger = logging.getLogger(__name__)


class JARTICDataSource(DataSource):
    def __init__(
        self,
        api_keys: List[str],
        base_url: str = "http://storage.compusophia.com:1475/traffic",
        rate_limit_per_key: int = 60,
        timeout: int = 3600,
        cache_dir: Optional[Path] = None,
        cleanup_after_parse: bool = True
    ):
        if not base_url or not base_url.startswith(("http://", "https://")):
            raise ValueError("Base URL must be a valid HTTP(S) URL")
        if timeout <= 0:
            raise ValueError("Timeout must be positive")
        self.api_keys = api_keys
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.cache_dir = Path(cache_dir) if cache_dir else Path("data/jartic/cache")
        self.cleanup_after_parse = cleanup_after_parse
        self.downloader = JARTICArchiveDownloader(
            base_url=self.base_url,
            cache_dir=self.cache_dir,
            timeout=self.timeout
        )
        self.parser = JARTICDataParser()
        self._location_cache: Dict[str, Location] = {}
        self._sensor_cache: Dict[str, List[Sensor]] = {}

    async def __aenter__(self):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.downloader.close()
        if self.cleanup_after_parse:
            logger.info("Cleaning up cached archive files")
            for file in self.cache_dir.glob("*.zip"):
                try:
                    file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to cleanup {file}: {e}")

    async def list_countries(self) -> List[Dict[str, str]]:
        return [{
            "code": "JP",
            "name": "Japan",
            "locations": 2600,
            "sensors": 2600
        }]

    async def find_locations(
        self,
        country_code: Optional[str] = None,
        parameter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Location]:
        if country_code and country_code.upper() != "JP":
            return []
            
        if self._location_cache:
            locations = list(self._location_cache.values())
        else:
            locations = self._generate_placeholder_locations(limit)
            self._location_cache.clear()
            for loc in locations:
                self._location_cache[loc.id] = loc
                
        if limit:
            locations = locations[:limit]

        return locations

    async def get_sensors(self, location: Location) -> List[Sensor]:
        if location.id in self._sensor_cache:
            return self._sensor_cache[location.id]

        sensors = []

        sensor_configs = [
            (
                "_traffic_volume",
                ParameterType.TRAFFIC_VOLUME,
                MeasurementUnit.VEHICLES_PER_HOUR,
                {"interval": "hourly", "sensor_type": "traffic_counter"},
                True
            ),
            (
                "_traffic_volume_5min",
                ParameterType.TRAFFIC_VOLUME,
                MeasurementUnit.VEHICLES_PER_5MIN,
                {"interval": "5min", "sensor_type": "traffic_counter"},
                location.metadata.get('has_5min_data', False)
            ),
            (
                "_vehicle_speed",
                ParameterType.VEHICLE_SPEED,
                MeasurementUnit.KILOMETERS_PER_HOUR,
                {"sensor_type": "speed_detector"},
                location.metadata.get('has_speed_data', False)
            ),
            (
                "_occupancy_rate",
                ParameterType.OCCUPANCY_RATE,
                MeasurementUnit.PERCENT_OCCUPANCY,
                {"sensor_type": "occupancy_detector"},
                location.metadata.get('has_occupancy_data', False)
            )
        ]

        for suffix, param, unit, metadata, should_create in sensor_configs:
            if should_create:
                sensors.append(
                    Sensor(
                        id=f"{location.id}{suffix}",
                        location=location,
                        parameter=param,
                        unit=unit,
                        is_active=True,
                        metadata={**metadata, "source": "JARTIC"}
                    )
                )

        self._sensor_cache[location.id] = sensors
        return sensors

    async def stream_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> AsyncIterator[Measurement]:
        if start_date is None:
            start_date = datetime(2023, 1, 1)
        if end_date is None:
            end_date = datetime.now()

        archives_to_process = await self._get_archives_in_range(start_date, end_date)

        for archive_info in archives_to_process:
            try:
                archive_path = await self.downloader.download_archive(
                    archive_info['year'],
                    archive_info['month']
                )

                async for measurement in self.parser.parse_measurements(
                    archive_path,
                    sensor,
                    start_date,
                    end_date
                ):
                    yield measurement

            except asyncio.TimeoutError:
                logger.error(
                    f"Download timeout for archive {archive_info['year']}-{archive_info['month']:02d}. "
                    f"The file is very large. Try again to resume from where it left off."
                )
                logger.info("Download timeout. Run the command again to resume downloading.")
                continue
            except Exception as e:
                logger.error(
                    f"Failed to process archive {archive_info['year']}-{archive_info['month']:02d}: {e}"
                )
                logger.error(f"Archive processing error: {e}")
                continue

    async def get_metadata(self) -> Dict[str, Any]:
        return {
            "name": "JARTIC Archive",
            "version": "v1",
            "base_url": self.base_url,
            "data_source": "Japan Road Traffic Information Center",
            "coverage": "Japan nationwide",
            "observation_points": 2600,
            "update_frequency": "monthly archives",
            "historical_range": "2023-present"
        }

    async def _get_archives_in_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        archive_index = await self.downloader.get_archive_index()

        archives_in_range = []
        for archive in archive_index:
            archive_year = archive['year']
            archive_month = archive['month']

            if (archive_year > start_date.year or
                (archive_year == start_date.year and archive_month >= start_date.month)):
                if (archive_year < end_date.year or
                    (archive_year == end_date.year and archive_month <= end_date.month)):
                    archives_in_range.append(archive)

        return sorted(
            archives_in_range,
            key=lambda x: (x['year'], x['month'])
        )

    def _generate_placeholder_locations(self, limit: Optional[int] = None) -> List[Location]:
        locations = []

        major_cities = [
            ("JARTIC_001", "Tokyo Station Area", "35.6812", "139.7671", "Tokyo", "Kanto", "tokyo"),
            ("JARTIC_002", "Osaka Station Area", "34.7025", "135.4959", "Osaka", "Kansai", "osaka"),
            ("JARTIC_003", "Nagoya Station Area", "35.1709", "136.8815", "Nagoya", "Chubu", "aichi"),
        ]

        for loc_id, name, lat, lon, city, region, prefecture in major_cities:
            locations.append(
                Location(
                    id=loc_id,
                    name=name,
                    coordinates=Coordinates(
                        latitude=Decimal(lat),
                        longitude=Decimal(lon)
                    ),
                    city=city,
                    country="JP",
                    provider="JARTIC",
                    metadata={"region": region, "prefecture": prefecture, "has_5min_data": True}
                )
            )

        max_locations = min(2600, limit or 100)
        for i in range(4, max_locations + 1):
            locations.append(
                Location(
                    id=f"JARTIC_{i:04d}",
                    name=f"Traffic Monitoring Point {i}",
                    coordinates=Coordinates(
                        latitude=Decimal(str(30 + (i % 16) * 0.5)),
                        longitude=Decimal(str(130 + (i % 20) * 0.5))
                    ),
                    city="Unknown",
                    country="JP",
                    provider="JARTIC",
                    metadata={"placeholder": True}
                )
            )

        return locations