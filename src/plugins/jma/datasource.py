from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime
from decimal import Decimal
import asyncio
import aiohttp
import logging
from pathlib import Path
import json
import ftplib
import gzip
import struct
from io import BytesIO

from ...domain.interfaces import DataSource
from ...domain.models import Location, Sensor, Measurement, Coordinates, ParameterType, MeasurementUnit
from ...domain.exceptions import DataSourceError, APIError
# from ...infrastructure.retry import RetryPolicy, execute_with_retry
from ...infrastructure.cache import Cache
from ...infrastructure.metrics import MetricsCollector
from ...core.api_client import RateLimitedAPIClient


logger = logging.getLogger(__name__)


class JMADataSource(DataSource):
    def __init__(
        self,
        api_client: Optional[RateLimitedAPIClient] = None,
        cache: Optional[Cache] = None,
        metrics: Optional[MetricsCollector] = None,
        retry_policy: Optional[Any] = None,
        base_url: str = "https://www.jma.go.jp/bosai",
        jra_ftp_host: str = "ftp.rda.ucar.edu",
        amedas_api_url: str = "https://www.jma.go.jp/bosai/amedas/data/latest_time.txt"
    ):
        self.base_url = base_url
        self.api_client = api_client or RateLimitedAPIClient(base_url=self.base_url)
        self.cache = cache
        self.metrics = metrics
        # self.retry_policy = retry_policy or RetryPolicy()
        self.jra_ftp_host = jra_ftp_host
        self.amedas_api_url = amedas_api_url
        self._session: Optional[aiohttp.ClientSession] = None
        
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
        
    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            
    async def get_locations(
        self, 
        country: Optional[str] = None,
        limit: Optional[int] = None,
        **filters: Any
    ) -> List[Location]:
        if country and country != "JP":
            return []
            
        locations = []
        
        try:
            session = await self._get_session()
            
            amedas_stations_url = f"{self.base_url}/amedas/const/amedastable.json"
            async with session.get(amedas_stations_url) as response:
                if response.status != 200:
                    raise APIError(f"Failed to fetch AMeDAS stations: {response.status}")
                    
                stations_data = await response.json()
                
            for station_id, station_info in stations_data.items():
                if limit and len(locations) >= limit:
                    break
                    
                location = Location(
                    id=f"JMA_AMEDAS_{station_id}",
                    name=station_info.get('kjName', station_info.get('enName', f'Station {station_id}')),
                    coordinates=Coordinates(
                        latitude=Decimal(str(station_info['lat'][0] + station_info['lat'][1] / 60)),
                        longitude=Decimal(str(station_info['lon'][0] + station_info['lon'][1] / 60))
                    ),
                    city=station_info.get('kjName', ''),
                    country="JP",
                    metadata={
                        'station_id': station_id,
                        'type': station_info.get('type', 'amedas'),
                        'elevation': station_info.get('alt', 0),
                        'region': station_info.get('regionKjName', ''),
                        'data_source': 'amedas'
                    }
                )
                locations.append(location)
                
            jra_stations = await self._get_jra_stations()
            for jra_station in jra_stations[:limit - len(locations)] if limit else jra_stations:
                locations.append(jra_station)
                
        except Exception as e:
            logger.error(f"Error fetching JMA locations: {e}")
            raise DataSourceError(f"Failed to get JMA locations: {e}")
            
        return locations
        
    async def _get_jra_stations(self) -> List[Location]:
        stations = []
        
        for lat in range(0, 60, 5):
            for lon in range(120, 150, 5):
                location = Location(
                    id=f"JMA_JRA55_{lat}N_{lon}E",
                    name=f"JRA-55 Grid {lat}°N {lon}°E",
                    coordinates=Coordinates(
                        latitude=Decimal(str(lat)),
                        longitude=Decimal(str(lon))
                    ),
                    city="",
                    country="JP",
                    metadata={
                        'type': 'reanalysis',
                        'grid_resolution': '1.25x1.25',
                        'data_source': 'jra55',
                        'levels': ['surface', '850hPa', '700hPa', '500hPa', '300hPa', '200hPa', '100hPa']
                    }
                )
                stations.append(location)
                
        return stations
        
    async def get_sensors(
        self,
        location: Location,
        parameters: Optional[List[ParameterType]] = None,
        **filters: Any
    ) -> List[Sensor]:
        sensors = []
        
        weather_params = {
            ParameterType.TEMPERATURE: ('temp', MeasurementUnit.CELSIUS),
            ParameterType.HUMIDITY: ('humidity', MeasurementUnit.PERCENT),
            ParameterType.PRESSURE: ('pressure', MeasurementUnit.HECTOPASCALS),
            ParameterType.WINDSPEED: ('windSpeed', MeasurementUnit.METERS_PER_SECOND),
            ParameterType.WINDDIRECTION: ('windDirection', MeasurementUnit.DEGREES),
            ParameterType.PRECIPITATION: ('precipitation10m', MeasurementUnit.MILLIMETERS),
            ParameterType.SOLAR_RADIATION: ('sun10m', MeasurementUnit.MINUTES),
            ParameterType.VISIBILITY: ('visibility', MeasurementUnit.METERS),
            ParameterType.CLOUD_COVER: ('cloud', MeasurementUnit.OKTAS),
            ParameterType.DEW_POINT: ('dew', MeasurementUnit.CELSIUS)
        }
        
        if location.metadata.get('data_source') == 'jra55':
            levels = location.metadata.get('levels', ['surface'])
            for level in levels:
                for param_type, (param_code, unit) in weather_params.items():
                    if parameters and param_type not in parameters:
                        continue
                        
                    sensor = Sensor(
                        id=f"{location.id}_{param_code}_{level}",
                        location=location,
                        parameter=param_type,
                        unit=unit,
                        is_active=True,
                        metadata={
                            'parameter_code': param_code,
                            'level': level,
                            'data_source': 'jra55'
                        }
                    )
                    sensors.append(sensor)
        else:
            for param_type, (param_code, unit) in weather_params.items():
                if parameters and param_type not in parameters:
                    continue
                    
                sensor = Sensor(
                    id=f"{location.id}_{param_code}",
                    location=location,
                    parameter=param_type,
                    unit=unit,
                    is_active=True,
                    metadata={
                        'parameter_code': param_code,
                        'data_source': 'amedas'
                    }
                )
                sensors.append(sensor)
                
        return sensors
        
    async def get_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        **filters: Any
    ) -> AsyncIterator[List[Measurement]]:
        if sensor.metadata.get('data_source') == 'jra55':
            async for measurements in self._get_jra_measurements(sensor, start_date, end_date, limit):
                yield measurements
        else:
            async for measurements in self._get_amedas_measurements(sensor, start_date, end_date, limit):
                yield measurements
                
    async def _get_amedas_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[List[Measurement]]:
        try:
            session = await self._get_session()
            station_id = sensor.location.metadata['station_id']
            param_code = sensor.metadata['parameter_code']
            
            current_date = start_date or datetime.now()
            end = end_date or datetime.now()
            
            measurements_count = 0
            
            while current_date <= end and (not limit or measurements_count < limit):
                date_str = current_date.strftime('%Y%m%d')
                hour_str = current_date.strftime('%H')
                
                amedas_url = f"{self.base_url}/amedas/data/point/{station_id}/{date_str}_{hour_str}.json"
                
                try:
                    async with session.get(amedas_url) as response:
                        if response.status == 404:
                            current_date = current_date.replace(hour=current_date.hour + 1)
                            continue
                            
                        if response.status != 200:
                            logger.warning(f"Failed to fetch AMeDAS data: {response.status}")
                            current_date = current_date.replace(hour=current_date.hour + 1)
                            continue
                            
                        data = await response.json()
                        
                    measurements = []
                    for time_str, values in data.items():
                        if param_code in values and values[param_code] is not None:
                            timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")
                            
                            measurement = Measurement(
                                sensor=sensor,
                                timestamp=timestamp,
                                value=Decimal(str(values[param_code])),
                                quality_flag="good" if values.get(f"{param_code}Quality", 0) == 0 else "suspect",
                                metadata={
                                    'data_source': 'amedas',
                                    'observation_type': 'surface'
                                }
                            )
                            measurements.append(measurement)
                            measurements_count += 1
                            
                            if limit and measurements_count >= limit:
                                break
                                
                    if measurements:
                        yield measurements
                        
                except Exception as e:
                    logger.error(f"Error fetching AMeDAS data for {current_date}: {e}")
                    
                current_date = current_date.replace(hour=current_date.hour + 1)
                
        except Exception as e:
            logger.error(f"Error in AMeDAS measurements: {e}")
            raise DataSourceError(f"Failed to get AMeDAS measurements: {e}")
            
    async def _get_jra_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[List[Measurement]]:
        logger.info(f"JRA-55 data access requires NCAR RDA account and manual download")
        logger.info(f"Visit: https://rda.ucar.edu/datasets/ds628.0/")
        logger.info(f"Grid point: {sensor.location.name}")
        logger.info(f"Parameter: {sensor.metadata['parameter_code']} at {sensor.metadata['level']}")
        
        demo_measurements = []
        if start_date:
            for i in range(min(10, limit or 10)):
                timestamp = start_date.replace(hour=i*6)
                measurement = Measurement(
                    sensor=sensor,
                    timestamp=timestamp,
                    value=Decimal("15.5") + Decimal(str(i * 0.1)),
                    quality_flag="demo",
                    metadata={
                        'data_source': 'jra55',
                        'note': 'Demo data - real JRA-55 requires manual download'
                    }
                )
                demo_measurements.append(measurement)
                
        if demo_measurements:
            yield demo_measurements
            
    async def list_countries(self) -> List[Dict[str, str]]:
        return [
            {'code': 'JP', 'name': 'Japan'},
        ]
    
    async def find_locations(
        self, 
        country_code: Optional[str] = None,
        parameter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Location]:
        return await self.get_locations(country=country_code, limit=limit)
    
    async def stream_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> AsyncIterator[Measurement]:
        async for measurements in self.get_measurements(sensor, start_date, end_date):
            for measurement in measurements:
                yield measurement
    
    async def get_metadata(self) -> Dict[str, Any]:
        return {
            'name': 'JMA',
            'description': 'Japan Meteorological Agency - AMeDAS stations and JRA-55 reanalysis',
            'resolution': 'Station data and 1.25x1.25 degrees reanalysis',
            'temporal': 'Real-time AMeDAS, 6-hourly JRA-55',
            'api_required': False,
            'coverage': 'Japan'
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        required_fields = ['timestamp', 'value']
        return all(field in data for field in required_fields)
        
    async def health_check(self) -> Dict[str, Any]:
        try:
            session = await self._get_session()
            async with session.get(self.amedas_api_url) as response:
                amedas_ok = response.status == 200
                
            return {
                'status': 'healthy' if amedas_ok else 'degraded',
                'amedas_api': 'available' if amedas_ok else 'unavailable',
                'jra55': 'requires_manual_download',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }