from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime, timedelta
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
            ParameterType.WIND_SPEED: ('windSpeed', MeasurementUnit.METERS_PER_SECOND),
            ParameterType.WIND_DIRECTION: ('windDirection', MeasurementUnit.DEGREES),
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
            
            # Map our parameter codes to AMeDAS field names
            param_mapping = {
                'temp': 'temp',
                'humidity': 'humidity',
                'pressure': 'pressure',
                'windSpeed': 'wind',
                'windDirection': 'windDirection',
                'precipitation10m': 'precipitation10m',
                'sun10m': 'sun10m',
                'visibility': 'visibility',
                'cloud': None,  # Not available in AMeDAS
                'dew': None  # Not available in AMeDAS
            }
            
            amedas_field = param_mapping.get(param_code)
            if amedas_field is None:
                logger.warning(f"Parameter {param_code} not available in AMeDAS data")
                return
            
            current_date = start_date or datetime.now()
            end = end_date or datetime.now()
            
            # JMA AMeDAS only provides recent data (last 48-72 hours)
            max_history = datetime.now() - timedelta(days=3)
            if current_date < max_history:
                logger.warning(f"JMA AMeDAS only provides data from {max_history.strftime('%Y-%m-%d')} onwards. Requested date {current_date.strftime('%Y-%m-%d')} is too old.")
                return
            
            measurements_count = 0
            
            # AMeDAS provides data in 10-minute intervals
            while current_date <= end and (not limit or measurements_count < limit):
                # Round to nearest 10 minutes
                minutes = (current_date.minute // 10) * 10
                timestamp_str = current_date.replace(minute=minutes, second=0, microsecond=0).strftime('%Y%m%d%H%M%S')
                
                amedas_url = f"{self.base_url}/amedas/data/map/{timestamp_str}.json"
                
                try:
                    async with session.get(amedas_url) as response:
                        if response.status == 404:
                            current_date = current_date + timedelta(minutes=10)
                            continue
                            
                        if response.status != 200:
                            logger.warning(f"Failed to fetch AMeDAS data: {response.status}")
                            current_date = current_date + timedelta(minutes=10)
                            continue
                            
                        data = await response.json()
                        
                    if station_id in data:
                        station_data = data[station_id]
                        if amedas_field in station_data:
                            value_data = station_data[amedas_field]
                            if isinstance(value_data, list) and len(value_data) >= 1:
                                value = value_data[0]
                                quality = value_data[1] if len(value_data) > 1 else 0
                                
                                measurement = Measurement(
                                    sensor=sensor,
                                    timestamp=current_date.replace(minute=minutes, second=0, microsecond=0),
                                    value=Decimal(str(value)),
                                    quality_flag="good" if quality == 0 else "suspect",
                                    metadata={
                                        'data_source': 'amedas',
                                        'observation_type': 'surface'
                                    }
                                )
                                
                                yield [measurement]
                                measurements_count += 1
                                
                except Exception as e:
                    logger.error(f"Error fetching AMeDAS data for {current_date}: {e}")
                    
                current_date = current_date + timedelta(minutes=10)
                
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
            'description': 'Japan Meteorological Agency - AMeDAS stations (recent data only) and JRA-55 reanalysis',
            'resolution': 'Station data (1,286 stations) and 1.25x1.25 degrees reanalysis',
            'temporal': 'AMeDAS: 10-minute intervals (last 48-72 hours only), JRA-55: 6-hourly',
            'api_required': False,
            'coverage': 'Japan',
            'limitations': 'AMeDAS provides only recent data (2-3 days). For historical data use Open-Meteo or NASA POWER.'
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