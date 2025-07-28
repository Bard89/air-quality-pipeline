from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio
import aiohttp
import logging
from pathlib import Path
import json

from ...domain.interfaces import DataSource
from ...domain.models import Location, Sensor, Measurement, Coordinates, ParameterType, MeasurementUnit
from ...domain.exceptions import DataSourceError, APIError
# from ...infrastructure.retry import RetryPolicy, execute_with_retry
from ...infrastructure.cache import Cache
from ...infrastructure.metrics import MetricsCollector
from ...core.api_client import RateLimitedAPIClient


logger = logging.getLogger(__name__)


class NASAPowerDataSource(DataSource):
    # NASA POWER uses -999 as a sentinel value for missing/invalid data
    MISSING_DATA_VALUE = -999
    
    def __init__(
        self,
        api_client: Optional[RateLimitedAPIClient] = None,
        cache: Optional[Cache] = None,
        metrics: Optional[MetricsCollector] = None,
        retry_policy: Optional[Any] = None,
        base_url: str = "https://power.larc.nasa.gov/api/temporal"
    ):
        self.base_url = base_url
        self.api_client = api_client or RateLimitedAPIClient(base_url=self.base_url)
        self.cache = cache
        self.metrics = metrics
        # self.retry_policy = retry_policy or RetryPolicy()
        self._session: Optional[aiohttp.ClientSession] = None
        
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
        
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
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
        
        japan_major_cities = [
            {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
            {"name": "Osaka", "lat": 34.6937, "lon": 135.5023},
            {"name": "Nagoya", "lat": 35.1815, "lon": 136.9066},
            {"name": "Sapporo", "lat": 43.0642, "lon": 141.3469},
            {"name": "Fukuoka", "lat": 33.5904, "lon": 130.4017},
            {"name": "Kobe", "lat": 34.6901, "lon": 135.1955},
            {"name": "Kyoto", "lat": 35.0116, "lon": 135.7681},
            {"name": "Yokohama", "lat": 35.4437, "lon": 139.6380},
            {"name": "Sendai", "lat": 38.2682, "lon": 140.8694},
            {"name": "Hiroshima", "lat": 34.3853, "lon": 132.4553},
            {"name": "Niigata", "lat": 37.9161, "lon": 139.0364},
            {"name": "Naha", "lat": 26.2124, "lon": 127.6792},
            {"name": "Kagoshima", "lat": 31.5969, "lon": 130.5571},
            {"name": "Matsuyama", "lat": 33.8395, "lon": 132.7657},
            {"name": "Kanazawa", "lat": 36.5944, "lon": 136.6256}
        ]
        
        grid_resolution = 0.5
        
        for idx, city in enumerate(japan_major_cities):
            if limit and idx >= limit:
                break
                
            location = Location(
                id=f"NASAPOWER_JP_{city['name'].upper()}",
                name=f"NASA POWER - {city['name']}",
                coordinates=Coordinates(
                    latitude=Decimal(str(city['lat'])),
                    longitude=Decimal(str(city['lon']))
                ),
                city=city['name'],
                country="JP",
                metadata={
                    'type': 'satellite_derived',
                    'grid_resolution': '0.5x0.5',
                    'data_source': 'nasapower',
                    'temporal_resolution': 'hourly',
                    'coverage_start': '2001-01-01',
                    'coverage_daily': '1984-01-01',
                    'data_latency': '1-2 days',
                    'validation': 'ground_station_calibrated'
                }
            )
            locations.append(location)
            
        for lat in range(30, 45, 5):
            for lon in range(130, 145, 5):
                if limit and len(locations) >= limit:
                    break
                    
                location = Location(
                    id=f"NASAPOWER_JP_GRID_{lat}N_{lon}E",
                    name=f"NASA POWER Grid {lat}°N {lon}°E",
                    coordinates=Coordinates(
                        latitude=Decimal(str(lat)),
                        longitude=Decimal(str(lon))
                    ),
                    city="",
                    country="JP",
                    metadata={
                        'type': 'grid_point',
                        'grid_resolution': '0.5x0.5',
                        'data_source': 'nasapower',
                        'temporal_resolution': 'hourly',
                        'coverage_start': '2001-01-01'
                    }
                )
                locations.append(location)
            # Break outer loop if limit reached
            if limit and len(locations) >= limit:
                break
                
        return locations
        
    async def get_sensors(
        self,
        location: Location,
        parameters: Optional[List[ParameterType]] = None,
        **filters: Any
    ) -> List[Sensor]:
        sensors = []
        
        power_params = {
            ParameterType.TEMPERATURE: {
                'parameter': 'T2M',
                'unit': MeasurementUnit.CELSIUS,
                'description': 'Temperature at 2 meters'
            },
            ParameterType.HUMIDITY: {
                'parameter': 'RH2M',
                'unit': MeasurementUnit.PERCENT,
                'description': 'Relative humidity at 2 meters'
            },
            ParameterType.PRESSURE: {
                'parameter': 'PS',
                'unit': MeasurementUnit.HECTOPASCALS,
                'description': 'Surface pressure'
            },
            ParameterType.WIND_SPEED: {
                'parameter': 'WS10M',
                'unit': MeasurementUnit.METERS_PER_SECOND,
                'description': 'Wind speed at 10 meters'
            },
            ParameterType.WIND_DIRECTION: {
                'parameter': 'WD10M',
                'unit': MeasurementUnit.DEGREES,
                'description': 'Wind direction at 10 meters'
            },
            ParameterType.PRECIPITATION: {
                'parameter': 'PRECTOTCORR',
                'unit': MeasurementUnit.MILLIMETERS,
                'description': 'Precipitation corrected'
            },
            ParameterType.SOLAR_RADIATION: {
                'parameter': 'ALLSKY_SFC_SW_DWN',
                'unit': MeasurementUnit.WATTS_PER_SQUARE_METER,
                'description': 'All sky surface shortwave downward irradiance'
            },
            ParameterType.CLOUD_COVER: {
                'parameter': 'CLOUD_AMT',
                'unit': MeasurementUnit.PERCENT,
                'description': 'Cloud amount'
            },
            ParameterType.DEW_POINT: {
                'parameter': 'T2MDEW',
                'unit': MeasurementUnit.CELSIUS,
                'description': 'Dew point temperature at 2 meters'
            }
        }
        
        for param_type, param_info in power_params.items():
            if parameters and param_type not in parameters:
                continue
                
            sensor = Sensor(
                id=f"{location.id}_{param_info['parameter']}",
                location=location,
                parameter=param_type,
                unit=param_info['unit'],
                is_active=True,
                metadata={
                    'power_parameter': param_info['parameter'],
                    'description': param_info['description'],
                    'data_source': 'nasapower',
                    'community': 'RE',
                    'temporal_resolution': 'hourly'
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
        try:
            session = await self._get_session()
            
            lat = float(sensor.location.coordinates.latitude)
            lon = float(sensor.location.coordinates.longitude)
            parameter = sensor.metadata['power_parameter']
            
            start = start_date or datetime.now() - timedelta(days=7)
            end = end_date or datetime.now()
            
            current_date = start
            measurements_count = 0
            
            while current_date <= end and (not limit or measurements_count < limit):
                chunk_end = min(current_date + timedelta(days=30), end)
                
                url = f"{self.base_url}/hourly/point"
                params = {
                    'start': current_date.strftime('%Y%m%d'),
                    'end': chunk_end.strftime('%Y%m%d'),
                    'latitude': lat,
                    'longitude': lon,
                    'community': 'RE',
                    'parameters': parameter,
                    'format': 'JSON',
                    'header': 'true'
                }
                
                # logger.debug(f"Requesting NASA POWER data: {params['start']} to {params['end']} for {parameter}")
                
                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=60)) as response:
                        if response.status == 404:
                            logger.warning(f"No data available for {current_date} to {chunk_end}")
                            current_date = chunk_end + timedelta(days=1)
                            continue
                            
                        if response.status != 200:
                            raise APIError(f"NASA POWER API error: {response.status}")
                            
                        data = await response.json()
                        # logger.debug(f"NASA POWER response keys: {list(data.keys())}")
                        
                    param_data = {}
                    if 'properties' in data:
                        properties = data.get('properties', {})
                        param_data = properties.get('parameter', {}).get(parameter, {})
                    elif 'parameters' in data:
                        param_data = data.get('parameters', {}).get(parameter, {})
                    else:
                        logger.warning(f"Unexpected NASA POWER response structure: {list(data.keys())[:5]}")
                        param_data = {}
                        
                    measurements = []
                    for date_str, value in param_data.items():
                        if value is not None and value != self.MISSING_DATA_VALUE:
                            timestamp = datetime.strptime(date_str, "%Y%m%d%H")
                            
                            measurement = Measurement(
                                sensor=sensor,
                                timestamp=timestamp,
                                value=Decimal(str(value)),
                                quality_flag="validated",
                                metadata={
                                    'data_source': 'nasapower',
                                    'data_version': data.get('header', {}).get('data_version', 'unknown') if 'header' in data else 'unknown'
                                }
                            )
                            measurements.append(measurement)
                            measurements_count += 1
                            
                            if limit and measurements_count >= limit:
                                break
                                
                    if measurements:
                        yield measurements
                            
                except Exception as e:
                    logger.error(f"Error fetching NASA POWER data for {current_date}: {e}")
                    
                current_date = chunk_end + timedelta(days=1)
                
        except Exception as e:
            logger.error(f"Error in NASA POWER measurements: {e}")
            raise DataSourceError(f"Failed to get NASA POWER measurements: {e}")
            
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
            'name': 'NASA POWER',
            'description': 'NASA Prediction Of Worldwide Energy Resources',
            'resolution': '0.5x0.5 degrees',
            'temporal': 'Hourly from 2001, Daily from 1984',
            'api_required': False,
            'coverage': 'Global'
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        required_fields = ['timestamp', 'value']
        return all(field in data for field in required_fields)
        
    async def health_check(self) -> Dict[str, Any]:
        try:
            session = await self._get_session()
            
            test_url = f"{self.base_url}/hourly/point"
            test_params = {
                'start': '20240101',
                'end': '20240101',
                'latitude': 35.6762,
                'longitude': 139.6503,
                'community': 'RE',
                'parameters': 'T2M',
                'format': 'JSON'
            }
            
            async with session.get(test_url, params=test_params) as response:
                api_ok = response.status == 200
                
            return {
                'status': 'healthy' if api_ok else 'unhealthy',
                'api': 'available' if api_ok else 'unavailable',
                'note': 'No API key required for NASA POWER',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }