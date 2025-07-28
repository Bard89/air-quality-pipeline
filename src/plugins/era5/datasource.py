from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio
import aiohttp
import logging
from pathlib import Path
import os
import tempfile
import numpy as np

from ...domain.interfaces import DataSource
from ...domain.models import Location, Sensor, Measurement, Coordinates, ParameterType, MeasurementUnit
from ...domain.exceptions import DataSourceError, APIError
# from ...infrastructure.retry import RetryPolicy, execute_with_retry
from ...infrastructure.cache import Cache
from ...infrastructure.metrics import MetricsCollector
from ...core.api_client import RateLimitedAPIClient


logger = logging.getLogger(__name__)


class ERA5DataSource(DataSource):
    def __init__(
        self,
        api_client: Optional[RateLimitedAPIClient] = None,
        cache: Optional[Cache] = None,
        metrics: Optional[MetricsCollector] = None,
        retry_policy: Optional[Any] = None,
        cds_api_key: Optional[str] = None,
        cds_api_url: str = "https://cds.climate.copernicus.eu/api/v2"
    ):
        self.cds_api_key = cds_api_key or os.environ.get('CDS_API_KEY')
        self.cds_api_url = cds_api_url
        self.api_client = api_client or RateLimitedAPIClient(base_url=self.cds_api_url)
        self.cache = cache
        self.metrics = metrics
        # self.retry_policy = retry_policy or RetryPolicy()
        self._session: Optional[aiohttp.ClientSession] = None
        
        if not self.cds_api_key:
            logger.warning("CDS API key not found. ERA5 data access will be limited.")
            
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {}
            if self.cds_api_key:
                headers['Authorization'] = f'Bearer {self.cds_api_key}'
            self._session = aiohttp.ClientSession(headers=headers)
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
        
        japan_bbox = {
            'lat_min': 24.0,
            'lat_max': 46.0,
            'lon_min': 122.0,
            'lon_max': 146.0
        }
        
        grid_resolution = 0.25
        
        lat_steps = int((japan_bbox['lat_max'] - japan_bbox['lat_min']) / grid_resolution)
        lon_steps = int((japan_bbox['lon_max'] - japan_bbox['lon_min']) / grid_resolution)
        
        for lat_idx in range(0, lat_steps, 4):
            for lon_idx in range(0, lon_steps, 4):
                if limit and len(locations) >= limit:
                    break
                    
                lat = japan_bbox['lat_min'] + lat_idx * grid_resolution
                lon = japan_bbox['lon_min'] + lon_idx * grid_resolution
                
                location = Location(
                    id=f"ERA5_JP_{lat:.2f}N_{lon:.2f}E",
                    name=f"ERA5 Grid {lat:.2f}°N {lon:.2f}°E",
                    coordinates=Coordinates(
                        latitude=Decimal(str(lat)),
                        longitude=Decimal(str(lon))
                    ),
                    city="",
                    country="JP",
                    metadata={
                        'type': 'reanalysis',
                        'grid_resolution': '0.25x0.25',
                        'data_source': 'era5',
                        'levels': ['surface', '1000hPa', '925hPa', '850hPa', '700hPa', '500hPa', '300hPa', '250hPa', '200hPa', '100hPa'],
                        'temporal_resolution': 'hourly',
                        'coverage_start': '1940-01-01',
                        'coverage_end': 'present'
                    }
                )
                locations.append(location)
                
        return locations
        
    async def get_sensors(
        self,
        location: Location,
        parameters: Optional[List[ParameterType]] = None,
        **filters: Any
    ) -> List[Sensor]:
        sensors = []
        
        era5_params = {
            ParameterType.TEMPERATURE: {
                'single_level': '2m_temperature',
                'pressure_level': 'temperature',
                'unit': MeasurementUnit.CELSIUS
            },
            ParameterType.HUMIDITY: {
                'single_level': '2m_dewpoint_temperature',
                'pressure_level': 'relative_humidity',
                'unit': MeasurementUnit.PERCENT
            },
            ParameterType.PRESSURE: {
                'single_level': 'surface_pressure',
                'unit': MeasurementUnit.HECTOPASCALS
            },
            ParameterType.WIND_SPEED: {
                'single_level': '10m_u_component_of_wind',
                'pressure_level': 'u_component_of_wind',
                'unit': MeasurementUnit.METERS_PER_SECOND
            },
            ParameterType.WIND_DIRECTION: {
                'single_level': '10m_v_component_of_wind',
                'pressure_level': 'v_component_of_wind',
                'unit': MeasurementUnit.DEGREES
            },
            ParameterType.PRECIPITATION: {
                'single_level': 'total_precipitation',
                'unit': MeasurementUnit.MILLIMETERS
            },
            ParameterType.SOLAR_RADIATION: {
                'single_level': 'surface_solar_radiation_downwards',
                'unit': MeasurementUnit.WATTS_PER_SQUARE_METER
            },
            ParameterType.CLOUD_COVER: {
                'single_level': 'total_cloud_cover',
                'unit': MeasurementUnit.PERCENT
            },
            ParameterType.VISIBILITY: {
                'single_level': 'visibility',
                'unit': MeasurementUnit.METERS
            },
            ParameterType.DEW_POINT: {
                'single_level': '2m_dewpoint_temperature',
                'unit': MeasurementUnit.CELSIUS
            }
        }
        
        levels = location.metadata.get('levels', ['surface'])
        
        for param_type, param_info in era5_params.items():
            if parameters and param_type not in parameters:
                continue
                
            if 'single_level' in param_info:
                sensor = Sensor(
                    id=f"{location.id}_{param_info['single_level']}_surface",
                    location=location,
                    parameter=param_type,
                    unit=param_info['unit'],
                    is_active=True,
                    metadata={
                        'era5_parameter': param_info['single_level'],
                        'level': 'surface',
                        'data_type': 'reanalysis-era5-single-levels',
                        'data_source': 'era5'
                    }
                )
                sensors.append(sensor)
                
            if 'pressure_level' in param_info:
                for level in levels[1:]:
                    sensor = Sensor(
                        id=f"{location.id}_{param_info['pressure_level']}_{level}",
                        location=location,
                        parameter=param_type,
                        unit=param_info['unit'],
                        is_active=True,
                        metadata={
                            'era5_parameter': param_info['pressure_level'],
                            'level': level,
                            'data_type': 'reanalysis-era5-pressure-levels',
                            'data_source': 'era5'
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
        if not self.cds_api_key:
            logger.warning("ERA5 data access requires CDS API key. Visit https://cds.climate.copernicus.eu/")
            async for measurements in self._get_demo_measurements(sensor, start_date, end_date, limit):
                yield measurements
            return
            
        try:
            async for measurements in self._fetch_era5_data(sensor, start_date, end_date, limit):
                yield measurements
        except Exception as e:
            logger.error(f"Error fetching ERA5 data: {e}")
            raise DataSourceError(f"Failed to get ERA5 measurements: {e}")
            
    async def _get_demo_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[List[Measurement]]:
        demo_data = {
            ParameterType.TEMPERATURE: (15.0, 25.0),
            ParameterType.HUMIDITY: (40.0, 80.0),
            ParameterType.PRESSURE: (1000.0, 1020.0),
            ParameterType.WIND_SPEED: (0.0, 15.0),
            ParameterType.WIND_DIRECTION: (0.0, 360.0),
            ParameterType.PRECIPITATION: (0.0, 5.0),
            ParameterType.SOLAR_RADIATION: (0.0, 800.0),
            ParameterType.CLOUD_COVER: (0.0, 100.0),
            ParameterType.VISIBILITY: (1000.0, 10000.0),
            ParameterType.DEW_POINT: (5.0, 20.0)
        }
        
        param_type = sensor.parameter
        min_val, max_val = demo_data.get(param_type, (0.0, 100.0))
        
        current_time = start_date or datetime.now()
        end_time = end_date or current_time + timedelta(days=1)
        measurements_count = 0
        
        measurements = []
        while current_time < end_time and (not limit or measurements_count < limit):
            value = min_val + (max_val - min_val) * np.sin(current_time.hour / 24 * 2 * np.pi) ** 2
            
            measurement = Measurement(
                sensor=sensor,
                timestamp=current_time,
                value=Decimal(str(round(value, 2))),
                quality_flag="demo",
                metadata={
                    'data_source': 'era5',
                    'note': 'Demo data - real ERA5 requires CDS API key'
                }
            )
            measurements.append(measurement)
            measurements_count += 1
            
            if len(measurements) >= 100:
                yield measurements
                measurements = []
                
            current_time += timedelta(hours=1)
            
        if measurements:
            yield measurements
            
    async def _fetch_era5_data(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[List[Measurement]]:
        """
        Placeholder implementation for ERA5 data fetching.
        
        This method requires the cdsapi package to be installed and configured
        with valid CDS API credentials. Actual implementation pending.
        
        To use ERA5 data:
        1. Install cdsapi: pip install cdsapi
        2. Register at https://cds.climate.copernicus.eu/
        3. Configure credentials in ~/.cdsapirc or CDSAPI_KEY environment variable
        """
        logger.info(f"ERA5 data download requires cdsapi Python package")
        logger.info(f"Install: pip install cdsapi")
        logger.info(f"Configure: Create ~/.cdsapirc with your CDS credentials")
        logger.info(f"Documentation: https://cds.climate.copernicus.eu/api-how-to")
        
        yield []
        
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
            'name': 'ERA5',
            'description': 'ECMWF ERA5 Reanalysis',
            'resolution': '0.25x0.25 degrees',
            'temporal': 'Hourly from 1940 to present',
            'api_required': True,
            'coverage': 'Global'
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        required_fields = ['timestamp', 'value']
        return all(field in data for field in required_fields)
        
    async def health_check(self) -> Dict[str, Any]:
        try:
            session = await self._get_session()
            
            if self.cds_api_key:
                async with session.get(f"{self.cds_api_url}/resources") as response:
                    cds_ok = response.status == 200
            else:
                cds_ok = False
                
            return {
                'status': 'healthy' if cds_ok else 'requires_api_key',
                'cds_api': 'available' if cds_ok else 'api_key_required',
                'note': 'Visit https://cds.climate.copernicus.eu/ for API access',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }