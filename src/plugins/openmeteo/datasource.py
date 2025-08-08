from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio
import logging
import aiohttp
import csv
from pathlib import Path

from ...domain.interfaces import DataSource
from ...domain.models import Location, Sensor, Measurement, Coordinates, ParameterType, MeasurementUnit
from ...domain.exceptions import DataSourceError, APIError
from ...infrastructure.cache import Cache
from ...infrastructure.metrics import MetricsCollector
from ...core.api_client import RateLimitedAPIClient


logger = logging.getLogger(__name__)


class OpenMeteoDataSource(DataSource):
    def __init__(
        self,
        api_client: Optional[RateLimitedAPIClient] = None,
        cache: Optional[Cache] = None,
        metrics: Optional[MetricsCollector] = None,
        base_url: str = "https://archive-api.open-meteo.com"
    ):
        self.base_url = base_url
        self.api_client = api_client or RateLimitedAPIClient(base_url=self.base_url, requests_per_minute=10000)
        self.cache = cache
        self.metrics = metrics
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
        locations = []
        
        if country == "IN":
            india_files = [
                Path("data/openaq/processed/in_airquality_all_20250729_024256.csv"),
                Path("data/openaq/processed/in_airquality_all_20250723_014552.csv")
            ]
            
            csv_file = None
            for f in india_files:
                if f.exists():
                    csv_file = f
                    break
                    
            if csv_file:
                seen_locations = {}
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        location_id = row['location_id']
                        if location_id not in seen_locations:
                            location_name = row['location_name']
                            lat = float(row['latitude'])
                            lon = float(row['longitude'])
                            
                            seen_locations[location_id] = Location(
                                id=f"OPENMETEO_IN_{location_id}",
                                name=location_name,
                                coordinates=Coordinates(
                                    latitude=Decimal(str(lat)),
                                    longitude=Decimal(str(lon))
                                ),
                                city=row.get('city', ''),
                                country="IN",
                                metadata={
                                    'data_source': 'openmeteo',
                                    'type': 'weather_station',
                                    'resolution': '0.1x0.1 degrees',
                                    'update_frequency': 'hourly',
                                    'openaq_location_id': location_id
                                }
                            )
                            
                            if limit and len(seen_locations) >= limit:
                                break
                                
                locations = list(seen_locations.values())
                
        elif country == "JP":
            japan_regions = [
                ("Tokyo", 35.6762, 139.6503),
                ("Osaka", 34.6937, 135.5023),
                ("Yokohama", 35.4437, 139.6380),
                ("Nagoya", 35.1815, 136.9066),
                ("Sapporo", 43.0642, 141.3469),
                ("Fukuoka", 33.5904, 130.4017),
                ("Kobe", 34.6901, 135.1955),
                ("Kawasaki", 35.5308, 139.7029),
                ("Kyoto", 35.0116, 135.7681),
                ("Saitama", 35.8617, 139.6455),
                ("Hiroshima", 34.3853, 132.4553),
                ("Sendai", 38.2682, 140.8694),
                ("Chiba", 35.6074, 140.1065),
                ("Niigata", 37.9162, 139.0364),
                ("Hamamatsu", 34.7108, 137.7261),
                ("Kumamoto", 32.8032, 130.7079),
                ("Sagamihara", 35.5531, 139.3544),
                ("Okayama", 34.6551, 133.9195),
                ("Oita", 33.2382, 131.6126),
                ("Kanazawa", 36.5611, 136.6565),
                ("Nagasaki", 32.7503, 129.8779),
                ("Toyama", 36.6959, 137.2137),
                ("Kochi", 33.5597, 133.5311),
                ("Takamatsu", 34.3428, 134.0435),
                ("Akita", 39.7200, 140.1023),
                ("Yokosuka", 35.2844, 139.6723),
                ("Wakayama", 34.2305, 135.1708),
                ("Gifu", 35.4237, 136.7607),
                ("Miyazaki", 31.9077, 131.4202),
                ("Nara", 34.6851, 135.8048)
            ]
            
            for i, (city, lat, lon) in enumerate(japan_regions):
                if limit and len(locations) >= limit:
                    break
                    
                location = Location(
                    id=f"OPENMETEO_JP_{i:03d}",
                    name=city,
                    coordinates=Coordinates(
                        latitude=Decimal(str(lat)),
                        longitude=Decimal(str(lon))
                    ),
                    city=city,
                    country="JP",
                    metadata={
                        'data_source': 'openmeteo',
                        'type': 'forecast_api',
                        'resolution': '0.1x0.1 degrees',
                        'update_frequency': 'hourly'
                    }
                )
                locations.append(location)
            
            lat_range = range(24, 46, 1)
            lon_range = range(123, 146, 1)
            grid_locations = []
            
            for lat in lat_range:
                for lon in lon_range:
                    if 24 <= lat <= 46 and 123 <= lon <= 146:
                        grid_id = f"OPENMETEO_GRID_{lat}N_{lon}E"
                        grid_name = f"Grid {lat}°N {lon}°E"
                        
                        location = Location(
                            id=grid_id,
                            name=grid_name,
                            coordinates=Coordinates(
                                latitude=Decimal(str(lat)),
                                longitude=Decimal(str(lon))
                            ),
                            city="",
                            country="JP",
                            metadata={
                                'data_source': 'openmeteo',
                                'type': 'gridded',
                                'resolution': '0.1x0.1 degrees'
                            }
                        )
                        grid_locations.append(location)
                        
            if limit:
                remaining = limit - len(locations)
                locations.extend(grid_locations[:remaining])
            else:
                locations.extend(grid_locations)
            
        return locations
        
    async def get_sensors(
        self,
        location: Location,
        parameters: Optional[List[ParameterType]] = None,
        **filters: Any
    ) -> List[Sensor]:
        sensors = []
        
        weather_params = {
            ParameterType.TEMPERATURE: ('temperature_2m', MeasurementUnit.CELSIUS),
            ParameterType.HUMIDITY: ('relative_humidity_2m', MeasurementUnit.PERCENT),
            ParameterType.PRESSURE: ('surface_pressure', MeasurementUnit.HECTOPASCALS),
            ParameterType.WIND_SPEED: ('wind_speed_10m', MeasurementUnit.METERS_PER_SECOND),
            ParameterType.WIND_DIRECTION: ('wind_direction_10m', MeasurementUnit.DEGREES),
            ParameterType.PRECIPITATION: ('precipitation', MeasurementUnit.MILLIMETERS),
            ParameterType.SOLAR_RADIATION: ('direct_radiation', MeasurementUnit.WATTS_PER_SQUARE_METER),
            ParameterType.VISIBILITY: ('visibility', MeasurementUnit.METERS),
            ParameterType.CLOUD_COVER: ('cloud_cover', MeasurementUnit.PERCENT),
            ParameterType.DEW_POINT: ('dew_point_2m', MeasurementUnit.CELSIUS)
        }
        
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
                    'data_source': 'openmeteo'
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
            
            start = start_date or datetime.now() - timedelta(days=7)
            end = end_date or datetime.now()
            
            current_start = start
            while current_start < end:
                chunk_end = min(current_start + timedelta(days=60), end)
                
                params = {
                    'latitude': float(sensor.location.coordinates.latitude),
                    'longitude': float(sensor.location.coordinates.longitude),
                    'start_date': current_start.strftime('%Y-%m-%d'),
                    'end_date': chunk_end.strftime('%Y-%m-%d'),
                    'hourly': sensor.metadata['parameter_code'],
                    'timezone': 'Asia/Tokyo'
                }
                
                url = f"{self.base_url}/v1/archive"
                
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Open-Meteo API error: {response.status}")
                        current_start = chunk_end + timedelta(days=1)
                        continue
                        
                    data = await response.json()
                    
                if 'hourly' not in data:
                    logger.warning(f"No hourly data in response for {sensor.location.name}")
                    current_start = chunk_end + timedelta(days=1)
                    continue
                    
                hourly_data = data['hourly']
                times = hourly_data.get('time', [])
                values = hourly_data.get(sensor.metadata['parameter_code'], [])
                
                measurements = []
                measurement_count = 0
                
                for i, (time_str, value) in enumerate(zip(times, values)):
                    if value is None:
                        continue
                        
                    if limit and measurement_count >= limit:
                        break
                        
                    timestamp = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    
                    measurement = Measurement(
                        sensor=sensor,
                        timestamp=timestamp,
                        value=Decimal(str(value)),
                        quality_flag="good",
                        metadata={
                            'data_source': 'openmeteo',
                            'model': 'era5'
                        }
                    )
                    measurements.append(measurement)
                    measurement_count += 1
                    
                    if len(measurements) >= 1000:
                        yield measurements
                        measurements = []
                        
                if measurements:
                    yield measurements
                    
                if limit and measurement_count >= limit:
                    break
                    
                current_start = chunk_end + timedelta(days=1)
                
        except Exception as e:
            logger.error(f"Error fetching Open-Meteo data: {e}")
            raise DataSourceError(f"Failed to get Open-Meteo measurements: {e}")
            
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
            'name': 'Open-Meteo',
            'description': 'Free weather API with historical data from ERA5 and other sources',
            'resolution': '0.1x0.1 degrees (~11km)',
            'temporal': 'Hourly',
            'api_required': False,
            'coverage': 'Global',
            'rate_limit': 'No limit for reasonable use'
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        required_fields = ['timestamp', 'value']
        return all(field in data for field in required_fields)
        
    async def health_check(self) -> Dict[str, Any]:
        try:
            session = await self._get_session()
            test_url = f"{self.base_url}/v1/archive"
            params = {
                'latitude': 35.6762,
                'longitude': 139.6503,
                'start_date': '2024-01-01',
                'end_date': '2024-01-01',
                'hourly': 'temperature_2m'
            }
            
            async with session.get(test_url, params=params) as response:
                api_ok = response.status == 200
                
            return {
                'status': 'healthy' if api_ok else 'degraded',
                'api': 'available' if api_ok else 'unavailable',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }