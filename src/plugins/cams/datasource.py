"""CAMS data source implementation for PM2.5 and atmospheric composition data."""

import asyncio
import logging
import os
from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
import json
import numpy as np
import xarray as xr
import pandas as pd
from dotenv import load_dotenv

from ...domain.interfaces import DataSource
from ...domain.models import Location, Sensor, Measurement, Coordinates, ParameterType, MeasurementUnit
from ...domain.exceptions import DataSourceError, APIError
from ...infrastructure.cache import Cache
from ...infrastructure.metrics import MetricsCollector
from .client import CAMSClient

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class CAMSDataSource(DataSource):
    """Data source for CAMS atmospheric composition data.
    
    Provides gridded PM2.5, PM10, and other pollutant data from:
    - EAC4 Reanalysis: Historical data with 3-hour resolution
    - Operational Forecasts: 5-day forecasts updated twice daily
    
    Coverage matches OpenMeteo grid: 24-46°N, 123-146°E
    """
    
    def __init__(
        self,
        api_uid: Optional[str] = None,
        api_key: Optional[str] = None,
        cache: Optional[Cache] = None,
        metrics: Optional[MetricsCollector] = None,
        data_dir: str = "data/cams"
    ):
        """Initialize CAMS data source.
        
        Args:
            api_uid: CDS API UID. If None, reads from CAMS_API_UID env variable
            api_key: CDS API key. If None, reads from CAMS_API_KEY env variable
            cache: Optional cache for storing downloaded data
            metrics: Optional metrics collector
            data_dir: Directory for storing downloaded files
        """
        # Get credentials from arguments or environment
        uid = api_uid or os.getenv('CAMS_API_UID')
        key = api_key or os.getenv('CAMS_API_KEY')
        
        self.client = CAMSClient(api_uid=uid, api_key=key)
        self.cache = cache
        self.metrics = metrics
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Define coverage area matching OpenMeteo
        self.coverage_area = {
            'north': 46,
            'west': 123,
            'south': 24,
            'east': 146
        }
        
        # Grid resolution (CAMS is 0.4° x 0.4°)
        self.resolution = 0.4
        
        # Available parameters
        self.parameters_map = {
            ParameterType.PM25: ('pm2p5', MeasurementUnit.MICROGRAMS_PER_CUBIC_METER),
            ParameterType.PM10: ('pm10', MeasurementUnit.MICROGRAMS_PER_CUBIC_METER),
            ParameterType.NO2: ('no2', MeasurementUnit.MICROGRAMS_PER_CUBIC_METER),
            ParameterType.SO2: ('so2', MeasurementUnit.MICROGRAMS_PER_CUBIC_METER),
            ParameterType.CO: ('co', MeasurementUnit.PARTS_PER_MILLION),
            ParameterType.O3: ('o3', MeasurementUnit.MICROGRAMS_PER_CUBIC_METER),
        }
        
        # Cache for loaded datasets
        self._dataset_cache: Dict[str, xr.Dataset] = {}
        
    async def get_locations(
        self,
        country: Optional[str] = None,
        limit: Optional[int] = None,
        **filters: Any
    ) -> List[Location]:
        """Get grid locations for CAMS data.
        
        Creates a grid of locations matching the coverage area.
        """
        locations = []
        
        # Generate grid points
        lats = np.arange(
            self.coverage_area['south'],
            self.coverage_area['north'] + self.resolution,
            self.resolution
        )
        lons = np.arange(
            self.coverage_area['west'],
            self.coverage_area['east'] + self.resolution,
            self.resolution
        )
        
        # For Japan, also include major cities with names
        japan_cities = [
            ("Tokyo", 35.6762, 139.6503),
            ("Osaka", 34.6937, 135.5023),
            ("Yokohama", 35.4437, 139.6380),
            ("Nagoya", 35.1815, 136.9066),
            ("Sapporo", 43.0642, 141.3469),
            ("Fukuoka", 33.5904, 130.4017),
            ("Kyoto", 35.0116, 135.7681),
            ("Kobe", 34.6901, 135.1955),
            ("Sendai", 38.2682, 140.8694),
            ("Hiroshima", 34.3853, 132.4553),
        ]
        
        # Add city locations first
        for city_name, lat, lon in japan_cities:
            if limit and len(locations) >= limit:
                break
                
            location = Location(
                id=f"CAMS_{city_name.upper()}",
                name=city_name,
                coordinates=Coordinates(
                    latitude=Decimal(str(lat)),
                    longitude=Decimal(str(lon))
                ),
                city=city_name,
                country="JP",
                metadata={
                    'data_source': 'cams',
                    'type': 'city_center',
                    'resolution': f'{self.resolution}x{self.resolution} degrees'
                }
            )
            locations.append(location)
        
        # Add grid points
        grid_count = 0
        for lat in lats:
            for lon in lons:
                if limit and len(locations) >= limit:
                    break
                    
                # Skip if too close to an existing city location
                skip = False
                for loc in locations:
                    if abs(float(loc.coordinates.latitude) - lat) < 0.2 and \
                       abs(float(loc.coordinates.longitude) - lon) < 0.2:
                        skip = True
                        break
                        
                if skip:
                    continue
                    
                grid_id = f"CAMS_GRID_{lat:.1f}N_{lon:.1f}E"
                grid_name = f"Grid {lat:.1f}°N {lon:.1f}°E"
                
                location = Location(
                    id=grid_id,
                    name=grid_name,
                    coordinates=Coordinates(
                        latitude=Decimal(str(lat)),
                        longitude=Decimal(str(lon))
                    ),
                    city="",
                    country="",
                    metadata={
                        'data_source': 'cams',
                        'type': 'grid_point',
                        'resolution': f'{self.resolution}x{self.resolution} degrees'
                    }
                )
                locations.append(location)
                grid_count += 1
                
        logger.info(f"Generated {len(locations)} CAMS locations ({len(japan_cities)} cities, {grid_count} grid points)")
        return locations
        
    async def get_sensors(
        self,
        location: Location,
        parameters: Optional[List[ParameterType]] = None,
        **filters: Any
    ) -> List[Sensor]:
        """Get available sensors (parameters) for a location."""
        sensors = []
        
        if parameters is None:
            parameters = list(self.parameters_map.keys())
            
        for param_type in parameters:
            if param_type not in self.parameters_map:
                continue
                
            param_code, unit = self.parameters_map[param_type]
            
            sensor = Sensor(
                id=f"{location.id}_{param_code}",
                location=location,
                parameter=param_type,
                unit=unit,
                is_active=True,
                metadata={
                    'parameter_code': param_code,
                    'data_source': 'cams',
                    'model_type': 'reanalysis/forecast'
                }
            )
            sensors.append(sensor)
            
        return sensors
        
    async def _load_data_for_period(
        self,
        start_date: datetime,
        end_date: datetime,
        use_forecast: bool = False
    ) -> xr.Dataset:
        """Load CAMS data for a time period.
        
        Args:
            start_date: Start of period
            end_date: End of period
            use_forecast: If True, use forecast data; otherwise use reanalysis
            
        Returns:
            xarray Dataset with CAMS data
        """
        # Check cache
        cache_key = f"{start_date.date()}_{end_date.date()}_{'forecast' if use_forecast else 'reanalysis'}"
        if cache_key in self._dataset_cache:
            return self._dataset_cache[cache_key]
            
        datasets = []
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        while current_date <= end_date_only:
            # Check if file already exists
            if use_forecast:
                nc_path = self.data_dir / 'forecast' / f"cams_forecast_{current_date.strftime('%Y%m%d')}_00.nc"
            else:
                nc_path = self.data_dir / 'raw' / f"cams_eac4_{current_date.strftime('%Y%m%d')}.nc"
                
            if not nc_path.exists():
                # Download data
                try:
                    if use_forecast:
                        base_time = datetime.combine(current_date, datetime.min.time())
                        nc_path = await asyncio.get_event_loop().run_in_executor(
                            None,
                            self.client.download_forecast,
                            base_time,
                            list(range(0, 24, 3))  # 0, 3, 6, ..., 21 hours
                        )
                    else:
                        nc_path = await asyncio.get_event_loop().run_in_executor(
                            None,
                            self.client.download_reanalysis,
                            datetime.combine(current_date, datetime.min.time())
                        )
                except Exception as e:
                    logger.error(f"Failed to download CAMS data for {current_date}: {e}")
                    current_date += timedelta(days=1)
                    continue
                    
            # Load dataset
            try:
                ds = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.client.load_and_process,
                    nc_path
                )
                datasets.append(ds)
            except Exception as e:
                logger.error(f"Failed to load {nc_path}: {e}")
                
            current_date += timedelta(days=1)
            
        if not datasets:
            raise DataSourceError("No CAMS data available for the requested period")
            
        # Combine datasets
        combined = xr.concat(datasets, dim='time')
        combined = combined.sortby('time')
        
        # Cache the combined dataset
        self._dataset_cache[cache_key] = combined
        
        return combined
        
    async def get_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        **filters: Any
    ) -> AsyncIterator[List[Measurement]]:
        """Get measurements for a sensor (location + parameter).
        
        Yields batches of measurements from CAMS gridded data.
        """
        try:
            # Default to last 7 days if no dates specified
            if start_date is None:
                start_date = datetime.now() - timedelta(days=7)
            if end_date is None:
                end_date = datetime.now()
                
            # Determine if we should use forecast (for future dates)
            use_forecast = end_date > datetime.now()
            
            # Load data for the period
            ds = await self._load_data_for_period(start_date, end_date, use_forecast)
            
            # Get parameter code
            param_code = sensor.metadata.get('parameter_code', 'pm2p5')
            
            # Check if variable exists in dataset
            if param_code not in ds.variables:
                logger.warning(f"Variable {param_code} not found in CAMS data")
                return
                
            # Extract location coordinates
            lat = float(sensor.location.coordinates.latitude)
            lon = float(sensor.location.coordinates.longitude)
            
            # Interpolate to location
            loc_data = ds[param_code].interp(
                lat=lat,
                lon=lon,
                method='linear'
            )
            
            # Convert to measurements
            measurements = []
            measurement_count = 0
            
            # After load_and_process, time dimension should always be 'time'
            time_values = loc_data.time.values
            
            for i, time_val in enumerate(time_values):
                if limit and measurement_count >= limit:
                    break
                    
                # Get value - use .item() to convert to scalar
                value_data = loc_data.isel(time=i)
                value = float(value_data.values.item())
                
                # Skip NaN values
                if np.isnan(value):
                    continue
                    
                # Convert numpy datetime64 to datetime
                timestamp = pd.Timestamp(time_val).to_pydatetime()
                
                # Check if within requested time range
                if timestamp < start_date or timestamp > end_date:
                    continue
                    
                measurement = Measurement(
                    sensor=sensor,
                    timestamp=timestamp,
                    value=Decimal(str(value)),
                    quality_flag="model",
                    metadata={
                        'data_source': 'cams',
                        'model_type': 'forecast' if use_forecast else 'reanalysis',
                        'spatial_resolution': f'{self.resolution} degrees',
                        'temporal_resolution': '3-hourly'
                    }
                )
                
                measurements.append(measurement)
                measurement_count += 1
                
                # Yield in batches
                if len(measurements) >= 100:
                    yield measurements
                    measurements = []
                    
            # Yield remaining measurements
            if measurements:
                yield measurements
                
        except Exception as e:
            logger.error(f"Error fetching CAMS data: {e}")
            raise DataSourceError(f"Failed to get CAMS measurements: {e}")
            
    async def get_grid_data(
        self,
        parameter: str,
        timestamp: datetime,
        use_forecast: bool = False
    ) -> xr.DataArray:
        """Get gridded data for a specific parameter and time.
        
        Args:
            parameter: Parameter code (e.g., 'pm2p5')
            timestamp: Time to get data for
            use_forecast: Whether to use forecast data
            
        Returns:
            xarray DataArray with gridded values
        """
        # Load data for the date
        ds = await self._load_data_for_period(
            timestamp.replace(hour=0),
            timestamp.replace(hour=23),
            use_forecast
        )
        
        # Get closest time
        data = ds[parameter].sel(time=timestamp, method='nearest')
        
        # Subset to coverage area
        data = data.sel(
            lat=slice(self.coverage_area['south'], self.coverage_area['north']),
            lon=slice(self.coverage_area['west'], self.coverage_area['east'])
        )
        
        return data
        
    async def list_countries(self) -> List[Dict[str, str]]:
        """List countries covered by CAMS."""
        return [
            {'code': 'JP', 'name': 'Japan'},
            {'code': 'KR', 'name': 'South Korea'},
            {'code': 'CN', 'name': 'China (Eastern)'},
        ]
        
    async def find_locations(
        self,
        country_code: Optional[str] = None,
        parameter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Location]:
        """Find locations matching criteria."""
        return await self.get_locations(country=country_code, limit=limit)
        
    async def stream_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> AsyncIterator[Measurement]:
        """Stream measurements one by one."""
        async for measurements in self.get_measurements(sensor, start_date, end_date):
            for measurement in measurements:
                yield measurement
                
    async def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the data source."""
        return {
            'name': 'CAMS',
            'description': 'Copernicus Atmosphere Monitoring Service - Global atmospheric composition',
            'resolution': f'{self.resolution}x{self.resolution} degrees (~40km)',
            'temporal': '3-hourly reanalysis, hourly forecast',
            'coverage': f"{self.coverage_area['south']}°N to {self.coverage_area['north']}°N, "
                       f"{self.coverage_area['west']}°E to {self.coverage_area['east']}°E",
            'parameters': list(self.parameters_map.keys()),
            'api_required': True,
            'data_types': ['reanalysis', 'forecast'],
            'forecast_range': '5 days',
            'latency': 'Reanalysis: 5 days, Forecast: real-time'
        }
        
    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate data structure."""
        required_fields = ['timestamp', 'value', 'parameter']
        return all(field in data for field in required_fields)
        
    async def health_check(self) -> Dict[str, Any]:
        """Check if CAMS service is available."""
        try:
            # Try to download a small test dataset
            test_date = datetime.now() - timedelta(days=10)  # Use date that should be available
            test_ds = await self._load_data_for_period(
                test_date,
                test_date,
                use_forecast=False
            )
            
            return {
                'status': 'healthy' if test_ds is not None else 'degraded',
                'service': 'CAMS',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'service': 'CAMS',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }