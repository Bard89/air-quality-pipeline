from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import asyncio
import logging
import os

from ...domain.interfaces import DataSource
from ...domain.models import (
    Location, Sensor, Measurement, Coordinates, 
    ParameterType, MeasurementUnit, FireEvent
)
from ...domain.exceptions import DataSourceError, APIError
from .api_client import FIRMSAPIClient
from .processor import FireEmissionProcessor

logger = logging.getLogger(__name__)


class FIRMSDataSource(DataSource):
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('FIRMS_API_KEY')
        if not self.api_key:
            raise ValueError("FIRMS API key required. Set FIRMS_API_KEY environment variable")
            
        self.api_client = FIRMSAPIClient(self.api_key)
        self.processor = FireEmissionProcessor()
        self._supported_countries = ["JP", "KR", "CN", "IN", "TH", "ID", "MY", "VN"]
        
    async def close(self):
        await self.api_client.close()
        
    async def get_locations(
        self,
        country: Optional[str] = None,
        limit: Optional[int] = None,
        **filters: Any
    ) -> List[Location]:
        # FIRMS doesn't have fixed locations - fires are detected dynamically
        # Return virtual locations for each supported country
        locations = []
        
        if country and country not in self._supported_countries:
            return []
            
        countries_to_process = [country] if country else self._supported_countries
        
        for country_code in countries_to_process:
            # Create a virtual location for fire monitoring in each country
            location = Location(
                id=f"FIRMS_{country_code}_MONITOR",
                name=f"Fire Monitoring - {country_code}",
                coordinates=self._get_country_center(country_code),
                city=f"{country_code} Fire Monitor",
                country=country_code,
                metadata={
                    'data_source': 'nasa_firms',
                    'type': 'fire_detection',
                    'satellites': ['MODIS', 'VIIRS'],
                    'update_frequency': '3_hours'
                }
            )
            locations.append(location)
            
            if limit and len(locations) >= limit:
                break
                
        return locations
        
    async def get_sensors(
        self,
        location: Location,
        parameters: Optional[List[ParameterType]] = None,
        **filters: Any
    ) -> List[Sensor]:
        # Create virtual sensors for fire detection parameters
        fire_params = [
            (ParameterType.FIRE_RADIATIVE_POWER, MeasurementUnit.MEGAWATTS),
            (ParameterType.FIRE_CONFIDENCE, MeasurementUnit.CONFIDENCE_PERCENT),
            (ParameterType.FIRE_BRIGHTNESS, MeasurementUnit.KELVIN)
        ]
        
        sensors = []
        for param_type, unit in fire_params:
            if parameters and param_type not in parameters:
                continue
                
            sensor = Sensor(
                id=f"{location.id}_{param_type.value}",
                location=location,
                parameter=param_type,
                unit=unit,
                is_active=True,
                metadata={
                    'data_source': 'nasa_firms',
                    'measurement_type': 'fire_detection'
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
            # Default to last 24 hours if no dates specified
            if not end_date:
                end_date = datetime.now(timezone.utc)
            if not start_date:
                start_date = end_date - timedelta(days=1)
                
            # Determine if we need historical data (older than 2 months)
            use_archive = (datetime.now(timezone.utc) - start_date).days > 60
            
            # Calculate days back for NRT data
            days_back = min((end_date - start_date).days + 1, 10)
            
            # Get country from location
            country_code = sensor.location.country
            if not country_code:
                logger.warning(f"No country code for location {sensor.location.id}")
                return
                
            # Fetch fire data
            fires = []
            
            # For large countries, process by regions
            if country_code in ["IN", "CN"]:
                regions = self._get_country_regions(country_code)
                logger.info(f"Processing {country_code} in {len(regions)} regions")
                
                for i, region in enumerate(regions):
                    logger.info(f"Processing region {i+1}/{len(regions)}")
                    region_desc = f"Region {i+1}: {region['west']:.1f}°E to {region['east']:.1f}°E, {region['south']:.1f}°N to {region['north']:.1f}°N"
                    logger.info(region_desc)
                    
                    if use_archive and (end_date - start_date).days > 10:
                        # Historical data
                        for satellite in ["MODIS", "VIIRS_SNPP"]:
                            try:
                                logger.info(f"Fetching {satellite} data for {region_desc}")
                                satellite_fires = await self.api_client.get_historical_fire_data(
                                    satellite,
                                    region,
                                    start_date,
                                    end_date
                                )
                                logger.info(f"Found {len(satellite_fires)} fires from {satellite}")
                                fires.extend(satellite_fires)
                            except Exception as e:
                                logger.error(f"Error fetching {satellite} for region {i+1}: {e}")
                    else:
                        # Recent data
                        for satellite in ["MODIS_NRT", "VIIRS_SNPP_NRT"]:
                            try:
                                region_fires = await self.api_client.get_fire_data(
                                    satellite,
                                    region,
                                    days_back,
                                    use_archive=use_archive
                                )
                                fires.extend(region_fires)
                            except Exception as e:
                                logger.error(f"Error fetching {satellite} for region {i+1}: {e}")
                                
                    # Small delay between regions
                    await asyncio.sleep(1)
            else:
                # Smaller countries - process normally
                if use_archive and (end_date - start_date).days > 10:
                    # For historical data spanning more than 10 days, use historical method
                    bounds = self._get_country_bounds(country_code)
                    if bounds:
                        for satellite in ["MODIS", "VIIRS_SNPP"]:
                            satellite_fires = await self.api_client.get_historical_fire_data(
                                satellite,
                                bounds,
                                start_date,
                                end_date
                            )
                            fires.extend(satellite_fires)
                else:
                    # Use regular method for recent/short periods
                    fires = await self.api_client.get_active_fires_by_country(
                        country_code,
                        days_back=days_back,
                        use_archive=use_archive
                    )
            
            # Process fires into measurements
            measurements = []
            for fire_data in fires:
                fire_event = self.processor.process_fire_detection(
                    fire_data,
                    fire_data.get('satellite', 'UNKNOWN')
                )
                
                if not fire_event:
                    continue
                    
                # Check if fire is within date range
                if fire_event.detection_time < start_date or fire_event.detection_time > end_date:
                    continue
                    
                # Create measurement based on sensor parameter
                value = None
                if sensor.parameter == ParameterType.FIRE_RADIATIVE_POWER:
                    value = fire_event.fire_radiative_power
                elif sensor.parameter == ParameterType.FIRE_CONFIDENCE:
                    value = Decimal(str(fire_event.confidence))
                elif sensor.parameter == ParameterType.FIRE_BRIGHTNESS:
                    value = fire_event.brightness_temperature
                    
                if value is not None:
                    measurement = Measurement(
                        sensor=sensor,
                        timestamp=fire_event.detection_time,
                        value=value,
                        quality_flag="good",
                        metadata={
                            'fire_id': fire_event.id,
                            'fire_location': {
                                'lat': float(fire_event.location.latitude),
                                'lon': float(fire_event.location.longitude)
                            },
                            'satellite': fire_event.satellite,
                            'scan_area_km2': float(fire_event.scan_area) if fire_event.scan_area else None,
                            'intensity_class': self.processor.classify_fire_intensity(fire_event)
                        }
                    )
                    measurements.append(measurement)
                    
                    if limit and len(measurements) >= limit:
                        break
                        
            # Yield measurements in batches
            if measurements:
                yield measurements
                
        except Exception as e:
            logger.error(f"Error fetching FIRMS data: {e}")
            raise DataSourceError(f"Failed to get FIRMS measurements: {e}")
            
    async def list_countries(self) -> List[Dict[str, str]]:
        return [
            {'code': 'JP', 'name': 'Japan'},
            {'code': 'KR', 'name': 'South Korea'},
            {'code': 'CN', 'name': 'China'},
            {'code': 'IN', 'name': 'India'},
            {'code': 'TH', 'name': 'Thailand'},
            {'code': 'ID', 'name': 'Indonesia'},
            {'code': 'MY', 'name': 'Malaysia'},
            {'code': 'VN', 'name': 'Vietnam'},
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
            'name': 'NASA FIRMS',
            'description': 'Fire Information for Resource Management System',
            'resolution': 'MODIS: 1km, VIIRS: 375m',
            'temporal': 'Near real-time (3 hour latency)',
            'api_required': True,
            'coverage': 'Global',
            'parameters': ['fire_radiative_power', 'fire_confidence', 'fire_brightness'],
            'update_frequency': '3-6 hours',
            'data_retention': '2 months for NRT, 1 year for standard'
        }
        
    def validate(self, data: Dict[str, Any]) -> bool:
        required_fields = ['latitude', 'longitude', 'detection_time']
        return all(field in data for field in required_fields)
        
    async def health_check(self) -> Dict[str, Any]:
        try:
            # Test API with small area
            test_area = {
                "west": 139.0,
                "east": 140.0,
                "north": 36.0,
                "south": 35.0
            }
            
            fires = await self.api_client.get_fire_data(
                "MODIS_NRT",
                test_area,
                days_back=1
            )
            
            api_ok = isinstance(fires, list)
            
            return {
                'status': 'healthy' if api_ok else 'degraded',
                'api': 'available' if api_ok else 'unavailable',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
    def _get_country_center(self, country_code: str) -> Coordinates:
        # Approximate country centers for monitoring
        centers = {
            "JP": Coordinates(Decimal("36.0"), Decimal("138.0")),
            "KR": Coordinates(Decimal("36.5"), Decimal("127.5")),
            "CN": Coordinates(Decimal("35.0"), Decimal("105.0")),
            "IN": Coordinates(Decimal("23.0"), Decimal("80.0")),
            "TH": Coordinates(Decimal("15.0"), Decimal("101.0")),
            "ID": Coordinates(Decimal("-2.0"), Decimal("118.0")),
            "MY": Coordinates(Decimal("3.0"), Decimal("102.0")),
            "VN": Coordinates(Decimal("16.0"), Decimal("106.0")),
        }
        return centers.get(country_code, Coordinates(Decimal("0"), Decimal("0")))
        
    def _get_country_bounds(self, country_code: str) -> Optional[Dict[str, float]]:
        # Country bounding boxes
        # Note: Large countries like IN, CN may need to be processed in smaller regions
        bounds = {
            "JP": {"west": 122.0, "east": 146.0, "north": 46.0, "south": 24.0},
            "KR": {"west": 124.0, "east": 132.0, "north": 39.0, "south": 33.0},
            "CN": {"west": 73.0, "east": 135.0, "north": 54.0, "south": 18.0},
            "IN": {"west": 68.0, "east": 97.0, "north": 36.0, "south": 6.0},
            "TH": {"west": 97.0, "east": 106.0, "north": 21.0, "south": 5.0},
            "ID": {"west": 95.0, "east": 141.0, "north": 6.0, "south": -11.0},
            "MY": {"west": 99.0, "east": 119.0, "north": 8.0, "south": 0.0},
            "VN": {"west": 102.0, "east": 110.0, "north": 24.0, "south": 8.0},
        }
        return bounds.get(country_code)
        
    def _get_country_regions(self, country_code: str) -> List[Dict[str, float]]:
        """
        Split large countries into smaller regions to avoid API timeouts
        """
        if country_code == "IN":
            # Split India into 4 regions
            return [
                {"west": 68.0, "east": 82.0, "north": 36.0, "south": 21.0},  # North/West
                {"west": 82.0, "east": 97.0, "north": 36.0, "south": 21.0},  # North/East
                {"west": 68.0, "east": 82.0, "north": 21.0, "south": 6.0},   # South/West
                {"west": 82.0, "east": 97.0, "north": 21.0, "south": 6.0},   # South/East
            ]
        elif country_code == "CN":
            # Split China into 6 regions
            return [
                {"west": 73.0, "east": 95.0, "north": 54.0, "south": 35.0},   # Northwest
                {"west": 95.0, "east": 115.0, "north": 54.0, "south": 35.0},  # North Central
                {"west": 115.0, "east": 135.0, "north": 54.0, "south": 35.0}, # Northeast
                {"west": 73.0, "east": 95.0, "north": 35.0, "south": 18.0},   # Southwest
                {"west": 95.0, "east": 115.0, "north": 35.0, "south": 18.0},  # South Central
                {"west": 115.0, "east": 135.0, "north": 35.0, "south": 18.0}, # Southeast
            ]
        else:
            # For smaller countries, return single region
            bounds = self._get_country_bounds(country_code)
            return [bounds] if bounds else []
        
    async def get_active_fires(
        self,
        country_code: str,
        days_back: int = 1
    ) -> List[FireEvent]:
        # Get raw fire data
        fires_data = await self.api_client.get_active_fires_by_country(
            country_code,
            days_back=days_back
        )
        
        # Process into FireEvent objects
        fire_events = []
        for fire_data in fires_data:
            fire_event = self.processor.process_fire_detection(
                fire_data,
                fire_data.get('satellite', 'UNKNOWN')
            )
            if fire_event:
                fire_events.append(fire_event)
                
        return fire_events