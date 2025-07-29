from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from decimal import Decimal
import math
import logging

from ...domain.models import Coordinates, Location, FireEvent

logger = logging.getLogger(__name__)


class FireEmissionProcessor:
    def __init__(self):
        # Emission factors based on literature
        # FRP to PM2.5 emission rate (kg/s per MW)
        self.emission_factors = {
            "forest": 0.015,  # Forest fires
            "savanna": 0.010,  # Grassland/savanna fires
            "agricultural": 0.012,  # Crop residue burning
            "default": 0.013  # Average value
        }
        
    def process_fire_detection(self, fire_data: Dict[str, Any], satellite: str) -> Optional[FireEvent]:
        try:
            # Extract required fields
            lat = fire_data.get('latitude')
            lon = fire_data.get('longitude')
            frp = fire_data.get('frp')  # Fire Radiative Power
            confidence = fire_data.get('confidence')
            brightness = fire_data.get('brightness') or fire_data.get('bright_t31')
            detection_time = fire_data.get('detection_time')
            
            if not all([lat, lon, detection_time]):
                return None
                
            # Generate unique fire ID
            fire_id = f"FIRE_{satellite}_{lat:.3f}_{lon:.3f}_{detection_time.strftime('%Y%m%d%H%M')}"
            
            # Handle confidence values
            if isinstance(confidence, str):
                confidence = int(confidence.replace('%', '')) if '%' in confidence else 50
            elif confidence is None:
                confidence = 50
                
            # Convert FRP (Fire Radiative Power) to Decimal
            frp_value = Decimal(str(frp)) if frp else Decimal('0')
            
            # Get scan area if available
            scan_area = None
            if 'scan' in fire_data and 'track' in fire_data:
                scan_area = Decimal(str(fire_data['scan'] * fire_data['track']))
                
            return FireEvent(
                id=fire_id,
                location=Coordinates(
                    latitude=Decimal(str(lat)),
                    longitude=Decimal(str(lon))
                ),
                detection_time=detection_time,
                fire_radiative_power=frp_value,
                confidence=int(confidence),
                satellite=satellite,
                brightness_temperature=Decimal(str(brightness)) if brightness else Decimal('0'),
                scan_area=scan_area,
                metadata={
                    "raw_data": fire_data,
                    "processing_time": datetime.now(timezone.utc).isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Error processing fire detection: {e}")
            return None
            
    def estimate_pm25_emission_rate(
        self, 
        fire_event: FireEvent,
        fire_type: str = "default"
    ) -> Decimal:
        emission_factor = self.emission_factors.get(fire_type, self.emission_factors["default"])
        
        # Convert FRP (MW) to PM2.5 emission rate (kg/s)
        # Formula: Emission_rate = FRP × Emission_factor
        emission_rate = float(fire_event.fire_radiative_power) * emission_factor
        
        return Decimal(str(emission_rate))
        
    def calculate_fire_impact_radius(self, fire_event: FireEvent) -> float:
        # Estimate impact radius based on FRP
        # Empirical relationship: radius (km) ≈ 10 × sqrt(FRP)
        frp_mw = float(fire_event.fire_radiative_power)
        
        if frp_mw <= 0:
            return 0.0
            
        # Base radius calculation
        base_radius = 10 * math.sqrt(frp_mw)
        
        # Adjust based on confidence
        confidence_factor = fire_event.confidence / 100.0
        
        return base_radius * confidence_factor
        
    def calculate_fire_proximity_index(
        self,
        fire_event: FireEvent,
        station_location: Coordinates,
        wind_speed: float,  # m/s
        wind_direction: float  # degrees
    ) -> float:
        # Calculate distance between fire and station
        distance = self._haversine_distance(
            fire_event.location.latitude,
            fire_event.location.longitude,
            station_location.latitude,
            station_location.longitude
        )
        
        if distance == 0:
            return 100.0  # Maximum impact if fire is at station
            
        # Calculate wind factor
        wind_factor = self._calculate_wind_factor(
            fire_event.location,
            station_location,
            wind_direction
        )
        
        # Fire Proximity Index calculation
        # FPI = (FRP × wind_factor) / (distance² × wind_speed)
        frp_mw = float(fire_event.fire_radiative_power)
        
        if wind_speed <= 0:
            wind_speed = 1.0  # Minimum wind speed
            
        fpi = (frp_mw * wind_factor) / (distance * distance * wind_speed)
        
        # Normalize to 0-100 scale
        return min(100.0, fpi * 1000)
        
    def estimate_smoke_arrival_time(
        self,
        fire_event: FireEvent,
        station_location: Coordinates,
        wind_speed: float  # m/s
    ) -> float:
        # Calculate distance
        distance_km = self._haversine_distance(
            fire_event.location.latitude,
            fire_event.location.longitude,
            station_location.latitude,
            station_location.longitude
        )
        
        # Convert to meters
        distance_m = distance_km * 1000
        
        # Time = distance / speed
        if wind_speed <= 0:
            return float('inf')
            
        arrival_time_seconds = distance_m / wind_speed
        arrival_time_hours = arrival_time_seconds / 3600
        
        return arrival_time_hours
        
    def _haversine_distance(
        self,
        lat1: Decimal,
        lon1: Decimal,
        lat2: Decimal,
        lon2: Decimal
    ) -> float:
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(
            lambda x: math.radians(float(x)),
            [lat1, lon1, lat2, lon2]
        )
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth radius in kilometers
        r = 6371
        
        return c * r
        
    def _calculate_wind_factor(
        self,
        fire_location: Coordinates,
        station_location: Coordinates,
        wind_direction: float
    ) -> float:
        # Calculate bearing from fire to station
        bearing = self._calculate_bearing(
            fire_location.latitude,
            fire_location.longitude,
            station_location.latitude,
            station_location.longitude
        )
        
        # Calculate angle difference
        angle_diff = abs(bearing - wind_direction)
        if angle_diff > 180:
            angle_diff = 360 - angle_diff
            
        # Wind factor: 1.0 when wind blows directly toward station, 0.0 when perpendicular
        wind_factor = math.cos(math.radians(angle_diff))
        
        # Only positive values (ignore when wind blows away)
        return max(0.0, wind_factor)
        
    def _calculate_bearing(
        self,
        lat1: Decimal,
        lon1: Decimal,
        lat2: Decimal,
        lon2: Decimal
    ) -> float:
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(
            lambda x: math.radians(float(x)),
            [lat1, lon1, lat2, lon2]
        )
        
        # Calculate bearing
        dlon = lon2 - lon1
        y = math.sin(dlon) * math.cos(lat2)
        x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
        
        bearing = math.atan2(y, x)
        
        # Convert to degrees and normalize to 0-360
        return (math.degrees(bearing) + 360) % 360
        
    def classify_fire_intensity(self, fire_event: FireEvent) -> str:
        frp = float(fire_event.fire_radiative_power)
        
        if frp < 10:
            return "low"
        elif frp < 50:
            return "moderate"
        elif frp < 100:
            return "high"
        elif frp < 500:
            return "very_high"
        else:
            return "extreme"