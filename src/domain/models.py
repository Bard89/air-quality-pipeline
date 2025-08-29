from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from decimal import Decimal
from enum import Enum


class ParameterType(str, Enum):
    PM25 = "pm25"
    PM10 = "pm10"
    PM1 = "pm1"
    NO2 = "no2"
    O3 = "o3"
    CO = "co"
    SO2 = "so2"
    NO = "no"
    NOX = "nox"
    BC = "bc"
    TEMPERATURE = "temperature"
    RELATIVE_HUMIDITY = "relativehumidity"
    PRESSURE = "pressure"
    WIND_SPEED = "windspeed"
    WIND_DIRECTION = "winddirection"
    PRECIPITATION = "precipitation"
    SOLAR_RADIATION = "solar_radiation"
    VISIBILITY = "visibility"
    CLOUD_COVER = "cloud_cover"
    DEW_POINT = "dew_point"
    HUMIDITY = "humidity"  # Keep for backward compatibility
    # Traffic parameters
    TRAFFIC_VOLUME = "traffic_volume"
    VEHICLE_SPEED = "vehicle_speed"
    OCCUPANCY_RATE = "occupancy_rate"
    # Transit proxy parameters
    TRANSIT_RIDERSHIP = "transit_ridership"
    # Fire detection parameters
    FIRE_RADIATIVE_POWER = "fire_radiative_power"
    FIRE_CONFIDENCE = "fire_confidence"
    FIRE_BRIGHTNESS = "fire_brightness"
    # Atmospheric parameters
    BOUNDARY_LAYER_HEIGHT = "boundary_layer_height"


class MeasurementUnit(str, Enum):
    MICROGRAMS_PER_CUBIC_METER = "µg/m³"
    PARTS_PER_MILLION = "ppm"
    PARTS_PER_BILLION = "ppb"
    CELSIUS = "c"
    FAHRENHEIT = "f"
    PERCENT = "%"
    HECTOPASCALS = "hpa"
    METERS_PER_SECOND = "m/s"
    DEGREES = "degrees"
    MILLIMETERS = "mm"
    WATTS_PER_SQUARE_METER = "W/m²"
    METERS = "m"
    OKTAS = "oktas"
    MINUTES = "minutes"
    # Traffic measurement units
    VEHICLES_PER_HOUR = "vehicles/hour"
    VEHICLES_PER_5MIN = "vehicles/5min"
    KILOMETERS_PER_HOUR = "km/h"
    PERCENT_OCCUPANCY = "%occupancy"
    PASSENGERS_PER_HOUR = "passengers/hour"
    # Fire measurement units
    MEGAWATTS = "MW"
    KELVIN = "K"
    CONFIDENCE_PERCENT = "confidence_%"


@dataclass(frozen=True)
class Coordinates:
    latitude: Decimal
    longitude: Decimal

    def __post_init__(self):
        if not -90 <= self.latitude <= 90:
            raise ValueError(f"Invalid latitude: {self.latitude}")
        if not -180 <= self.longitude <= 180:
            raise ValueError(f"Invalid longitude: {self.longitude}")


@dataclass(frozen=True)
class Location:
    id: str
    name: str
    coordinates: Coordinates
    city: Optional[str] = None
    country: Optional[str] = None
    provider: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.id)


@dataclass(frozen=True)
class Sensor:
    id: str
    location: Location
    parameter: ParameterType
    unit: MeasurementUnit
    is_active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.id)


@dataclass(frozen=True)
class Measurement:
    sensor: Sensor
    timestamp: datetime
    value: Decimal
    quality_flag: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.value < 0 and self.sensor.parameter not in [ParameterType.TEMPERATURE, ParameterType.DEW_POINT]:
            raise ValueError(f"Negative value {self.value} for parameter {self.sensor.parameter}")


@dataclass
class DataSourceConfig:
    name: str
    base_url: str
    api_keys: List[str]
    rate_limit_per_key: int = 60
    timeout: int = 30
    retry_count: int = 3
    cache_ttl: int = 300
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DownloadJob:
    id: str
    source: str
    country_code: Optional[str] = None
    parameters: List[ParameterType] = field(default_factory=list)
    location_ids: Optional[List[str]] = None
    max_locations: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FireEvent:
    id: str
    location: Coordinates
    detection_time: datetime
    fire_radiative_power: Decimal
    confidence: int
    satellite: str  # MODIS or VIIRS
    brightness_temperature: Decimal
    scan_area: Optional[Decimal] = None  # Area of fire pixel in km²
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransportEvent:
    """Detected pollution transport event between upwind and downwind stations."""
    upwind_station_id: int
    downwind_station_id: int
    parameter: str
    event_start: datetime
    event_peak: datetime
    event_end: datetime
    upwind_peak_value: float
    downwind_peak_value: float
    lag_hours: int
    transport_efficiency: float  # Ratio of downwind to upwind concentration change
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Validate datetime ordering
        if self.event_start > self.event_peak:
            raise ValueError(f"Event start ({self.event_start}) must be before or equal to peak ({self.event_peak})")
        if self.event_peak > self.event_end:
            raise ValueError(f"Event peak ({self.event_peak}) must be before or equal to end ({self.event_end})")
        
        # Validate numeric fields
        if self.lag_hours < 0:
            raise ValueError(f"Lag hours must be non-negative, got {self.lag_hours}")
        if self.upwind_peak_value < 0:
            raise ValueError(f"Upwind peak value must be non-negative, got {self.upwind_peak_value}")
        if self.downwind_peak_value < 0:
            raise ValueError(f"Downwind peak value must be non-negative, got {self.downwind_peak_value}")
        if not 0 <= self.transport_efficiency <= 5:  # Allow up to 5x amplification
            raise ValueError(f"Transport efficiency must be between 0 and 5, got {self.transport_efficiency}")


@dataclass(frozen=True)
class LagCorrelation:
    """Results of lag correlation analysis between stations."""
    upwind_station_id: int
    downwind_station_id: int
    parameter: str
    lag_hours: int
    correlation: float
    p_value: float
    sample_size: int
    time_range_start: datetime
    time_range_end: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Validate correlation coefficient
        if not -1 <= self.correlation <= 1:
            raise ValueError(f"Correlation must be between -1 and 1, got {self.correlation}")
        
        # Validate p-value
        if not 0 <= self.p_value <= 1:
            raise ValueError(f"P-value must be between 0 and 1, got {self.p_value}")
        
        # Validate lag hours
        if self.lag_hours < 0:
            raise ValueError(f"Lag hours must be non-negative, got {self.lag_hours}")
        
        # Validate sample size
        if self.sample_size <= 0:
            raise ValueError(f"Sample size must be positive, got {self.sample_size}")
        
        # Validate time range
        if self.time_range_start > self.time_range_end:
            raise ValueError(f"Start time ({self.time_range_start}) must be before end time ({self.time_range_end})")


@dataclass(frozen=True)
class UpwindStation:
    """Upwind monitoring station with transport metadata."""
    sensor_id: int
    location_id: int
    location_name: str
    city: str
    country: str
    latitude: float
    longitude: float
    parameter: str
    distance_km: float
    bearing_degrees: float
    is_upwind: bool
    lag_hours: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Validate geographic coordinates
        if not -90 <= self.latitude <= 90:
            raise ValueError(f"Latitude must be between -90 and 90, got {self.latitude}")
        if not -180 <= self.longitude <= 180:
            raise ValueError(f"Longitude must be between -180 and 180, got {self.longitude}")
        
        # Validate distance
        if self.distance_km < 0:
            raise ValueError(f"Distance must be non-negative, got {self.distance_km}")
        
        # Validate bearing
        if not 0 <= self.bearing_degrees <= 360:
            raise ValueError(f"Bearing must be between 0 and 360 degrees, got {self.bearing_degrees}")
        
        # Validate lag hours
        if self.lag_hours < 0:
            raise ValueError(f"Lag hours must be non-negative, got {self.lag_hours}")