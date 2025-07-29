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