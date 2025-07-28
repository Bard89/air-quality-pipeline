# API Documentation

## Plugin Architecture

The platform uses a plugin-based architecture for data sources. Each plugin implements the `DataSource` interface.

### DataSource Interface

```python
class DataSource(ABC):
    async def get_locations(
        self, 
        country: Optional[str] = None,
        limit: Optional[int] = None,
        **filters: Any
    ) -> List[Location]
    
    async def get_sensors(
        self,
        location: Location,
        parameters: Optional[List[ParameterType]] = None,
        **filters: Any
    ) -> List[Sensor]
    
    async def get_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        **filters: Any
    ) -> AsyncIterator[List[Measurement]]
    
    async def close(self) -> None
```

### Core Models

#### Location
```python
class Location:
    id: str
    name: str
    coordinates: Coordinates
    city: Optional[str]
    country: Optional[str]
    metadata: Dict[str, Any]
```

#### Sensor
```python
class Sensor:
    id: str
    location: Location
    parameter: ParameterType
    unit: MeasurementUnit
    is_active: bool
    metadata: Dict[str, Any]
```

#### Measurement
```python
class Measurement:
    sensor: Sensor
    timestamp: datetime
    value: Decimal
    quality_flag: Optional[str]
    metadata: Dict[str, Any]
```

### Parameter Types

```python
class ParameterType(Enum):
    # Air Quality
    PM25 = "pm25"
    PM10 = "pm10"
    NO2 = "no2"
    O3 = "o3"
    CO = "co"
    SO2 = "so2"
    
    # Weather
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"
    WINDSPEED = "windspeed"
    PRECIPITATION = "precipitation"
    
    # Traffic
    TRAFFIC_VOLUME = "traffic_volume"
    TRAFFIC_SPEED = "traffic_speed"
```

## Creating a New Plugin

### 1. Create Plugin Directory
```
src/plugins/yourplugin/
├── __init__.py
├── datasource.py
└── register.py
```

### 2. Implement DataSource
```python
# datasource.py
from ...domain.interfaces import DataSource
from ...domain.models import Location, Sensor, Measurement

class YourPluginDataSource(DataSource):
    async def get_locations(self, **kwargs) -> List[Location]:
        # Implement location discovery
        pass
    
    async def get_sensors(self, location: Location, **kwargs) -> List[Sensor]:
        # Implement sensor discovery
        pass
    
    async def get_measurements(self, sensor: Sensor, **kwargs):
        # Yield measurements in batches
        yield measurements
```

### 3. Register Plugin
```python
# register.py
def register_plugin(registry):
    registry.register('yourplugin', YourPluginDataSource)
```

### 4. Add to Auto-Discovery
Edit `src/plugins/__init__.py`:
```python
plugin_modules = [
    'openaq',
    'jartic',
    'jma',
    'era5',
    'nasapower',
    'openmeteo',
    'yourplugin',  # Add here
]
```

## Rate Limiting

Use the built-in rate limiter:
```python
from ...core.api_client import RateLimitedAPIClient

class YourPluginDataSource(DataSource):
    def __init__(self):
        self.api_client = RateLimitedAPIClient(
            base_url="https://api.example.com",
            rate_limit=60  # requests per minute
        )
```

## Checkpoint Support

For resumable downloads:
```python
from ...core.checkpoint_manager import CheckpointManager

checkpoint = CheckpointManager("yourplugin")
state = checkpoint.load() or {"last_location": 0}

# Update checkpoint
checkpoint.save({"last_location": 150})
```

## Error Handling

Use domain exceptions:
```python
from ...domain.exceptions import DataSourceError, APIError

try:
    response = await self.api_client.get(url)
except Exception as e:
    raise APIError(f"Failed to fetch data: {e}")
```

## Testing Your Plugin

```python
# test_plugin.py
import asyncio
from src.plugins import get_registry

async def test():
    registry = get_registry()
    registry.auto_discover()
    
    DataSource = registry.get('yourplugin')
    ds = DataSource()
    
    # Test location discovery
    locations = await ds.get_locations(country='US', limit=5)
    print(f"Found {len(locations)} locations")
    
    # Test measurements
    if locations:
        sensors = await ds.get_sensors(locations[0])
        async for measurements in ds.get_measurements(sensors[0]):
            print(f"Got {len(measurements)} measurements")
            break
    
    await ds.close()

asyncio.run(test())
```

## Best Practices

1. **Batch Operations**: Return measurements in batches of 1000-5000
2. **Memory Efficiency**: Use async generators for large datasets
3. **Error Recovery**: Implement retry logic for network failures
4. **Metadata**: Store useful information in metadata fields
5. **Type Safety**: Use proper type hints and enums
6. **Documentation**: Add docstrings to all public methods

## Example: OpenAQ Plugin

See `src/plugins/openaq/` for a complete implementation example featuring:
- Multi-page API pagination
- Rate limiting with retries
- Checkpoint/resume support
- Parameter filtering
- Location discovery
- Sensor management

## Available Plugins

Currently implemented plugins:
- `openaq` - OpenAQ air quality data
- `jartic` - Japan Road Traffic Information Center
- `jma` - Japan Meteorological Agency
- `nasapower` - NASA POWER weather data
- `openmeteo` - Open-Meteo weather data
- `era5` - ERA5 reanalysis data