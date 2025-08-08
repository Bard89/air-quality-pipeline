# Environmental data Collector

Downloads air quality, weather, traffic and other related data.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # Add your API keys
```

## Usage

```bash
# Air quality
python scripts/download_air_quality.py --country JP --max-locations 10 --parallel

# Weather  
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31

# Traffic - Download archives
python scripts/download_jartic_archives.py --start 2024-01 --end 2024-12

# Traffic - Process downloaded archives (memory-safe parallel processing)
python scripts/process_jartic_parallel.py --archive jartic_typeB_2023_01.zip
python scripts/process_jartic_parallel.py --archive jartic_typeB_2023_01.zip --workers 2  # Safer for low-memory systems
python scripts/process_jartic_parallel.py --archive jartic_typeB_2023_01.zip --sample

# Fire detection
python scripts/download_fire_data.py --country JP --days 7

# ERA5 PBL height
python scripts/download_era5_pbl.py --country JP --start 2024-01-01 --end 2024-01-07

# Elevation grid
python scripts/download_elevation_grid.py --country JP

# HYSPLIT backward trajectories
python scripts/download_hysplit_trajectories.py \
    --country JP \
    --start 2024-01-01 \
    --end 2024-01-07 \
    --altitude 500 \
    --duration -96 \
    --frequency 24

# Sentinel-5P satellite data (requires registration)
# Register at: https://s5phub.copernicus.eu/dhus
python scripts/download_sentinel5p.py \
    --product NO2 \
    --country JP \
    --start 2024-01-01 \
    --end 2024-01-07 \
    --username YOUR_USERNAME \
    --password YOUR_PASSWORD
```

## Advanced Data Processing

### Processing JARTIC Traffic Archives
```bash
# Memory-safe parallel processing (4GB archives, ~50M records each)
python scripts/process_jartic_parallel.py --archive jartic_typeB_2023_01.zip

# Custom worker count (default: CPU cores / 2, max 4)
python scripts/process_jartic_parallel.py --archive jartic_typeB_2023_01.zip --workers 2  # Recommended for 8GB RAM
python scripts/process_jartic_parallel.py --archive jartic_typeB_2023_01.zip --workers 1  # Safe mode for low memory

# Sample data without processing
python scripts/process_jartic_parallel.py --archive jartic_typeB_2023_01.zip --sample

# Features:
# - Memory-safe batch processing of 51 prefectures
# - Real-time progress: "Processing: 75.0% (38/51) | ETA: 5m 23s"
# - Automatic memory management with garbage collection
# - Record limiting to prevent memory exhaustion
# - Single file handle to avoid system resource leaks
# - Outputs standardized CSV with traffic volumes per location
```

### HYSPLIT Trajectory Analysis
```bash
# Download backward trajectories for major cities
python scripts/download_hysplit_trajectories.py \
    --country JP \
    --start 2024-01-01 \
    --end 2024-01-31 \
    --altitude 500 \      # Starting altitude in meters
    --duration -96 \      # 96 hours backward
    --frequency 24        # Daily trajectories

# Available countries: JP, IN, KR
# Trajectories saved as both JSON and CSV
```

### Sentinel-5P Satellite Data
```python
# Using the Python API
from src.plugins.sentinel5p.datasource import Sentinel5PDownloader
from datetime import datetime

downloader = Sentinel5PDownloader(
    username='your_username',  # Register at https://s5phub.copernicus.eu/dhus
    password='your_password'
)

# Search for NO2 products over Japan
products = downloader.search_products(
    product_type='NO2',  # Options: NO2, SO2, CO, CH4, HCHO, O3, AER_AI
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    country='JP',
    max_cloud_cover=50.0
)

# Download products
for product in products[:5]:
    downloader.download_product(product, output_path='../Project-Data/data/sentinel5p/raw/')
```

## External Data Management

This project uses a centralized data storage system that references data from `../Project-Data/data/`. All downloaded data is stored there for sharing across multiple analysis projects.

### Reading Data

```python
from src.utils.data_reader import DataReader
from datetime import datetime

reader = DataReader()

# Read air quality data
df_air = reader.read_openaq(
    country='JP',
    parameters=['pm25', 'pm10']
)

# Read weather data
df_weather = reader.read_weather(
    source='openmeteo',
    country='JP', 
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

# Read elevation data
df_elevation = reader.read_elevation('JP')

# Read fire detection data
df_fires = reader.read_fires('IN')
```

### Data Catalog

View available data files:

```bash
# Show summary of all data sources
python scripts/data_catalog.py --summary

# List files for specific source
python scripts/data_catalog.py --source openaq

# Filter by country
python scripts/data_catalog.py --country JP

# Export catalog to CSV
python scripts/data_catalog.py --export catalog.csv
```

### Storage Configuration

When downloading new data, it automatically saves to `../Project-Data/data/`:

```python
from src.infrastructure.external_storage import ExternalDataStorage
from datetime import datetime

storage = ExternalDataStorage(
    source='openaq',
    country='JP',
    data_type='airquality',
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 1, 31)
)
# Files will be saved to: ../Project-Data/data/openaq/processed/
```

## Data Sources

### Implemented
| Source | Real-time | Latency | Historical |
|--------|-----------|---------|------------|
| **Air Quality** (OpenAQ) | ✓ Yes | Seconds-minutes | 2016-present |
| **Weather** (Open-Meteo) | ✓ Yes | <1 hour | 1940-present |
| **Weather** (NASA POWER) | ✗ No | 2-3 days | 1984-present |
| **Weather** (JMA) | ✓ Yes | 10 min* | Last 3 days only |
| **Weather** (ERA5) | ✗ No | 5 days | 1940-present |
| **Traffic** (JARTIC) | ✓ Yes | 5 min | 2019-present |
| **Fire Detection** (FIRMS) | ✓ Yes | 3 hours** | Last 2 months |
| **Atmospheric** (ERA5 PBL) | ✗ No | 5 days | 1940-present |
| **Elevation Grid** | ✗ No | One-time | Static elevation |

### Advanced Data Sources (New)
| Source | Status | Real-time | Coverage |
|--------|--------|-----------|----------|
| **HYSPLIT Trajectories** | ✅ Ready | ✓ Yes | 96-hour backward trajectories |
| **Sentinel-5P Satellite** | ✅ Ready | ✗ No | Global NO2, SO2, CO, CH4, aerosols |
| **JARTIC Processing** | ✅ Ready | ✓ Yes | Japan traffic volume/speed/occupancy |
| **CAMS Chemical Transport** | 🔧 Planned | ✗ No | 2003-present atmospheric composition |
| **Industrial Emissions** (CEMS) | 🔧 Planned | ✓ Yes | China/India facility emissions |
| **Urban Form** | 🔧 Planned | ✗ No | Building heights, street canyons |
| **Natural Sources** (Dust) | 🔧 Planned | ✓ Yes | Asian dust and volcanic ash forecasts |

*JMA: 10-min for precipitation products, hourly for other parameters
**FIRMS: <60 seconds for US/Canada, 30 min for geostationary satellites, 3 hours global

## Docs

### Available
- [Air Quality](docs/AIR_QUALITY.md)
- [Weather](docs/WEATHER.md)
- [Traffic](docs/TRAFFIC.md)
- [Fire Detection](docs/FIRE_DETECTION.md)
- [ERA5 PBL Height](docs/ERA5_PBL.md)
- [Elevation Grid](docs/ELEVATION.md)
- [New Data Sources Guide](docs/NEW_DATA_SOURCES.md) - HYSPLIT, Sentinel-5P, JARTIC processing
- [ML Integration TODO](docs/ML_INTEGRATION_TODO.md)

## Why These Data Sources?

### Air Quality Foundation
**OpenAQ**: Ground truth measurements of PM2.5 (particulate matter <2.5μm), NO2 (nitrogen dioxide), O3 (ozone), SO2 (sulfur dioxide), and CO (carbon monoxide) from official monitoring stations.

### Meteorological Drivers
**Weather (Open-Meteo, NASA POWER, JMA)**: Temperature, humidity, wind speed/direction, precipitation, and pressure data that control pollution dispersion and chemical reactions. NASA POWER = Prediction of Worldwide Energy Resources, JMA = Japan Meteorological Agency.

**ERA5 PBL Height**: Planetary Boundary Layer height from ECMWF Reanalysis v5 (ERA5) - the atmospheric "ceiling" that determines how high pollutants can mix vertically (lower = more concentrated pollution).

### Emission Sources
**Fire Detection (FIRMS)**: Fire Information for Resource Management System - real-time satellite detection of active fires with Fire Radiative Power (FRP) measuring heat output intensity in megawatts.

**Traffic (JARTIC)**: Japan Road Traffic Information Center - congestion data showing vehicle density and speeds on major highways and urban roads.

**Elevation Grid**: Ground elevation data in meters above sea level used to identify valleys, mountains, and terrain features that affect air flow.

### Transport & Dispersion (Planned)
**Upwind Monitoring**: Tracking pollution levels at locations upwind from target areas to predict incoming air mass quality.

**HYSPLIT Trajectories**: Hybrid Single-Particle Lagrangian Integrated Trajectory model - backward air parcel trajectories showing where air masses originated from over the past 48-96 hours.

**CAMS Chemical Transport**: Copernicus Atmosphere Monitoring Service - European model providing gridded estimates of atmospheric composition including aerosols and reactive gases.

### Local Amplification (Planned)
**Terrain Analysis**: Calculating Terrain Ruggedness Index (TRI), valley depth, and sky view factor to identify pollution-trapping topography.

**Urban Form**: Building height, street width ratios, and urban density metrics that create street canyon effects and modify local wind patterns.

### Enhanced Coverage (Planned)
**Sentinel-5P Satellite**: Daily satellite measurements of NO2 (nitrogen dioxide), SO2 (sulfur dioxide), CO (carbon monoxide), CH4 (methane), and aerosol optical depth at 5.5km resolution.

**Industrial Emissions (CEMS)**: Continuous Emission Monitoring System data from industrial stacks showing real-time SO2 (sulfur dioxide), NOx (nitrogen oxides), and PM (particulate matter) emissions.

**Natural Sources**: Dust storm forecasts and volcanic ash advisories tracking natural particulate matter sources.
