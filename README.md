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

# Traffic
python scripts/download_jartic_archives.py --start 2024-01 --end 2024-12

# Fire detection
python scripts/download_fire_data.py --country JP --days 7

# ERA5 PBL height
python scripts/download_era5_pbl.py --country JP --start 2024-01-01 --end 2024-01-07

# Elevation grid
python scripts/download_elevation_grid.py --country JP
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

### Planned Enhancements
| Source | Real-time | Latency | Coverage |
|--------|-----------|---------|----------|
| **Upwind Monitoring** | ✓ Yes | 1-3 hours | Via OpenAQ |
| **HYSPLIT Trajectories** | ✓ Yes | 6 hours | 96-hour backward |
| **CAMS Chemical Transport** | ✗ No | 3-5 days | 2003-present |
| **Sentinel-5P Satellite** | ✗ No | 3-5 days | 2018-present |
| **Industrial Emissions** (CEMS) | ✓ Yes | 1 hour | China/India |
| **Urban Form** | ✗ No | Static | One-time analysis |
| **Natural Sources** (Dust) | ✓ Yes | 6 hours | Forecast only |

*JMA: 10-min for precipitation products, hourly for other parameters
**FIRMS: <60 seconds for US/Canada, 30 min for geostationary satellites, 3 hours global

## Data Processing Pipeline

### Overview
The project includes a comprehensive data processing pipeline that:
1. Processes raw data from each source into standardized formats
2. Aggregates data using H3 hexagonal grid system (Resolution 8: ~0.5 km² hexagons)
3. Combines all sources into a unified dataset with hourly resolution

### How H3 Hexagonal Aggregation Works

#### What is H3?
H3 is a hierarchical hexagonal geospatial indexing system developed by Uber. It divides the Earth's surface into hexagonal cells at multiple resolutions, where each hexagon has a unique identifier.

#### Why Hexagons Instead of Squares?
```
   Square Grid                    Hexagonal Grid
   ┌───┬───┬───┐                  ⬡---⬡---⬡
   │ A │ B │ C │                 ⬡ A ⬡ B ⬡
   ├───┼───┼───┤                ⬡---⬡---⬡---⬡
   │ D │ X │ E │                ⬡ C ⬡ X ⬡ D ⬡
   ├───┼───┼───┤                 ⬡---⬡---⬡---⬡
   │ F │ G │ H │                  ⬡ E ⬡ F ⬡
   └───┴───┴───┘                   ⬡---⬡---⬡

   Distances from X:               Distances from X:
   - To edges: 1 unit              - To all neighbors: 1 unit
   - To corners: 1.41 units        - Uniform distance!
```

**Advantages of Hexagons:**
- **Uniform Distance**: All 6 neighbors are equidistant from center (vs 2 distances for squares)
- **Better Gradients**: More accurate for interpolation and gradient calculations
- **Isotropic**: No directional bias (squares favor cardinal directions)
- **Natural Clustering**: Better represents circular influence zones (pollution dispersion, etc.)

#### The Aggregation Process

**Step 1: Assign Points to Hexagons**
```python
# Each lat/lon point gets mapped to a hexagon ID
lat, lon = 35.6762, 139.6503  # Tokyo
hex_id = h3.latlng_to_cell(lat, lon, resolution=8)
# Result: "8844c0a31dfffff" (unique hexagon identifier)
```

**Step 2: Aggregate Within Hexagons**
```
Raw Data Points                 Hexagon Aggregation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Location A: PM2.5 = 23 µg/m³    ┌─────────────────┐
Location B: PM2.5 = 28 µg/m³ -> │ Hexagon X       │
Location C: PM2.5 = 25 µg/m³    │ PM2.5 mean: 25.3│
(all within same hexagon)       │ PM2.5 std: 2.5  │
                                │ PM2.5 max: 28   │
                                │ Count: 3        │
                                └─────────────────┘
```

**Step 3: Temporal Aggregation**
```
5-minute traffic data           Hourly aggregation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
14:00 - Speed: 60 km/h         ┌─────────────────┐
14:05 - Speed: 55 km/h         │ Hour: 14:00     │
14:10 - Speed: 45 km/h      -> │ Avg Speed: 52.5 │
...                             │ Min Speed: 45   │
14:55 - Speed: 50 km/h         │ Max Speed: 60   │
                                └─────────────────┘
```

#### Resolution Hierarchy
H3 supports 16 resolutions (0-15). We use Resolution 8 for fine-grained analysis:

| Resolution | Avg Hexagon Area | Edge Length | Use Case |
|------------|------------------|-------------|----------|
| 6 | ~36.13 km² | ~6.13 km | Country-level analysis |
| 7 | ~5.16 km² | ~2.32 km | Regional/city analysis |
| **8** | **~0.74 km²** | **~0.88 km** | **Our default - neighborhood** |
| 9 | ~0.11 km² | ~0.33 km | Street-level analysis |
| 10 | ~0.015 km² | ~0.13 km | Building-level |

#### Example: Processing Tokyo Air Quality Data

**Input**: 1000 sensor readings across Tokyo over 24 hours
```
timestamp,latitude,longitude,pm25
2023-01-01 00:00:00,35.6762,139.6503,23.5
2023-01-01 00:00:00,35.6894,139.6917,25.2
... (998 more rows)
```

**Processing Steps**:
1. **Add H3 Index**: Each row gets hexagon ID based on lat/lon
2. **Group by Hexagon & Hour**: Data grouped by (h3_index, timestamp_hour)
3. **Aggregate Statistics**: Calculate mean, std, min, max for each group
4. **Result**: ~50-100 hexagon-hours (depending on spatial distribution)

**Output**:
```
timestamp,h3_index_res8,pm25_mean,pm25_std,pm25_min,pm25_max,count
2023-01-01 00:00,8844c0a31dfffff,24.3,2.1,21.5,28.9,15
2023-01-01 00:00,8844c0a33bfffff,22.8,1.8,20.2,25.5,12
...
```

#### Handling Multiple Data Sources

When combining different data sources with varying spatial densities:

```
OpenAQ Sensors      OpenMeteo Grid      Unified H3 Grid
     •                  ▫ ▫ ▫            ⬡ ⬡ ⬡
   •   •              ▫ ▫ ▫ ▫     ->    ⬡ ⬡ ⬡ ⬡
     •                ▫ ▫ ▫ ▫            ⬡ ⬡ ⬡ ⬡
(irregular)          (regular)          (standardized)
```

All data sources are mapped to the same H3 grid, enabling:
- Direct spatial joins without complex distance calculations
- Consistent resolution across all data types
- Efficient storage and querying

#### Benefits for Machine Learning

The hexagonal aggregation provides:
- **Fixed-size feature vectors**: Each hexagon-hour is one training sample
- **Spatial context**: Neighboring hexagons can be easily identified
- **Reduced noise**: Aggregation smooths out sensor anomalies
- **Balanced datasets**: Regular grid prevents spatial sampling bias
- **Efficient computation**: Hexagon IDs enable fast spatial operations

### Processing Individual Sources

```bash
# Process all sources for a date range
python scripts/process_all_sources.py --country JP --start 2023-01-01 --end 2023-01-31

# Process specific sources only
python scripts/process_all_sources.py --country JP --start 2023-01-01 --end 2023-01-31 --sources openaq openmeteo

# Available sources: openaq, openmeteo, nasapower, era5, firms, jartic, terrain
```

### Creating Unified Dataset

```bash
# Create unified dataset with H3 hexagonal aggregation
python scripts/create_unified_dataset.py --country JP --start 2023-01-01 --end 2023-01-31

# Specify custom sources
python scripts/create_unified_dataset.py --country JP --start 2023-01-01 --end 2023-01-31 --sources openaq openmeteo era5

# Output location
python scripts/create_unified_dataset.py --country JP --start 2023-01-01 --end 2023-01-31 --output unified_jp_202301.csv
```

### H3 Hexagonal Grid System
- **Resolution 8**: ~0.5 km² hexagons (610m edge length) for fine-grained analysis
- **Why Hexagons**: Uniform distance to all neighbors (vs 2 for squares), better for gradient analysis
- **Hierarchical**: Can aggregate to coarser resolutions (Resolution 7: ~5 km² for regional analysis)

### Output Format
The unified dataset contains:
- `timestamp`: Hourly UTC timestamps
- `h3_index_res8`: H3 hexagon identifier
- `h3_lat_res8`, `h3_lon_res8`: Hexagon center coordinates
- **Air Quality**: pm25_ugm3, pm10_ugm3, no2_ugm3, so2_ugm3, co_ppm, o3_ugm3
- **Weather**: temperature_c, humidity_pct, pressure_hpa, wind_speed_ms, precipitation_mm
- **Boundary Layer**: pbl_height_meters
- **Fire**: fire_count, total_frp_mw, max_brightness_k, distance_to_nearest_fire_km
- **Traffic**: avg_speed_kmh, avg_congestion_level, total_vehicle_count
- **Terrain**: elevation_m, slope_degrees, aspect_degrees
- **Derived Features**: heat_index, pm_ratio, has_fire, traffic_index, hour, day_of_week, is_weekend

### Processing Features
- **Temporal Aggregation**: Sub-hourly data (e.g., 5-min traffic) averaged to hourly
- **Spatial Aggregation**: All measurements within hexagon aggregated (mean, std, min, max)
- **Missing Value Handling**: Forward fill (3h), interpolation, or mean imputation
- **Encoding Detection**: Auto-detects UTF-8 or Shift-JIS for Japanese data
- **Memory Optimization**: Processes in monthly chunks for large datasets

## Docs

### Available
- [Air Quality](docs/AIR_QUALITY.md)
- [Weather](docs/WEATHER.md)
- [Traffic](docs/TRAFFIC.md)
- [Fire Detection](docs/FIRE_DETECTION.md)
- [ERA5 PBL Height](docs/ERA5_PBL.md)
- [Elevation Grid](docs/ELEVATION.md)

### Planned Documentation
- Upwind Transport Monitoring Guide
- HYSPLIT Trajectory Guide
- CAMS Chemical Transport Guide
- Sentinel-5P Satellite Guide
- Industrial Emissions (CEMS) Guide
- Urban Form Analysis Guide
- Natural Sources (Dust) Guide

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
