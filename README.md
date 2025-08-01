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
