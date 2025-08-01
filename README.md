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
**OpenAQ**: Ground truth PM2.5/NO2/O3 ... measurements from monitoring stations. Essential baseline but sometimes sparse coverage. Mostly cities and hige differences in data quantity and quality between countiries. Good coverage in Asia for Korea, Japan and India. China missing. 

### Meteorological Drivers
**Weather (Open-Meteo, NASA POWER, JMA)**: Wind disperses pollution, rain washes it out, temperature drives chemical reactions. Weather explains maybe 40-60% of pollution variability.

**ERA5 PBL Height**: The "ceiling" that traps pollution. Low PBL (<500m) can increase ground-level pollution 2-5x. Critical for predicting morning rush hour peaks.

### Emission Sources
**Fire Detection (FIRMS)**: Wildfires/agricultural burning can spike PM2.5 10x within hours. Real-time alerts prevent missing sudden pollution events.

**Traffic (JARTIC)**: Rush hour emissions trapped by morning inversions. Traffic volume directly correlates with NO2/PM2.5 in urban areas.

### Transport & Dispersion (Planned)
**Upwind Monitoring**: 50-70% of pollution is transboundary. Tracking upwind cities provides 24-48hr advance warning of incoming pollution.

**HYSPLIT Trajectories**: Identifies where pollution came from. Essential for attribution and predicting which regions will be affected.

**CAMS Chemical Transport**: Fills gaps between ground stations with modeled pollution fields. Captures pollution plumes missed by sparse monitors.

### Local Amplification (Planned)
**Terrain Analysis**: Valleys trap pollution like bowls. Cities in basins (Kyoto, Seoul) see 2-5x higher concentrations than flat areas.

**Urban Form**: Street canyons trap vehicle emissions. Building height/density affects local wind patterns and pollution hotspots.

### Enhanced Coverage (Planned)
**Sentinel-5P Satellite**: Daily NO2/SO2 maps reveal pollution between ground stations. Identifies industrial emissions and ship tracks.

**Industrial Emissions (CEMS)**: Real-time stack monitoring from major polluters. Direct emission rates improve model accuracy near industrial zones.

**Natural Sources**: Dust storms add massive PM. Advance warning prevents misattribution of natural vs anthropogenic pollution.
