# Multi-Modal Environmental Data Collection Platform

A data collection system for environmental monitoring, featuring air quality, weather, and traffic data from multiple sources. WIP

## Overview

This platform collects and processes environmental data from various sources:
- **Air Quality**: Real-time and historical pollution measurements
- **Weather**: Multi-source meteorological data with atmospheric layers
- **Traffic**: Vehicle flow and congestion data
- **Extensible**: Plugin architecture for adding new data sources

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure API keys (if needed)
cp .env.example .env

# Download air quality data
python download_air_quality.py --country JP --max-locations 10 --parallel

# Download weather data
python download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31

# Download traffic data
python download_jartic_archives.py --start 2024-01 --end 2024-12
```

## Data Sources

### 📊 [Air Quality Data](docs/AIR_QUALITY.md)
- **OpenAQ**: Global network with 100+ parameters
- **Coverage**: 100+ countries, thousands of monitoring stations
- **Parameters**: PM2.5, PM10, NO2, O3, CO, SO2, and more
- **Resolution**: Real-time to hourly measurements

### 🌤️ [Weather Data](docs/WEATHER.md)
- **Open-Meteo**: 0.1° grid resolution, no API limits, historical data
- **NASA POWER**: Global coverage, satellite-derived, historical data
- **ERA5**: 0.25° resolution with atmospheric layers (API key required)
- **JMA AMeDAS**: 1,300+ stations, 10-min intervals (last 3 days only)

### 🚗 [Traffic Data](docs/TRAFFIC.md)
- **JARTIC**: Japan Road Traffic Information Center
- **Coverage**: ~2,600 monitoring locations across Japan
- **Resolution**: 5-minute intervals
- **Parameters**: Vehicle counts, speeds, congestion levels

## Architecture

```
src/
├── domain/          # Core business models and interfaces
├── application/     # CLI commands and orchestration
├── infrastructure/  # Storage, caching, retry logic
└── plugins/         # Data source implementations
    ├── openaq/      # Air quality
    ├── jartic/      # Traffic data
    ├── jma/         # Japanese weather
    ├── era5/        # ERA5 reanalysis
    └── nasapower/   # NASA weather
```
## Documentation

- [Air Quality Data Guide](docs/AIR_QUALITY.md)
- [Weather Data Guide](docs/WEATHER.md)
- [Traffic Data Guide](docs/TRAFFIC.md)
- [Architecture Overview](ARCHITECTURE.md)
