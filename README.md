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
```

## Data Sources

### Implemented
- **Air Quality**: OpenAQ
- **Weather**: Open-Meteo, NASA POWER, ERA5, JMA
- **Traffic**: JARTIC (Japan only)
- **Fire Detection**: NASA FIRMS (real-time satellite fire data)
- **Atmospheric**: ERA5 PBL height (planetary boundary layer)

### TODO - Planned Enhancements
- **Transport Monitoring**: Upwind station tracking (500-1500km) # TODO
- **Terrain Analysis**: SRTM/ASTER elevation, valley detection # TODO
- **Trajectory Analysis**: HYSPLIT backward trajectories # TODO
- **Chemical Transport**: CAMS reanalysis (PM2.5, NO2, SO2) # TODO
- **Satellite Data**: Sentinel-5P TROPOMI (NO2, SO2, CO) # TODO
- **Industrial Emissions**: CEMS data (China/India) # TODO
- **Urban Form**: Street canyon effects, building data # TODO
- **Natural Sources**: Dust storm forecasts # TODO

## Docs

### Available
- [Air Quality](docs/AIR_QUALITY.md)
- [Weather](docs/WEATHER.md)
- [Traffic](docs/TRAFFIC.md)
- [Fire Detection](docs/FIRE_DETECTION.md)
- [ERA5 PBL Height](docs/ERA5_PBL.md)

### TODO - Planned Documentation
- Transport Monitoring Guide # TODO
- Terrain Analysis Guide # TODO
- Trajectory Analysis Guide # TODO
- Satellite Data Guide # TODO
- Industrial Emissions Guide # TODO
