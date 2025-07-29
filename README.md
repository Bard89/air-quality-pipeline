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

- **Air Quality**: OpenAQ
- **Weather**: Open-Meteo, NASA POWER, ERA5, JMA
- **Traffic**: JARTIC (Japan only)
- **Fire Detection**: NASA FIRMS (real-time satellite fire data)
- **Atmospheric**: ERA5 PBL height (planetary boundary layer)

## Docs

- [Air Quality](docs/AIR_QUALITY.md)
- [Weather](docs/WEATHER.md)
- [Traffic](docs/TRAFFIC.md)
- [Fire Detection](docs/FIRE_DETECTION.md) # API works only for recent fire data, for historical data manual download necessary
- [ERA5 PBL Height](docs/ERA5_PBL.md)
