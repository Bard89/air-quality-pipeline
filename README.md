# Environmental Data Collector

Downloads air quality, weather, and traffic data.

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
```

## Data Sources

- **Air Quality**: OpenAQ
- **Weather**: Open-Meteo, NASA POWER, ERA5, JMA
- **Traffic**: JARTIC (Japan only)
- **Fire Detection**: NASA FIRMS (real-time satellite fire data)

## Docs

- [Air Quality](docs/AIR_QUALITY.md)
- [Weather](docs/WEATHER.md)
- [Traffic](docs/TRAFFIC.md)
- [Fire Detection](docs/FIRE_DETECTION.md) # API works only for recent fire data, for historical data manual download necessary