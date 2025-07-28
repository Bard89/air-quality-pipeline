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
```

## Data Sources

- **Air Quality**: OpenAQ
- **Weather**: Open-Meteo, NASA POWER, ERA5, JMA
- **Traffic**: JARTIC (Japan only)

## Docs

- [Air Quality](docs/AIR_QUALITY.md)
- [Weather](docs/WEATHER.md)
- [Traffic](docs/TRAFFIC.md)