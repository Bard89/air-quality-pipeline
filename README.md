# Air quality related data collection tool

## Able to collect

- **Air Quality/Pollution measurements**
- **Weather**
- **Traffic**

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure API keys (if needed)
cp .env.example .env

# Download air quality data
python scripts/download_air_quality.py --country JP --max-locations 10 --parallel

# Download weather data (use incremental version for large datasets)
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31

# Download traffic data
python scripts/download_jartic_archives.py --start 2024-01 --end 2024-12
```

## Data Sources

### 📊 [Air Quality Data](docs/AIR_QUALITY.md)
- **OpenAQ**

### 🌤️ [Weather Data](docs/WEATHER.md)
- **Open-Meteo**
- **NASA POWER**
- **ERA5**
- **JMA AMeDAS**

### 🚗 [Traffic Data](docs/TRAFFIC.md)
- **JARTIC**

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

MIT License