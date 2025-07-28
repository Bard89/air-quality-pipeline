# Weather Data Collection Guide

## Overview

The weather module provides access to multiple meteorological data sources with varying resolutions, from satellite-derived gridded data to high-density ground station networks.

## Quick Start

```bash
# List available sources
python download_weather_data.py --list-sources

# JMA AMeDAS - Note: Only provides recent data (last few days), not historical
# For January 2024 data, use Open-Meteo or NASA POWER instead
python download_weather_parallel.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31

# Download from Open-Meteo (high resolution grid)
python download_weather_parallel.py --source openmeteo --country JP --start 2024-01-01 --end 2024-12-31

# Quick test with NASA POWER
python download_weather_data.py --source nasapower --country JP --max-locations 5
```

## Data Sources Comparison

### 🌡️ JMA AMeDAS (Most Granular - Recent Data Only)
- **Stations**: 1,286 weather stations across Japan
- **Temporal**: 10-minute intervals
- **Type**: Actual ground measurements
- **API Key**: Not required
- **Limitation**: Only provides recent data (last few days)
- **Best for**: Real-time monitoring, not historical analysis

### 🌍 Open-Meteo (Best Balance)
- **Resolution**: 0.1° × 0.1° (~11km grid)
- **Coverage**: 500+ grid points for Japan
- **Temporal**: Hourly
- **API Key**: Not required
- **Rate Limit**: None for reasonable use
- **Best for**: High-resolution historical analysis

### 🛰️ ERA5 (Most Comprehensive)
- **Resolution**: 0.25° × 0.25° (~31km grid)
- **Coverage**: ~150 grid points for Japan
- **Temporal**: Hourly
- **Levels**: Multiple atmospheric layers
- **API Key**: CDS API key required
- **Best for**: Atmospheric research

### 🚀 NASA POWER (Easy Start)
- **Resolution**: 0.5° × 0.5° (~50km grid)
- **Coverage**: ~24 grid points for Japan
- **Temporal**: Hourly/Daily
- **API Key**: Not required
- **Best for**: Quick prototyping

## Available Parameters

All sources provide these core parameters:
- `temperature` - Air temperature (°C)
- `humidity` - Relative humidity (%)
- `pressure` - Atmospheric pressure (hPa)
- `windspeed` - Wind speed (m/s)
- `winddirection` - Wind direction (degrees)
- `precipitation` - Rainfall/snowfall (mm)
- `solar_radiation` - Solar irradiance (W/m²)
- `visibility` - Visibility distance (m)
- `cloud_cover` - Cloud coverage (% or oktas)
- `dew_point` - Dew point temperature (°C)

## Command Options

### Sequential Download
```bash
python download_weather_data.py [OPTIONS]
```

### Parallel Download (Recommended)
```bash
python download_weather_parallel.py [OPTIONS]
```

Options:
- `--source, -s`: Weather data source (jma, openmeteo, era5, nasapower)
- `--country, -c`: Country code (default: JP)
- `--parameters, -p`: Comma-separated parameters
- `--start`: Start date (YYYY-MM-DD)
- `--end`: End date (YYYY-MM-DD)
- `--max-locations`: Limit number of locations
- `--max-concurrent`: Parallel connections (default: 5)
- `--no-analyze`: Skip automatic analysis

## API Configuration

### NASA POWER
No configuration needed - works out of the box.

### JMA
No configuration needed for AMeDAS data.

### Open-Meteo
No configuration needed - free access.

### ERA5
Requires CDS API key:
```env
CDSAPI_KEY=your-cds-api-key-here
```

Get your key at: https://cds.climate.copernicus.eu/

## Output Format

Data is saved with date ranges in filename:
`data/{source}/processed/{country}_{source}_weather_{start_date}_to_{end_date}.csv`

### CSV Structure
```csv
timestamp,value,sensor_id,location_id,location_name,latitude,longitude,parameter,unit,city,country,data_source,level,quality_flag
2024-01-01T00:00:00,15.2,JMA_AMEDAS_47662_temp,JMA_AMEDAS_47662,Tokyo,35.6895,139.6917,temperature,c,Tokyo,JP,amedas,surface,good
```

## Performance Comparison

### Download Speeds
- **Sequential**: ~10 measurements/second
- **Parallel**: ~100+ measurements/second

### Data Volume Examples
- JMA AMeDAS (100 stations, 1 month): ~13M measurements
- JMA AMeDAS (all 1,286 stations, 1 month): ~167M measurements
- Open-Meteo (100 locations, 1 month): ~670K measurements
- NASA POWER (24 locations, 1 month): ~160K measurements

### Recommended Approach
```bash
# For comprehensive Japan weather data in 2024:

# Option 1: High resolution grid (Open-Meteo)
python download_weather_parallel.py --source openmeteo --country JP --start 2024-01-01 --end 2024-12-31 --max-locations 500

# Option 2: NASA POWER (quick overview)
python download_weather_parallel.py --source nasapower --country JP --start 2024-01-01 --end 2024-12-31

# Option 3: ERA5 (requires API key)
python download_weather_parallel.py --source era5 --country JP --start 2024-01-01 --end 2024-12-31
```

## Automated Scripts

### Download Full Year
```bash
# Downloads 2024 weather data month by month
./download_2024_weather_fast.sh
```

## Common Use Cases

### High-Resolution Analysis
```bash
# Open-Meteo for 0.1° grid resolution
python download_weather_parallel.py --source openmeteo --country JP --start 2024-07-01 --end 2024-07-31 --max-locations 200
```

### Spatial Coverage Study
```bash
# Open-Meteo for dense grid coverage
python download_weather_parallel.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31 --max-locations 300
```

### Multi-Level Atmospheric Data
```bash
# ERA5 for atmospheric profiles (requires API key)
python download_weather_parallel.py --source era5 --country JP --start 2024-01-01 --end 2024-01-31
```

## Tips for Large Downloads

1. **Use Parallel Mode**: Always use `download_weather_parallel.py` for large datasets
2. **Monthly Chunks**: The tool automatically splits large date ranges into months
3. **Monitor Progress**: Watch the progress bars and ETA estimates
4. **Start Small**: Test with `--max-locations 10` first
5. **Check Disk Space**: Full year of JMA data can be several GB

## Troubleshooting

### Slow Downloads
- Use parallel mode with higher `--max-concurrent`
- Consider using fewer locations or shorter date ranges
- NASA POWER and Open-Meteo have no rate limits

### Memory Issues
- The parallel downloader processes data in chunks
- Reduce `--max-concurrent` if memory is limited
- Data is saved incrementally

### API Errors
- ERA5 requires valid CDS API credentials
- JMA may have temporary outages
- Open-Meteo and NASA POWER are generally very reliable