# Weather Data Collection Guide

## Overview

The weather module provides access to multiple meteorological data sources with varying resolutions, from satellite-derived gridded data to high-density ground station networks.

## Quick Start

```bash
# List available sources
python scripts/check_weather_data.py

# Download historical data (Open-Meteo recommended)
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31

# Download recent data from JMA (last 3 days only)
# First, get the dates:
START=$(date -I -d "2 days ago")
END=$(date -I)
python scripts/download_weather_incremental.py --source jma --country JP --start $START --end $END

# Download from NASA POWER (slower but reliable)
python scripts/download_weather_incremental.py --source nasapower --country JP --start 2024-01-01 --end 2024-01-31 --max-locations 15

# Download full year data
# Open-Meteo (recommended - fastest for historical data)
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-12-31 --max-concurrent 10

# NASA POWER (slower but reliable alternative)
python scripts/download_weather_incremental.py --source nasapower --country JP --start 2024-01-01 --end 2024-12-31 --max-concurrent 10
```

## Data Sources Comparison

### 🌡️ JMA AMeDAS (Most Granular - Recent Data Only)
- **Stations**: 1,300+ weather stations across Japan
- **Temporal**: 10-minute intervals (highest resolution)
- **Type**: Actual ground measurements
- **API Key**: Not required
- **Limitation**: Only provides recent data (last 3 days) - no historical archive
- **Best for**: Real-time monitoring, current conditions
- **Speed**: Fast for recent data

### 🌍 Open-Meteo (Recommended for Historical Data)
- **Resolution**: 0.1° × 0.1° (~11km grid)
- **Coverage**: 500+ grid points for Japan
- **Temporal**: Hourly, historical from 1940
- **API Key**: Not required
- **Rate Limit**: 10,000 requests/day
- **Speed**: Fast (~3.5 seconds per location)
- **Best for**: Historical weather analysis, research

### 🛰️ ERA5 (Most Comprehensive)
- **Resolution**: 0.25° × 0.25° (~31km grid)
- **Coverage**: ~150 grid points for Japan
- **Temporal**: Hourly
- **Levels**: Multiple atmospheric layers
- **API Key**: CDS API key required
- **Best for**: Atmospheric research

### 🚀 NASA POWER (Reliable Alternative)
- **Resolution**: 0.5° × 0.5° (~50km grid)
- **Coverage**: 100+ grid points for Japan
- **Temporal**: Hourly (2001+), Daily (1984+)
- **API Key**: Not required
- **Speed**: Slow (~45 seconds per location)
- **Best for**: When Open-Meteo is unavailable

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

### Download Command
```bash
python scripts/download_weather_incremental.py [OPTIONS]
```
- Writes data progressively to disk in batches
- Handles large datasets without memory issues
- Supports parallel downloads with --max-concurrent
- All weather sources supported

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
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-12-31 --max-locations 500

# Option 2: NASA POWER (quick overview)
python scripts/download_weather_incremental.py --source nasapower --country JP --start 2024-01-01 --end 2024-12-31

# Option 3: ERA5 (requires API key)
python scripts/download_weather_incremental.py --source era5 --country JP --start 2024-01-01 --end 2024-12-31
```

## Automated Downloads

### Download Full Year
```bash
# Downloads 2024 weather data month by month
# Example: Loop through each month
for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
    python scripts/download_weather_incremental.py \
        --source openmeteo \
        --country JP \
        --start "2024-$month-01" \
        --end "2024-$month-$(date -d "2024-$month-01 +1 month -1 day" +%d)" \
        --max-concurrent 10
done
```

## Common Use Cases

### High-Resolution Analysis
```bash
# Open-Meteo for 0.1° grid resolution
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-07-01 --end 2024-07-31 --max-locations 200
```

### Spatial Coverage Study
```bash
# Open-Meteo for dense grid coverage
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31 --max-locations 300
```

### Multi-Level Atmospheric Data
```bash
# ERA5 for atmospheric profiles (requires API key)
python scripts/download_weather_incremental.py --source era5 --country JP --start 2024-01-01 --end 2024-01-31
```

## Downloading Full Year Data

### Parallel Commands for All Sources (January example)
```bash
# Open-Meteo - January 2024, all locations (~500 grid points)
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31 --max-concurrent 10 &

# NASA POWER - January 2024, all locations (~100 grid points)
python scripts/download_weather_incremental.py --source nasapower --country JP --start 2024-01-01 --end 2024-01-31 --max-concurrent 10 &

# JMA - Recent data only (specify dates within last 3 days)
START=$(date -I -d "2 days ago")
END=$(date -I)
python scripts/download_weather_incremental.py --source jma --country JP --start $START --end $END --max-concurrent 10
```

### Performance Expectations
- **Open-Meteo**: ~3.5 seconds per location
- **NASA POWER**: ~45 seconds per location (13x slower)
- **JMA**: Fast but only recent data

## Tips for Large Downloads

1. **Use Incremental Version**: `download_weather_incremental.py` writes data progressively
2. **Increase Concurrency**: Use `--max-concurrent 10` or higher
3. **Monthly Processing**: Download month by month for better control
4. **Monitor Disk Usage**: Full year can be 50GB+ for all sources
5. **Check Progress**: Files grow incrementally during download

## Troubleshooting

### JMA Returns No Data
- JMA AMeDAS only provides last 3 days of data - this is an API limitation
- Always use dates within 3 days of today
- For historical Japanese weather data, use Open-Meteo (recommended) or NASA POWER

### Memory Issues
- Always use `download_weather_incremental.py` for large datasets
- Data is written in batches of 1,000 rows to prevent memory issues
- Monitor file growth with `watch -n 5 'ls -lh data/*/processed/'`
- Fixed CSV escaping and resource management in latest version

### Slow Downloads
- NASA POWER is inherently slow (~45s/location)
- Open-Meteo is fastest for historical data
- Use multiple concurrent downloads