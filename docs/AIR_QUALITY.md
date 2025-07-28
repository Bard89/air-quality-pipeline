# Air Quality Data Collection Guide

## Overview

The air quality module collects pollution measurements from the OpenAQ platform, providing access to data from thousands of monitoring stations worldwide.

## Quick Start

```bash
# List available countries
python download_air_quality.py --list-countries

# Download PM2.5 data from Japan (10 locations)
python download_air_quality.py --country JP --parameters pm25 --max-locations 10 --country-wide

# Download all parameters with parallel mode (faster)
python download_air_quality.py --country IN --country-wide --max-locations 50 --parallel
```

## Data Source: OpenAQ

- **Coverage**: 100+ countries, 15,000+ locations
- **Update Frequency**: Real-time to hourly
- **Historical Data**: Varies by station (typically 2016-present)
- **API Limits**: 60 requests/minute per key

## Available Parameters

### Particulate Matter
- `pm25` - Fine particles (≤2.5 μm) - Most common
- `pm10` - Coarse particles (≤10 μm)
- `pm1` - Ultrafine particles (≤1 μm)

### Gases
- `no2` - Nitrogen dioxide
- `o3` - Ozone
- `co` - Carbon monoxide
- `so2` - Sulfur dioxide
- `no` - Nitric oxide
- `nox` - Nitrogen oxides

### Meteorological (where available)
- `temperature` - Air temperature
- `relativehumidity` - Relative humidity
- `pressure` - Atmospheric pressure
- `windspeed` - Wind speed
- `winddirection` - Wind direction

### Other
- `bc` - Black carbon
- `um003` - Ultrafine particle count (0.3μm)
- `um010` - Particle count (1.0μm)
- `um025` - Particle count (2.5μm)
- `um100` - Particle count (10μm)

## Command Options

```bash
python download_air_quality.py [OPTIONS]
```

- `--country, -c`: Country code (e.g., US, IN, JP, TH)
- `--parameters, -p`: Comma-separated parameters (default: all)
- `--country-wide`: Download ALL available data (no date filtering)
- `--max-locations`: Limit number of locations
- `--parallel`: Enable parallel mode (requires multiple API keys)
- `--analyze, -a`: Auto-analyze after download (default: true)
- `--list-countries`: List all available countries

## API Configuration

### Single API Key
```env
OPENAQ_API_KEY=your-key-here
```

### Multiple API Keys (for parallel mode)
```env
OPENAQ_API_KEY_01=first-key-here
OPENAQ_API_KEY_02=second-key-here
OPENAQ_API_KEY_03=third-key-here
# ... up to OPENAQ_API_KEY_100
```

Benefits of multiple keys:
- Each key = 60 requests/minute
- 3 keys = 180 requests/minute (3x faster)
- 10 keys = 600 requests/minute (10x faster)

## Output Format

Data is saved to: `data/openaq/processed/{country}_airquality_all_{timestamp}.csv`

### CSV Structure (Long Format)
```csv
datetime,value,sensor_id,location_id,location_name,latitude,longitude,parameter,unit,city,country
2024-01-01T00:00:00Z,25.5,12345,1001,Tokyo Station,35.6812,139.7671,pm25,µg/m³,Tokyo,JP
```

### Convert to Wide Format
```bash
python transform_to_wide.py data/openaq/processed/jp_airquality_all_20241215_123045.csv
```

Wide format has one row per location/hour with parameters as columns.

## Checkpoint System

Downloads are automatically resumable:

```bash
# Start download
python download_air_quality.py --country IN --country-wide

# If interrupted, resume with same command
python download_air_quality.py --country IN --country-wide
# Output: "Resuming from checkpoint (location 150/500)"
```

Checkpoint files:
- Main: `data/openaq/checkpoints/checkpoint_{country}_all_parallel.json`
- History: `data/openaq/checkpoints/checkpoint_history.json`

## Performance Tips

1. **Use Parallel Mode**: With multiple API keys, enable `--parallel` for 2-3x faster downloads
2. **Limit Locations**: Use `--max-locations` for testing or specific regions
3. **Filter Parameters**: Download only needed parameters to reduce data size
4. **Country-Wide Mode**: Always use `--country-wide` for bulk downloads

## Common Use Cases

### High-Pollution Monitoring
```bash
# Major Asian cities
python download_air_quality.py --country IN --parameters pm25,pm10 --country-wide --max-locations 100

# China air quality
python download_air_quality.py --country CN --country-wide --parallel
```

### Research Dataset
```bash
# Multi-parameter analysis
python download_air_quality.py --country JP --parameters pm25,no2,o3,so2 --country-wide
```

### Quick Testing
```bash
# Small dataset for development
python download_air_quality.py --country TH --max-locations 5 --country-wide
```

## Troubleshooting

### API Rate Limits
- Base limit: 60 requests/minute
- Use multiple API keys for higher limits
- The tool automatically handles rate limiting

### Large Downloads
- Downloads are incremental (safe to interrupt)
- Use checkpoint system to resume
- Consider using `--max-locations` to limit scope

### Data Quality
- Some stations may have gaps in data
- Use the analysis feature to check completeness
- Filter by parameter availability if needed