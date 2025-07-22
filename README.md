# Air Quality Data Collection

A minimal, efficient tool for downloading global air quality data from OpenAQ. Downloads sensor-specific measurements with precise coordinates for machine learning applications.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Get your OpenAQ API key:
   - Sign up at https://explore.openaq.org/register
   - Create `.env` file: `cp .env.example .env`
   - Add your API key to `.env`

## Usage

### Download Air Quality Data

```bash
# List available countries
./download_air_quality.py --list-countries

# Download full year from India
./download_air_quality.py --country IN --start 2024-01-01 --end 2024-12-31

# Download last 30 days from Japan
./download_air_quality.py --country JP --days 30

# Download specific parameters from USA
./download_air_quality.py --country US --days 90 --parameters pm25,pm10,no2

# Download all particulate matter data
./download_air_quality.py --country DE --days 30 --parameters pm1,pm25,pm10

# Download air quality + weather data
./download_air_quality.py --country JP --days 7 --parameters pm25,temperature,relativehumidity

# Limit sensors for faster downloads
./download_air_quality.py --country TH --days 7 --parameters pm25 --limit-sensors 10

# Smart mode: Auto-select best locations (minimal API requests)
./download_air_quality.py --country IN --days 30 --parameters pm25 --smart

# Download full year efficiently with smart mode
./download_air_quality.py --country CN --start 2024-01-01 --end 2024-12-31 --smart

# Download entire country data efficiently
./download_air_quality.py --country IN --start 2024-01-01 --end 2024-12-31 --country-wide --max-locations 100

# Download all PM2.5 data from top 50 locations
./download_air_quality.py --country IN --days 365 --parameters pm25 --country-wide --max-locations 50
```

### Command Options

- `--country, -c`: Country code (e.g., US, IN, JP, TH)
- `--start, -s`: Start date (YYYY-MM-DD)
- `--end, -e`: End date (YYYY-MM-DD)
- `--days, -d`: Alternative: download last N days
- `--parameters, -p`: Comma-separated parameters (see available parameters below)
- `--limit-sensors, -l`: Limit sensors per parameter
- `--analyze, -a`: Auto-analyze after download (default: true)
- `--smart`: Smart mode - auto-selects best locations for data quality and minimal API usage
- `--country-wide`: Download all data from a country efficiently (best for large datasets)
- `--max-locations`: Limit number of locations (use with --country-wide)

### Available Parameters

**Particulate Matter:**
- `pm25` - Fine particles (≤2.5 μm) - Most common
- `pm10` - Coarse particles (≤10 μm)
- `pm1` - Ultrafine particles (≤1 μm)

**Gases:**
- `no2` - Nitrogen dioxide
- `o3` - Ozone
- `co` - Carbon monoxide
- `so2` - Sulfur dioxide
- `no` - Nitric oxide
- `nox` - Nitrogen oxides

**Meteorological:**
- `temperature` - Air temperature
- `relativehumidity` - Relative humidity
- `pressure` - Atmospheric pressure
- `windspeed` - Wind speed
- `winddirection` - Wind direction

**Other:**
- `bc` - Black carbon
- `um003` - Ultrafine particle count (0.3μm)
- `um010` - Particle count (1.0μm)
- `um025` - Particle count (2.5μm)
- `um100` - Particle count (10μm)

## Output

Data is saved to `data/openaq/processed/{country}_airquality_{startdate}_{enddate}.csv`

### CSV Format
- `datetime`: UTC timestamp
- `value`: Measurement value
- `sensor_id`: Unique sensor identifier
- `location_id`: Location identifier
- `location_name`: Human-readable location
- `latitude`, `longitude`: Exact sensor coordinates
- `parameter`: Pollutant type
- `unit`: Measurement unit
- `city`, `country`: Geographic info

### Automatic Analysis
Each download includes:
- Spatial distribution of sensors
- Parameter statistics (mean, min, max, percentiles)
- Data completeness metrics
- Coverage analysis

## Project Structure

```
├── download_air_quality.py   # Main CLI tool
├── src/
│   ├── core/                # Reusable components
│   │   ├── api_client.py    # Rate-limited HTTP client
│   │   └── data_storage.py  # File management
│   ├── openaq/              # OpenAQ-specific modules
│   │   ├── client.py        # API wrapper
│   │   ├── location_finder.py
│   │   └── data_downloader.py
│   └── utils/
│       └── data_analyzer.py # Data analysis
└── data/                    # Downloaded data (gitignored)
```

## Examples

### Re-analyze Existing Data
```python
from src.utils.data_analyzer import analyze_dataset
analyze_dataset('data/openaq/processed/india_airquality_20240101_20241231.csv')
```

### High-Pollution Countries
Recommended countries with extensive sensor networks:
- **Asia**: IN (India), CN (China), TH (Thailand), JP (Japan), KR (South Korea)
- **Europe**: DE (Germany), GB (United Kingdom), PL (Poland)
- **Americas**: US (United States), MX (Mexico), CL (Chile)

## Performance

### Download Strategies

#### Country-Wide Mode (Best for Full Country)
Use `--country-wide` to download all data from a country efficiently:
- **Incremental saving**: Data saved after each location completes
- **Safe to interrupt**: Automatic checkpoint/resume capability
- **No data loss**: Even if API blocks you, completed data is safe
- Respects rate limits (60 req/min, 2000 req/hour)
- Use `--max-locations` to limit scope
- Processes sensor by sensor with 90-day chunks

**Example for full country:**
```bash
# Download all India PM2.5 data from top 100 locations
./download_air_quality.py --country IN --start 2024-01-01 --end 2024-12-31 \
  --parameters pm25 --country-wide --max-locations 100

# Download EVERYTHING from India (safe to interrupt)
./download_air_quality.py --country IN --start 2024-01-01 --end 2024-12-31 --country-wide

# If interrupted, just run the same command again - it will resume automatically!
```

**How to Safely Interrupt & Resume:**
1. **To interrupt**: Press `Ctrl+C` anytime - data already downloaded is safe
2. **To resume**: Run the EXACT same command again
3. **Progress saved**: Checkpoint file tracks completed locations
4. **No duplicates**: Resume skips already downloaded locations

**Example:**
```bash
# Start download (might take hours)
./download_air_quality.py --country IN --start 2024-01-01 --end 2024-12-31 --country-wide

# Press Ctrl+C after 2 hours...
# Later, resume with same command:
./download_air_quality.py --country IN --start 2024-01-01 --end 2024-12-31 --country-wide
# Output: "Resuming from checkpoint (location 150/500)"
```

#### Smart Mode (Best for Quick Analysis)
The `--smart` flag auto-selects best locations for data quality

**API Request Comparison:**
| Mode | Scope | API Requests | Time |
|------|-------|--------------|------|
| Standard | 500 locations, 1 year | ~24,000 | 10+ hours |
| Country-wide | 100 locations, 1 year | ~400 | ~10 minutes |
| Smart | 20 locations, 1 year | ~100 | ~3 minutes |

### Tips for Minimal API Usage

```bash
# For large datasets, always use --smart
./download_air_quality.py --country IN --start 2024-01-01 --end 2024-12-31 --smart

# Combine with parameter filtering
./download_air_quality.py --country CN --days 365 --parameters pm25,pm10 --smart

# For testing, limit sensors
./download_air_quality.py --country US --days 7 --limit-sensors 5
```

### General Performance

- Rate limited to 60 requests/minute
- Smart mode typically completes in minutes instead of hours
- Shows time estimates before downloading

## License

MIT