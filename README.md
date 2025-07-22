# Air Quality Data Collection

A minimal, efficient tool for downloading global air quality data from OpenAQ. Downloads sensor-specific measurements with precise coordinates for machine learning applications.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Get your OpenAQ API key(s):
   - Sign up at https://explore.openaq.org/register
   - Create `.env` file: `cp .env.example .env`
   - Add your API key(s) to `.env`
   
   **Single API key:**
   ```
   OPENAQ_API_KEY=your-key-here
   ```
   
   **Multiple API keys (for faster downloads):**
   ```
   OPENAQ_API_KEY_01=first-key-here
   OPENAQ_API_KEY_02=second-key-here
   OPENAQ_API_KEY_03=third-key-here
   # ... up to OPENAQ_API_KEY_100
   ```
   
   Using multiple API keys multiplies your rate limit (e.g., 3 keys = 180 requests/minute)
   
   **Benefits of multiple keys:**
   - Each additional key reduces download time proportionally
   - 3 keys = ~3x faster downloads
   - 10 keys = ~10x faster downloads
   - Automatic round-robin rotation between keys
   - No complex setup required

## Usage

### Download Air Quality Data

```bash
# List available countries
./download_air_quality.py --list-countries

# Download ALL data from India (no date filtering)
./download_air_quality.py --country IN --country-wide

# Download with specific parameters
./download_air_quality.py --country US --parameters pm25,pm10,no2 --country-wide

# Limit to top 100 locations for faster downloads
./download_air_quality.py --country IN --country-wide --max-locations 100

# Download all PM2.5 data from top 50 locations
./download_air_quality.py --country IN --parameters pm25 --country-wide --max-locations 50

# Use parallel mode for experimental faster downloads (requires multiple API keys)
./download_air_quality.py --country IN --country-wide --max-locations 10 --parallel
```

### Command Options

- `--country, -c`: Country code (e.g., US, IN, JP, TH)
- `--parameters, -p`: Comma-separated parameters (see available parameters below)
- `--country-wide`: Download ALL available data from a country (no date filtering)
- `--max-locations`: Limit number of locations (use with --country-wide)
- `--analyze, -a`: Auto-analyze after download (default: true)
- `--list-countries`: List all available countries
- `--parallel`: Enable parallel mode (experimental, requires multiple API keys)

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

Data is saved to `data/openaq/processed/{country}_airquality_all_{timestamp}.csv`

**Note**: Due to OpenAQ API limitations, the tool now downloads ALL available data from sensors without date filtering. The API ignores date parameters and returns data starting from the oldest available measurements.

### CSV Format (Long Format)
- `datetime`: UTC timestamp
- `value`: Measurement value
- `sensor_id`: Unique sensor identifier
- `location_id`: Location identifier
- `location_name`: Human-readable location
- `latitude`, `longitude`: Exact sensor coordinates
- `parameter`: Pollutant type
- `unit`: Measurement unit
- `city`, `country`: Geographic info

### Convert to Wide Format

Transform data to have one row per location/hour with all parameters as columns:

```bash
# Convert downloaded data
python3 transform_to_wide.py data/openaq/processed/in_airquality_all_20241215_123045.csv

# Creates: in_airquality_all_20241215_123045_wide.csv
```

**Wide Format Columns:**
- `datetime`: Hourly timestamp
- `location_id`, `location_name`, `latitude`, `longitude`: Location info
- Parameter columns with units: `pm25_µg/m³`, `pm10_µg/m³`, `co_µg/m³`, `no2_µg/m³`, `o3_ppm`, `so2_µg/m³`, `temperature_c`, `relativehumidity_%`, etc.

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
│   │   ├── api_client_multi_key.py  # Multi-key rotation for faster downloads
│   │   ├── api_client_parallel.py   # Parallel API client (experimental)
│   │   └── data_storage.py  # File management
│   ├── openaq/              # OpenAQ-specific modules
│   │   ├── client.py        # API wrapper
│   │   ├── location_finder.py
│   │   ├── data_downloader.py
│   │   ├── incremental_downloader_all.py  # Downloads all sensor data
│   │   └── incremental_downloader_parallel.py  # Parallel downloader (experimental)
│   └── utils/
│       └── data_analyzer.py # Data analysis
└── data/                    # Downloaded data (gitignored)
```

## Examples

### Re-analyze Existing Data
```python
from src.utils.data_analyzer import analyze_dataset
analyze_dataset('data/openaq/processed/in_airquality_all_20241215_123045.csv')
```

### Transform Data Format
```bash
# Convert long format to wide format (one row per location/hour)
python3 transform_to_wide.py data/openaq/processed/in_airquality_all_20241215_123045.csv
```

### High-Pollution Countries
Recommended countries with extensive sensor networks:
- **Asia**: IN (India), CN (China), TH (Thailand), JP (Japan), KR (South Korea)
- **Europe**: DE (Germany), GB (United Kingdom), PL (Poland)
- **Americas**: US (United States), MX (Mexico), CL (Chile)

## Performance

### Download Strategies

#### Country-Wide Mode (Downloads ALL Available Data)
Use `--country-wide` to download all historical data from a country:
- **No date filtering**: Downloads ALL available measurements from each sensor
- **Incremental saving**: Data saved after each sensor completes
- **Safe to interrupt**: Automatic checkpoint/resume capability
- **No data loss**: Even if API blocks you, completed data is safe
- Respects rate limits (60 req/min)
- Use `--max-locations` to limit scope

**Example for full country:**
```bash
# Download ALL PM2.5 data from top 100 locations in India
./download_air_quality.py --country IN --parameters pm25 --country-wide --max-locations 100

# Download EVERYTHING from India (safe to interrupt)
./download_air_quality.py --country IN --country-wide

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
./download_air_quality.py --country IN --country-wide

# Press Ctrl+C after 2 hours...
# Later, resume with same command:
./download_air_quality.py --country IN --country-wide
# Output: "Resuming from checkpoint (location 150/500)"
```

**Important Note:**
The tool downloads ALL available historical data from each sensor because the OpenAQ API v3 has a limitation where it ignores date filtering parameters and returns data starting from the oldest available measurements.

### Tips for Efficient Downloads

```bash
# Always use --country-wide for bulk downloads
./download_air_quality.py --country IN --country-wide

# Combine with parameter filtering to reduce data size
./download_air_quality.py --country CN --parameters pm25,pm10 --country-wide --max-locations 100

# For testing, limit locations
./download_air_quality.py --country US --country-wide --max-locations 10
```

### General Performance

- Base rate limit: 60 requests/minute per API key
- With multiple keys: N keys × 60 = total requests/minute
- Effective delay between requests: 1.0s ÷ number of keys
- Data saved incrementally after each sensor completes (safe from interruptions)
- Downloads ALL historical data from each sensor (no date filtering)
- Shows detailed progress: start time, elapsed time, measurements/second, and ETA
- Real-time progress updates during sensor downloads

**Speed improvements with multiple API keys:**
- 1 key: Standard speed (60 req/min)
- 2 keys: 2x faster (120 req/min, 0.5s delay)
- 3 keys: 3x faster (180 req/min, 0.33s delay)
- 5 keys: 5x faster (300 req/min, 0.2s delay)
- 10 keys: 10x faster (600 req/min, 0.1s delay)
- 20 keys: 20x faster (1,200 req/min, 0.05s delay)
- 50 keys: 50x faster (3,000 req/min, 0.02s delay)
- 100 keys: 100x faster (6,000 req/min, 0.01s delay)

## License

MIT