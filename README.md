# Air Quality & Traffic Data Collection

A minimal, efficient tool for downloading global air quality data from OpenAQ and traffic data from JARTIC (Japan). Downloads sensor-specific measurements with precise coordinates for machine learning applications.

## Features

- **Global air quality data** from OpenAQ with 100+ parameters
- **Japanese traffic data** from JARTIC (free, no API key required)
- **Precise GPS coordinates** for each sensor location
- **Automatic checkpoint/resume** for interrupted downloads
- **Parallel downloads** with multiple API keys (air quality)
- **Wide format conversion** for ML-ready datasets
- **Built-in data analysis** with statistics and visualizations

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure API keys:
   **For OpenAQ (air quality data):**
   - Sign up at https://explore.openaq.org/register
   - Create `.env` file: `cp .env.example .env`
   - Add your API key(s) to `.env`
   
   **For JARTIC (traffic data):**
   - No API key required - JARTIC provides free public access
   
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

**Note**: The scripts have executable permissions, so you can also run them directly with `./script_name.py` on Unix-like systems.

### Download Air Quality Data

```bash
# List available countries
python download_air_quality.py --list-countries

# Download ALL data from India (no date filtering)
python download_air_quality.py --country IN --country-wide

# Download with specific parameters
python download_air_quality.py --country US --parameters pm25,pm10,no2 --country-wide

# Limit to top 100 locations for faster downloads
python download_air_quality.py --country IN --country-wide --max-locations 100

# Download all PM2.5 data from top 50 locations
python download_air_quality.py --country IN --parameters pm25 --country-wide --max-locations 50

# Use parallel mode for 2-3x faster downloads (requires multiple API keys)
python download_air_quality.py --country IN --country-wide --max-locations 10 --parallel
```

### Download Traffic Data (Japan Only)

#### Step 1: Download Archives
```bash
# Download JARTIC archives for specific months
python download_jartic_archives.py --start 2024-01 --end 2024-12

# Download with custom cache directory
python download_jartic_archives.py --start 2024-01 --end 2024-03 --cache-dir /custom/path
```

#### Step 2: Process Archives
```bash
# Extract all traffic data (recommended - gets ALL sensors)
python process_jartic_simple.py --start 2024-01-01 --end 2024-01-31

# Count measurements first for accurate progress
python process_jartic_simple_v2.py --start 2024-01-01 --end 2024-01-31

# Process with parallel workers
python process_jartic_archives.py --start 2024-01-01 --end 2024-01-31 --workers 8

# Process limited locations only
python process_jartic_archives.py --start 2024-01-01 --end 2024-01-31 --max-locations 100
```

### Command Options

**Air Quality (download_air_quality.py):**
- `--country, -c`: Country code (e.g., US, IN, JP, TH)
- `--parameters, -p`: Comma-separated parameters (see available parameters below)
- `--country-wide`: Download ALL available data from a country (no date filtering)
- `--max-locations`: Limit number of locations (use with --country-wide)
- `--analyze, -a`: Auto-analyze after download (default: true)
- `--list-countries`: List all available countries
- `--parallel`: Enable parallel mode for faster downloads (requires multiple API keys)

**JARTIC Archive Download (download_jartic_archives.py):**
- `--start, -s`: Start month (YYYY-MM)
- `--end, -e`: End month (YYYY-MM)
- `--cache-dir`: Cache directory for archives (default: data/jartic/cache)

**JARTIC Processing Scripts:**
- `process_jartic_simple.py`: Extract all traffic data from archives
  - `--start, -s`: Start date (YYYY-MM-DD)
  - `--end, -e`: End date (YYYY-MM-DD)
  - `--cache-dir`: Archive directory
  
- `process_jartic_archives.py`: Process with location filtering
  - `--workers, -w`: Number of parallel workers (default: 4)
  - `--max-locations`: Limit number of locations
  - `--analyze, -a`: Analyze data after processing

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

**Air Quality Data:**
Data is saved to `data/openaq/processed/{country}_airquality_all_{timestamp}.csv`

**Traffic Data:**
Data is saved to `data/jartic/processed/jp_traffic_{timestamp}.csv`

**Note**: Due to OpenAQ API limitations, the tool now downloads ALL available data from sensors without date filtering. The API ignores date parameters and returns data starting from the oldest available measurements.

### CSV Format (Long Format)

**Air Quality Data:**
- `datetime`: UTC timestamp
- `value`: Measurement value
- `sensor_id`: Unique sensor identifier
- `location_id`: Location identifier
- `location_name`: Human-readable location
- `latitude`, `longitude`: Exact sensor coordinates
- `parameter`: Pollutant type
- `unit`: Measurement unit
- `city`, `country`: Geographic info

**Traffic Data:**
- `timestamp`: UTC timestamp
- `location_id`: Traffic sensor identifier
- `location_name`: Human-readable location
- `latitude`, `longitude`: Sensor coordinates
- `parameter`: Traffic metric (e.g., vehicle_count, speed)
- `value`: Measurement value
- `unit`: Measurement unit

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
├── download_air_quality.py   # Air quality data CLI
├── download_traffic_data.py  # Traffic data CLI (Japan)
├── transform_to_wide.py      # Convert to wide format
├── view_checkpoints.py       # View download history
├── src/
│   ├── core/                # Reusable components
│   │   ├── api_client.py    # Rate-limited HTTP client
│   │   ├── api_client_multi_key.py  # Multi-key rotation for faster downloads
│   │   ├── api_client_parallel.py   # Parallel API client for concurrent requests
│   │   ├── checkpoint_manager.py    # Resume capability for downloads
│   │   └── data_storage.py  # File management
│   ├── openaq/              # OpenAQ-specific modules
│   │   ├── client.py        # API wrapper
│   │   ├── location_finder.py
│   │   ├── data_downloader.py
│   │   ├── incremental_downloader_all.py  # Downloads all sensor data
│   │   └── incremental_downloader_parallel.py  # Parallel downloader with location batching
│   ├── plugins/jartic/      # JARTIC traffic data plugin
│   │   ├── datasource.py    # JARTIC data source implementation
│   │   ├── archive_downloader.py  # Historical archive handler
│   │   └── data_parser.py   # Parse JARTIC CSV formats
│   └── utils/
│       ├── data_analyzer.py # Data analysis
│       └── csv_to_wide_format.py  # Wide format conversion
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
python transform_to_wide.py data/openaq/processed/in_airquality_all_20241215_123045.csv
```

### View Download History
```bash
# View all download checkpoints
python view_checkpoints.py

# Filter by country
python view_checkpoints.py --country JP

# Show details for specific file
python view_checkpoints.py --file data/openaq/processed/jp_airquality_all_20241215_123045.csv
```

### High-Pollution Countries
Recommended countries with extensive sensor networks:
- **Asia**: IN (India), CN (China), TH (Thailand), JP (Japan), KR (South Korea)
- **Europe**: DE (Germany), GB (United Kingdom), PL (Poland)
- **Americas**: US (United States), MX (Mexico), CL (Chile)

### Traffic Data Availability
- **Japan**: Historical traffic data available through JARTIC (free, no API key required)
- Coverage: ~2,600 traffic monitoring locations across Japan
- Data retention: Historical archives available for download
- Parameters: Vehicle counts, speeds, and traffic density
- Archive size: ~4GB per month (compressed)
- Update frequency: 5-minute intervals

### JARTIC Data Format
Downloaded traffic data includes:
- `timestamp`: ISO 8601 format with 5-minute intervals
- `location_id`: JARTIC sensor ID (e.g., JARTIC_21001)
- `location_name`: Japanese road name (romanized)
- `parameter`: traffic_volume (vehicles/5min)
- `prefecture`: Prefecture name from archive
- Note: Coordinates are placeholder (0,0) - actual mapping in development

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
python download_air_quality.py --country IN --parameters pm25 --country-wide --max-locations 100

# Download EVERYTHING from India (safe to interrupt)
python download_air_quality.py --country IN --country-wide

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
python download_air_quality.py --country IN --country-wide

# Press Ctrl+C after 2 hours...
# Later, resume with same command:
python download_air_quality.py --country IN --country-wide
# Output: "Resuming from checkpoint (location 150/500)"
```

**Checkpoint System:**
The download system uses an advanced checkpoint manager that:
- **Automatic resume**: Finds existing downloads and continues from last position
- **History tracking**: Maintains history of all checkpoint saves
- **Smart file matching**: Associates checkpoints with their output CSV files
- **No data loss**: Even if script crashes, all progress is preserved

**Checkpoint files location:**
- Main checkpoint: `data/openaq/checkpoints/checkpoint_{country}_all_parallel.json`
- History file: `data/openaq/checkpoints/checkpoint_history.json`

**Manual checkpoint management:**
```bash
# View all download history
python view_checkpoints.py

# View specific country downloads
python view_checkpoints.py --country JP

# Fix checkpoint if needed (counts actual CSV records)
python fix_checkpoint_history.py
```

**Important Notes:**
- The tool downloads ALL available historical data from each sensor because the OpenAQ API v3 ignores date filtering parameters
- Maximum 16,000 measurements per sensor due to API page limit (pages 17+ consistently timeout)
- Parallel mode automatically skips problematic pages to ensure smooth downloads

### Tips for Efficient Downloads

```bash
# Always use --country-wide for bulk downloads
python download_air_quality.py --country IN --country-wide

# Combine with parameter filtering to reduce data size
python download_air_quality.py --country CN --parameters pm25,pm10 --country-wide --max-locations 100

# For testing, limit locations
python download_air_quality.py --country US --country-wide --max-locations 10

# Use parallel mode with multiple API keys for fastest downloads
python download_air_quality.py --country VN --country-wide --parallel
```

### Parallel Mode Features

When using `--parallel` with multiple API keys:
- **Concurrent API requests**: Uses all available API keys simultaneously
- **Smart location batching**: Processes multiple locations in parallel when beneficial
- **API key randomization**: Distributes load evenly across all keys
- **Automatic optimization**: Chooses between parallel sensors or parallel locations based on data structure
- **2-3x faster downloads**: Especially effective with 10+ API keys

**Note**: Each sensor is limited to 16,000 measurements (16 pages × 1,000 per page) due to API constraints.

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
- 5 keys: 5x faster (300 req/min, 0.2s delay)
- 10 keys: 10x faster (600 req/min, 0.1s delay)
- 100 keys: 100x faster (6,000 req/min, 0.01s delay)

## Data Sources

- **OpenAQ**: Global air quality data from government monitoring stations
- **JARTIC**: Japan Road Traffic Information Center - free historical traffic data for Japan

## License

MIT