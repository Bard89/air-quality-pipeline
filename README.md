# Air Quality Data Collection

Collect and process air quality data from OpenAQ for machine learning models. This project downloads sensor-specific measurements with precise coordinates for accurate geographical predictions.

## Setup

### Prerequisites
- Python 3.8+
- OpenAQ API key (free)

### Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Get OpenAQ API key:
   - Sign up at https://explore.openaq.org/register
   - Find your API key in account settings

4. Configure environment:
```bash
cp .env.example .env
# Add your API key to .env file
```

## Project Structure

```
├── src/
│   ├── core/                 # Reusable components
│   │   ├── api_client.py     # Rate-limited API client
│   │   └── data_storage.py   # Data persistence
│   ├── openaq/              # OpenAQ-specific modules
│   │   ├── client.py        # OpenAQ API wrapper
│   │   ├── location_finder.py # Find sensors by location
│   │   └── data_downloader.py # Download measurements
│   └── utils/               # Utility scripts
│       └── data_summary.py  # Data analysis tools
├── config/                  # Configuration files
│   ├── openaq_config.json   # API settings
│   └── country_mapping.json # Country ID mappings
└── data/                   # Downloaded data (gitignored)
    └── openaq/
        ├── raw/            # Original API responses
        └── processed/      # Cleaned CSV files
```

## Quick Start

Run the interactive example to download data:

```bash
python example_download.py
```

This will guide you through:
1. Selecting a country
2. Finding active PM2.5 sensors
3. Downloading data with exact coordinates

## Usage

### 1. Download Sensor Data by Country

Download air quality data from multiple sensors in a specific country:

```bash
python src/download_vietnam_sensors.py
```

This will:
- Find all active sensors in Vietnam
- Download PM2.5 measurements with exact coordinates
- Save data as CSV with sensor locations

### 2. Analyze Downloaded Data

View summary statistics of downloaded data:

```bash
python src/utils/data_summary.py
```

### 3. Custom Data Collection

```python
from src.openaq.client import OpenAQClient
from src.openaq.location_finder import LocationFinder
from src.openaq.data_downloader import DataDownloader

# Initialize client
client = OpenAQClient(api_key="your_key")
finder = LocationFinder(client)
downloader = DataDownloader(client)

# Find sensors in a country
locations = finder.find_locations_in_country('VN', country_mapping)
sensors = finder.find_active_sensors(locations, parameter='pm25')

# Download data
start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
end_date = datetime(2024, 12, 31, tzinfo=timezone.utc)
df = downloader.download_multiple_sensors(sensors, start_date, end_date)
```

## Data Format

Downloaded CSV files include:
- `datetime`: UTC timestamp
- `value`: Measurement value
- `sensor_id`: Unique sensor identifier
- `location_id`: Location identifier
- `location_name`: Human-readable location
- `latitude`, `longitude`: Exact sensor coordinates
- `parameter`: Pollutant type (e.g., pm25)
- `unit`: Measurement unit (e.g., µg/m³)

## Configuration

### API Settings (`config/openaq_config.json`)
- `rate_limit`: API request limits
- `target_countries`: Countries to focus on
- `pollutants`: Parameters to collect

### Target Countries
Default high-pollution countries:
- China (CN)
- India (IN)
- Vietnam (VN)
- Bangladesh (BD)
- Pakistan (PK)
- Indonesia (ID)
- Thailand (TH)
- Philippines (PH)

## Rate Limits

OpenAQ allows 60 requests per minute. The client automatically handles rate limiting.

## Examples

### Download One Month of Data
```python
# See src/download_vietnam_sensors.py for full example
start_date = datetime(2024, 6, 1, tzinfo=timezone.utc)
end_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
df = downloader.download_sensor_data(sensor_id, start_date, end_date)
```

### Parse Raw JSON to CSV
```python
from src.old_scripts.parse_to_csv import OpenAQParser
parser = OpenAQParser()
parser.process_all_raw_files()
```

## Troubleshooting

### No data returned
- Check if location has recent data using `get_latest_measurements()`
- Some sensors may have historical data only

### Rate limit errors
- Reduce concurrent requests
- Increase delay between requests in config

### Memory issues with large downloads
- Download data in smaller date ranges
- Process data in chunks

## Contributing

1. Keep code clean and self-documenting
2. No unnecessary comments
3. Use type hints
4. Follow existing structure