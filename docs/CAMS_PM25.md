# CAMS PM2.5 Data Source

The CAMS (Copernicus Atmosphere Monitoring Service) plugin provides gridded PM2.5 and other atmospheric pollutant data covering Japan and surrounding regions.

## Overview

- **Coverage**: 24°N to 46°N, 123°E to 146°E (matching OpenMeteo weather grid)
- **Resolution**: 0.4° × 0.4° (~40 km)
- **Parameters**: PM2.5, PM10, NO2, SO2, O3, CO
- **Temporal**: 3-hourly reanalysis (5-day lag), hourly forecasts (5 days ahead)
- **Data Types**:
  - **EAC4 Reanalysis**: Historical data from 2003-present
  - **Operational Forecasts**: 5-day forecasts updated twice daily

## Key Features

1. **Complete Grid Coverage**: No gaps between stations
2. **Transboundary Transport**: Models pollution movement from China/Korea to Japan
3. **Forecast Capability**: 5-day PM2.5 forecasts for predictions
4. **Multiple Pollutants**: Comprehensive atmospheric composition data
5. **Consistent Quality**: Model-based with data assimilation

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
# Specifically needs: cdsapi, xarray, netCDF4, scipy
```

### 2. Get CDS API Access

1. Register at: https://ads.atmosphere.copernicus.eu/
2. Get your API key from your profile page (key only, NOT the UID:key format)
3. Add credentials to your `.env` file:

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and add your CAMS API key:
# IMPORTANT: Use only the API key, NOT the UID:key format
CAMS_API_KEY=abcdef-1234-5678-90ab-cdef12345678
# CAMS_API_UID is optional and not used by the new API
```

Alternative: You can also create `~/.cdsapirc` file:
```
url: https://ads.atmosphere.copernicus.eu/api
key: YOUR_API_KEY_ONLY
```

**Note**: The API has been updated. Make sure to:
- Use the URL without `/v2` suffix
- Use only the API key (not the `UID:key` format)

## Usage

### Download Historical PM2.5 Data

```bash
# Download PM2.5 for January 2024
python scripts/download_cams_pm25.py --start 2024-01-01 --end 2024-01-31

# Download for specific cities
python scripts/download_cams_pm25.py --start 2024-01-01 --end 2024-01-31 --locations Tokyo Osaka Kyoto

# Download multiple parameters
python scripts/download_cams_pm25.py --start 2024-01-01 --end 2024-01-31 --parameters pm25 pm10 no2

# Use forecast data (for recent/future dates)
python scripts/download_cams_pm25.py --start 2024-01-20 --end 2024-01-25 --forecast
```

### Using the Python API

```python
from src.plugins.cams.datasource import CAMSDataSource
from datetime import datetime
import os

# Initialize data source (uses credentials from .env automatically)
cams = CAMSDataSource()

# Or explicitly provide credentials
cams = CAMSDataSource(
    api_uid=os.getenv('CAMS_API_UID'),
    api_key=os.getenv('CAMS_API_KEY')
)

# Get locations (grid points + cities)
locations = await cams.get_locations()

# Get PM2.5 data for Tokyo
tokyo = next(loc for loc in locations if 'Tokyo' in loc.name)
sensors = await cams.get_sensors(tokyo, parameters=[ParameterType.PM25])

# Stream measurements
async for measurements in cams.get_measurements(
    sensors[0],
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7)
):
    for m in measurements:
        print(f"{m.timestamp}: {m.value} µg/m³")

# Get gridded data
grid_data = await cams.get_grid_data(
    parameter='pm2p5',
    timestamp=datetime(2024, 1, 15, 12, 0),
    use_forecast=False
)
```

## Integration with Weather Data

CAMS data is designed to match the OpenMeteo grid exactly:

```python
# Both cover the same area
coverage = {
    'north': 46,   # Northern Hokkaido
    'south': 24,   # Okinawa
    'west': 123,   # Korean strait
    'east': 146    # Pacific
}

# Combine CAMS PM2.5 with OpenMeteo weather
weather_data = load_openmeteo(date)  # Your existing weather data
pm25_data = load_cams(date)          # CAMS PM2.5 data

# Spatial join on matching coordinates
unified = merge_on_coordinates(weather_data, pm25_data)
```

## 6-Hour PM2.5 Prediction

CAMS is ideal for short-term predictions:

```python
def predict_pm25_6hours(location, current_time):
    # Method 1: Use CAMS forecast directly
    forecast = cams.get_forecast(current_time + 6h, location)
    
    # Method 2: Combine with wind transport
    current_pm25 = cams.get_analysis(current_time, location)
    wind = openmeteo.get_wind(current_time, location)
    
    # Calculate upwind location
    upwind_loc = backtrack(location, wind, 6h)
    upwind_pm25 = cams.get_analysis(current_time, upwind_loc)
    
    # Weighted prediction
    prediction = 0.6 * forecast + 0.4 * upwind_pm25
    return prediction
```

## Data Structure

### Output CSV Format
```csv
timestamp,location_id,location_name,latitude,longitude,parameter,value,unit,data_type
2024-01-01T00:00:00,CAMS_TOKYO,Tokyo,35.6762,139.6503,pm25,15.2,µg/m³,reanalysis
2024-01-01T03:00:00,CAMS_TOKYO,Tokyo,35.6762,139.6503,pm25,16.8,µg/m³,reanalysis
```

### Storage Layout
```
data/cams/
├── raw/                          # Original NetCDF files
│   ├── cams_eac4_20240101.nc    # Reanalysis data
│   └── cams_eac4_20240102.nc
├── forecast/                     # Forecast NetCDF files
│   └── cams_forecast_20240120_00.nc
└── processed/                    # CSV outputs
    └── cams_pm25_20240101_20240131_*.csv
```

## Advantages Over Ground Stations

1. **Complete Coverage**: No spatial gaps
2. **Ocean Coverage**: Tracks pollution over water
3. **Consistent Quality**: Same accuracy everywhere
4. **Transport Modeling**: Built-in atmospheric chemistry
5. **Forecast Capability**: 5-day predictions available

## API Rate Limits

- CDS API: Reasonable use expected (avoid >100 requests/hour)
- File size: ~10-50 MB per day depending on parameters
- Processing time: 1-5 minutes per request

## Validation

CAMS data is regularly validated against ground measurements:
- Typical R² with ground stations: 0.7-0.85
- Better performance in winter (less humidity interference)
- Slight underestimation during extreme events

## References

- [CAMS Documentation](https://atmosphere.copernicus.eu/data)
- [CDS API Guide](https://ads.atmosphere.copernicus.eu/api-how-to)
- [EAC4 Reanalysis Details](https://www.ecmwf.int/en/elibrary/19877-cams-reanalysis-atmospheric-composition)