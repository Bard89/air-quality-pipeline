# Fire Detection Data (NASA FIRMS)

Real-time and historical satellite fire detection data.

## Coverage

- **Countries**: JP, KR, CN, IN, TH, ID, MY, VN
- **Resolution**: MODIS (1km), VIIRS (375m)
- **Update frequency**: 3-6 hours for real-time
- **History**: 
  - Near Real-Time (NRT): Last 2 months with 3-hour latency
  - Archive: Historical data back to 2000 (MODIS) and 2012 (VIIRS)

## Parameters

- `fire_radiative_power`: Fire intensity in MW
- `fire_confidence`: Detection confidence (%)
- `fire_brightness`: Temperature in Kelvin

## Usage

```bash
# Download recent fire data (last 7 days)
python scripts/download_fire_data.py --country JP --days 7

# Download historical fire data
python scripts/download_fire_data.py --country IN --start 2024-01-01 --end 2024-01-31

# Download burning season data
python scripts/download_fire_data.py --country TH --start 2023-03-01 --end 2023-03-31
```

## API Registration

1. Register at https://firms.modaps.eosdis.nasa.gov/api/
2. Add `FIRMS_API_KEY` to `.env`

## Output Format

CSV with fire location, intensity, satellite, and timestamp.

## Fire Intensity Classes

- **Low**: < 10 MW
- **Moderate**: 10-50 MW  
- **High**: 50-100 MW
- **Very High**: 100-500 MW
- **Extreme**: > 500 MW

## Data Notes

- **API Limitation**: The FIRMS API only provides Near Real-Time (NRT) data for approximately the last 2 months
- **Historical data**: For data older than 2 months, use the FIRMS Archive Download tool (requires manual download)
- Maximum 10 days per API request (automatically chunked for longer periods)
- Archive data exists but requires different access method: MODIS (2000-present), VIIRS (2012-present)
- Large countries (IN, CN) are processed in regions to avoid timeouts
- Fire detection depends on cloud cover and satellite passes

## Historical Data Access

For data older than ~2 months:
1. Visit https://firms.modaps.eosdis.nasa.gov/download/
2. Select date range and area
3. Download as CSV/Shapefile
4. Process locally