# Traffic Data Collection Guide

## Overview

The traffic module collects vehicle flow data from the Japan Road Traffic Information Center (JARTIC), providing detailed traffic measurements from ~2,600 monitoring locations across Japan.

## Quick Start

```bash
# Step 1: Download archives for 2024
python download_jartic_archives.py --start 2024-01 --end 2024-12

# Step 2: Extract CSV files
python extract_jartic_csvs.py --all

# Step 3: Process into unified format
python process_extracted_csvs.py --start 2024-01-01 --end 2024-12-31
```

## Data Source: JARTIC

- **Coverage**: ~2,600 traffic monitoring locations across Japan
- **Temporal Resolution**: 5-minute intervals
- **Parameters**: Vehicle counts, average speeds, occupancy rates
- **Historical Data**: Archives available from 2019
- **Access**: Free, no API key required
- **Archive Size**: ~4GB per month (compressed)

## Download Process

### Step 1: Download Archives

```bash
python download_jartic_archives.py --start YYYY-MM --end YYYY-MM [OPTIONS]
```

Options:
- `--start, -s`: Start month (YYYY-MM format)
- `--end, -e`: End month (YYYY-MM format)
- `--cache-dir`: Directory for archives (default: data/jartic/cache)

Example:
```bash
# Download Q1 2024
python download_jartic_archives.py --start 2024-01 --end 2024-03

# Download full year
python download_jartic_archives.py --start 2024-01 --end 2024-12
```

Archives are saved to: `data/jartic/cache/jartic_typeB_YYYY_MM.zip`

### Step 2: Extract CSV Files

```bash
python extract_jartic_csvs.py [OPTIONS]
```

Options:
- `--archive, -a`: Extract specific archive
- `--all`: Extract all archives in cache
- `--output-dir, -o`: Output directory (default: data/jartic/extracted)
- `--no-convert`: Keep Shift-JIS encoding (default: convert to UTF-8)

Example:
```bash
# Extract specific month
python extract_jartic_csvs.py --archive data/jartic/cache/jartic_typeB_2024_02.zip

# Extract all downloaded archives
python extract_jartic_csvs.py --all
```

Files are extracted to: `data/jartic/extracted/YYYY_MM/`

### Step 3: Process Data

```bash
python process_extracted_csvs.py --start YYYY-MM-DD --end YYYY-MM-DD [OPTIONS]
```

Options:
- `--start, -s`: Start date
- `--end, -e`: End date
- `--input-dir, -i`: Directory with extracted CSVs
- `--month, -m`: Process specific month (e.g., 2024_02)

Example:
```bash
# Process February 2024
python process_extracted_csvs.py --start 2024-02-01 --end 2024-02-28

# Process full year
python process_extracted_csvs.py --start 2024-01-01 --end 2024-12-31
```

## Data Format

### Raw JARTIC Format
- Multiple CSV files per prefecture
- 5-minute interval measurements
- Shift-JIS encoding (Japanese characters)
- Various traffic parameters per location

### Processed Format
```csv
timestamp,location_id,location_name,latitude,longitude,parameter,value,unit,prefecture,data_source
2024-02-15T10:00:00+09:00,JARTIC_21001,国道1号線東京,0.0,0.0,traffic_volume,125,vehicles/5min,Tokyo,jartic
```

**Note**: Coordinates are currently placeholders (0,0). Actual location mapping is in development.

## Encoding Notes

JARTIC files use Shift-JIS encoding for Japanese text. The extraction process:
1. Automatically converts to UTF-8 by default
2. Use `--no-convert` to keep original encoding
3. Manual conversion available via `convert_jartic_encoding.py`

### Manual Encoding Conversion
```bash
# Convert directory to UTF-8
python convert_jartic_encoding.py --input-dir data/jartic/extracted/2024_02

# Convert in-place with backups
python convert_jartic_encoding.py --input-dir data/jartic/extracted/2024_02 --in-place
```

## Archive Structure

Each monthly archive contains:
```
jartic_typeB_YYYY_MM.zip
├── prefecture1/
│   ├── traffic_data_YYYYMMDD_HH.csv
│   └── ...
├── prefecture2/
│   └── ...
└── README.txt
```

## Performance Considerations

### Download Speed
- Archive downloads: ~50-100 MB/s (depends on connection)
- Each month: ~4GB compressed → ~20GB extracted
- Full year: ~48GB compressed → ~240GB extracted

### Processing Speed
- Extraction: ~5-10 minutes per month
- Processing: ~2-5 minutes per month
- Full year processing: ~1-2 hours

### Disk Space Requirements
- Per month: ~4GB (archive) + ~20GB (extracted)
- Full year: ~48GB (archives) + ~240GB (extracted)
- Processed output: ~10-20GB per year

## Common Use Cases

### Monthly Analysis
```bash
# Download and process single month
python download_jartic_archives.py --start 2024-03 --end 2024-03
python extract_jartic_csvs.py --archive data/jartic/cache/jartic_typeB_2024_03.zip
python process_extracted_csvs.py --start 2024-03-01 --end 2024-03-31
```

### Quarterly Reports
```bash
# Q1 2024
python download_jartic_archives.py --start 2024-01 --end 2024-03
python extract_jartic_csvs.py --all
python process_extracted_csvs.py --start 2024-01-01 --end 2024-03-31
```

### Annual Dataset
```bash
# Full year 2024
python download_jartic_archives.py --start 2024-01 --end 2024-12
python extract_jartic_csvs.py --all
python process_extracted_csvs.py --start 2024-01-01 --end 2024-12-31
```

## Data Quality Notes

1. **Coverage**: Urban areas have denser sensor networks
2. **Gaps**: Some rural areas may have limited coverage
3. **Maintenance**: Occasional sensor outages may create gaps
4. **Time Zone**: All timestamps are in JST (UTC+9)
5. **Coordinates**: Currently set to (0,0) - geocoding in development

## Integration with Other Data

Traffic data can be combined with:
- **Weather data**: Analyze traffic patterns during different weather conditions
- **Air quality**: Study relationship between traffic volume and pollution levels
- **Time series**: 5-minute resolution enables detailed temporal analysis

Example merge:
```python
# Merge traffic with weather data
traffic_df = pd.read_csv('jp_traffic_202402.csv')
weather_df = pd.read_csv('jp_jma_weather_20240201_to_20240229.csv')

# Resample weather to 5-minute intervals
# Merge on timestamp and location
```

## Troubleshooting

### Download Issues
- JARTIC servers may be slow during peak hours
- Use a stable internet connection for large downloads
- Archives are resumable if connection drops

### Extraction Errors
- Ensure sufficient disk space (20GB per month)
- Check for corrupted archives with `unzip -t`
- Re-download if archive is corrupted

### Encoding Problems
- Use `--no-convert` if UTF-8 conversion fails
- Try `convert_jartic_encoding.py` for manual conversion
- Some older files may have mixed encodings