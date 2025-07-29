# ERA5 Planetary Boundary Layer Height

## What is PBL Height?

The Planetary Boundary Layer (PBL) is the lowest part of the atmosphere that is directly influenced by Earth's surface. Its height determines the vertical space available for pollution mixing.

**Key concept**: PBL height acts like an invisible ceiling that traps pollution. When it's low, pollution concentrates near the ground. When it's high, pollution disperses vertically.

**Shortcut/Abbreviation**: 
- PBL = Planetary Boundary Layer
- BLH = Boundary Layer Height (used in ERA5 data files)
- PBLH = PBL Height (common in literature)

**Impact on Air Pollution**:
- **Low PBL (<500m)**: Pollution trapped, concentrations increase 2-5x
- **Medium PBL (500-1000m)**: Moderate mixing, typical urban pollution levels
- **High PBL (>1000m)**: Good vertical mixing, pollution diluted effectively

**Simple formula**: `PM2.5 concentration ≈ Emissions / (PBL_height × Wind_speed)`

## Coverage

### Spatial Coverage
- **Current implementation**: Japan (JP)
- **Available**: Global coverage
- **Resolution**: 0.25° × 0.25° (approximately 28km)
- **What this means**: One data point covers roughly a city district
- **Pollution relevance**: Good for regional patterns, may miss local hotspots

### Temporal Coverage
- **Period**: 1940 to present (5-day lag)
- **Frequency**: Hourly data
- **What this means**: Can track PBL evolution through entire pollution episodes
- **Best practice**: Download 2-3 days before and after pollution events for context

## Parameters

### boundary_layer_height
- **Variable name in files**: `blh` (Boundary Layer Height)
- **Units**: meters above ground level
- **Range**: 10m to 4000m
- **Accuracy**: ±20% (better in daytime, worse at night)
- **What it represents**: Height where air stops mixing with surface

## Quick Start

```bash
# Demo mode (no API key required)
python scripts/download_era5_pbl.py --country JP --start 2024-01-01 --end 2024-01-31
```

## Setup for Real Data

### 1. Register for CDS Access

1. Create account at: https://cds.climate.copernicus.eu/user/register
2. Login and go to: https://cds.climate.copernicus.eu/api-how-to
3. Copy your UID and API key
4. Accept the license at: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=download

### 2. Configure CDS API

Add to `.env`:
```
CDS_API_KEY=YOUR_UID:YOUR_API_KEY
```

Or create `~/.cdsapirc`:
```bash
cat > ~/.cdsapirc << EOF
url: https://cds.climate.copernicus.eu/api/v2
key: YOUR_UID:YOUR_API_KEY
EOF

# Secure the file with restricted permissions
chmod 600 ~/.cdsapirc
```

### 3. Download Historical Data

```bash
# Download specific months
python scripts/download_era5_historical.py --start-year 2023 --end-year 2024 --start-month 1 --end-month 12

# Convert NetCDF to CSV
python scripts/convert_era5_to_csv.py data/era5/raw/era5_pbl_2023_01.nc

# Convert all files in directory
python scripts/convert_era5_to_csv.py data/era5/raw/
```

## Output Format

CSV with timestamp, location, and PBL height in meters.

## PBL Height Characteristics

### Typical Range: 200-2000m
- **What it means**: The invisible ceiling varies from just above buildings (200m) to cloud level (2000m)
- **Pollution impact**: At 200m, pollution is 10x more concentrated than at 2000m
- **Real example**: Tokyo winter mornings often have 200m PBL, causing visible smog

### Diurnal Cycle (Daily Pattern)
- **Night/Early Morning (200-500m)**: 
  - Surface cools → stable air → shallow PBL
  - Pollution accumulates overnight
  - Worst air quality typically 6-9 AM
- **Afternoon (1000-2000m)**:
  - Sun heats surface → convection → deep PBL
  - Pollution dilutes vertically
  - Best air quality typically 2-5 PM

### Seasonal Variation
- **Winter**: 
  - Average PBL: 300-800m
  - Strong temperature inversions trap pollution
  - PM2.5 episodes most frequent
- **Summer**: 
  - Average PBL: 800-1500m
  - Strong convection disperses pollution
  - Better air quality despite higher emissions

### Weather Effects
- **Stable conditions (High pressure, clear nights)**:
  - PBL: 100-400m
  - Pollution accumulation risk: HIGH
  - Common in: Winter anticyclones
- **Unstable conditions (Low pressure, windy)**:
  - PBL: 1000-3000m
  - Pollution dispersal: GOOD
  - Common in: Storm systems

## Relationship to Air Pollution

### The Core Equation
```
PM2.5 = Emissions / (PBL_height × Ventilation) + Background
```

**What each term means**:
- **Emissions**: Pollution sources (traffic, industry, fires)
- **PBL_height**: Vertical mixing space (our ERA5 data)
- **Ventilation**: Horizontal wind that carries pollution away
- **Background**: Regional pollution level

### Practical Impact Thresholds

#### Critical PBL < 300m
- **What happens**: Pollution trapped below skyscrapers
- **PM2.5 multiplication**: 3-5x normal levels
- **Visibility**: <5km, hazy conditions
- **Health alert**: Sensitive groups should stay indoors
- **Common scenario**: Winter morning rush hour

#### Moderate PBL 300-700m
- **What happens**: Pollution mixes to aircraft landing height
- **PM2.5 multiplication**: 1.5-2x normal levels
- **Visibility**: 5-10km, slight haze
- **Health impact**: Outdoor exercise discouraged
- **Common scenario**: Cool autumn days

#### Good PBL > 1000m
- **What happens**: Pollution disperses into cumulus cloud layer
- **PM2.5 levels**: Near background concentrations
- **Visibility**: >15km, clear skies
- **Health impact**: Normal outdoor activities OK
- **Common scenario**: Summer afternoons

## Integration with Air Quality Models

### 1. Ventilation Index (VI)
```
VI = PBL_height × wind_speed
```
- **What it measures**: Atmosphere's ability to flush out pollution
- **Good VI (>6000 m²/s)**: Pollution disperses quickly
- **Poor VI (<1000 m²/s)**: Pollution accumulates
- **Example**: PBL=200m × Wind=2m/s = VI=400 (very poor)

### 2. Mixing Volume Estimation
```
Mixing_Volume = PBL_height × Area
```
- **What it calculates**: Total air volume available for dilution
- **Use case**: Estimating pollution concentration from known emissions
- **Example**: Factory emits 100kg/hr into 500m PBL → higher concentration than 1500m PBL

### 3. Temperature Inversion Detection
- **Strong inversion**: PBL < 200m with sharp temperature increase above
- **Pollution impact**: Acts like a lid, zero vertical mixing
- **Detection**: Sudden PBL drop + stable conditions = inversion alert

### 4. Episode Prediction Rules
- **High risk**: PBL < 500m for >12 hours + Wind < 2m/s
- **Moderate risk**: PBL < 800m + Previous day PM2.5 > 35µg/m³
- **Low risk**: PBL > 1200m or Wind > 5m/s

## Data Download Options

### Web Interface (Small amounts)
- Go to https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels
- Select "Boundary layer height" and your dates
- Download directly from browser

### Python API (Large amounts)
```python
import cdsapi
c = cdsapi.Client()

c.retrieve(
    'reanalysis-era5-single-levels',
    {
        'product_type': 'reanalysis',
        'variable': 'boundary_layer_height',
        'year': '2024',
        'month': ['01', '02', '03'],
        'day': [str(d).zfill(2) for d in range(1, 32)],
        'time': [f'{h:02d}:00' for h in range(24)],
        'area': [46, 122, 24, 146],  # Japan
        'format': 'netcdf',
    },
    'era5_pbl_2024_q1.nc'
)
```

### Geographic Areas
- Japan: `[46, 122, 24, 146]`  # North, West, South, East
- Korea: `[39, 124, 33, 132]`
- China: `[54, 73, 18, 135]`

## Data Characteristics

- **File Size**: ~500MB per month for Japan region
- **Download Speed**: 10-60 minutes per month depending on queue
- **Alternative**: ERA5-Land for 0.1° resolution (from 1950)

## Tips

1. **Check queue status**: https://cds.climate.copernicus.eu/live/queue
2. **Download in parallel**: Run multiple scripts for different months
3. **Retry failed downloads**: The script checks if files exist
4. **Use GRIB format**: Smaller file size but needs `cfgrib` library