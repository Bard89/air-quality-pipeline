# Elevation Grid

Downloads elevation data grid for Asian countries.

## Usage

```bash
# Download elevation grid
python scripts/download_elevation_grid.py --country JP

# Options
--country CODE           # Country code (required)
--resolution 0.02        # Grid resolution in degrees (default: 0.01 = ~1km)
--list-countries         # Show available countries
```

## Available Countries

- **JP**: Japan
- **KR**: South Korea  
- **CN**: China
- **IN**: India
- **TH**: Thailand
- **VN**: Vietnam
- **ID**: Indonesia
- **MY**: Malaysia
- **PH**: Philippines
- **TW**: Taiwan

## Output

Saves elevation grid to: `data/terrain/processed/{country}_elevation_grid.csv`

### CSV Format

| Column | Description |
|--------|-------------|
| latitude | Latitude coordinate |
| longitude | Longitude coordinate |
| elevation_m | Elevation in meters |
| country | Country code |
| grid_resolution_deg | Resolution in degrees |
| grid_resolution_km | Resolution in kilometers |

## File Sizes

- Japan: ~231MB (4.9M points)
- South Korea: ~5MB (90K points)
- China: ~600MB (15M points)
- India: ~300MB (8M points)

## Example

```bash
# Download Japan elevation grid
python scripts/download_elevation_grid.py --country JP

# Download Korea with 2km resolution
python scripts/download_elevation_grid.py --country KR --resolution 0.02
```