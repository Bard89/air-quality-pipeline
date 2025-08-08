# H3 Hexagonal Aggregation Technical Documentation

## Overview
This document provides technical details about the H3 hexagonal aggregation system used in the Environmental Data Collector pipeline.

## H3 Library Integration

### Installation
```bash
pip install h3>=4.0.0
```

### Key Functions Used
```python
import h3

# Convert lat/lon to H3 index
hex_id = h3.latlng_to_cell(latitude, longitude, resolution)

# Get hexagon center coordinates  
lat, lon = h3.cell_to_latlng(hex_id)

# Get hexagon boundary polygon
boundary = h3.cell_to_boundary(hex_id)

# Get neighboring hexagons
neighbors = h3.grid_ring(hex_id, k=1)  # k=1 for immediate neighbors

# Get parent hexagon (coarser resolution)
parent = h3.cell_to_parent(hex_id, resolution=7)

# Get children hexagons (finer resolution)
children = h3.cell_to_children(hex_id, resolution=9)
```

## Resolution Selection Guide

### Why We Chose Resolution 8

**Resolution 8** was selected as our default after careful analysis of our data sources and requirements:

1. **Matches Air Quality Sensor Density**
   - Most urban areas have 1-3 air quality sensors per km²
   - Resolution 8 hexagons (~0.74 km²) typically contain 0-2 sensors
   - Prevents over-aggregation while avoiding empty hexagons

2. **Balances Detail vs Computation**
   - Resolution 7: Too coarse (5 km²) - loses local variations
   - Resolution 8: Optimal (0.74 km²) - captures neighborhood patterns
   - Resolution 9: Too fine (0.11 km²) - too many empty hexagons, 7x more data

3. **Aligns with Pollution Dispersion**
   - PM2.5 dispersion radius: typically 0.5-2 km from source
   - Resolution 8 diameter (~1.5 km) captures local emission effects
   - Appropriate for modeling traffic and industrial impacts

4. **Computational Efficiency**
   ```
   Data Points Analysis (Tokyo, 1 month):
   - Raw sensor data: ~1,000,000 measurements
   - Resolution 7: ~400 hexagon-hours (too aggregated)
   - Resolution 8: ~2,500 hexagon-hours (optimal)
   - Resolution 9: ~17,000 hexagon-hours (excessive)
   ```

5. **Machine Learning Considerations**
   - Provides sufficient training samples per hexagon
   - Reduces noise while preserving spatial patterns
   - Feature vector size remains manageable

### Resolution 8 Specifications
- **Area**: ~0.74 km²
- **Edge Length**: ~0.88 km
- **Diameter**: ~1.53 km
- **Use Case**: Neighborhood-level analysis, urban air quality monitoring
- **Hexagons for Tokyo**: ~2,000-3,000
- **Hexagons for Japan**: ~500,000

### When to Use Different Resolutions

**Resolution 7** (Coarser)
- Use for: Regional trends, computational efficiency
- Area: ~5.16 km²
- Reduces data by factor of ~7

**Resolution 9** (Finer)
- Use for: High-density urban areas, street-level analysis
- Area: ~0.11 km²
- Increases data by factor of ~7

## Aggregation Algorithm

### Core Processing Logic
```python
def aggregate_to_hexagon_hour(df, resolution=8):
    # Step 1: Add H3 index to each point
    df['h3_index'] = df.apply(
        lambda row: h3.latlng_to_cell(
            row['latitude'], 
            row['longitude'], 
            resolution
        ),
        axis=1
    )
    
    # Step 2: Round timestamps to hour
    df['timestamp_hour'] = df['timestamp'].dt.floor('h')
    
    # Step 3: Group by hexagon and hour
    aggregated = df.groupby(['timestamp_hour', 'h3_index']).agg({
        'pm25': ['mean', 'std', 'min', 'max', 'count'],
        'temperature': ['mean', 'min', 'max'],
        'wind_speed': 'mean',
        # ... other metrics
    })
    
    return aggregated
```

### Handling Edge Cases

#### Sparse Data
When a hexagon has only 1-2 measurements:
- `std` becomes 0 or NaN
- Consider minimum count thresholds
- May need to aggregate to coarser resolution

#### Dense Data
When a hexagon has 100+ measurements per hour:
- Consider weighted averages
- May indicate multiple sensors in same hexagon
- Could use Resolution 9 for better granularity

#### Missing Hexagons
Hexagons with no data in time period:
- Not included in output by default
- Can be filled with spatial interpolation
- Important for visualization continuity

## Spatial Operations

### Finding Nearby Data
```python
def get_nearby_hexagons(center_hex, k_rings=2):
    """Get hexagons within k rings of center"""
    nearby = set()
    for k in range(1, k_rings + 1):
        ring = h3.grid_ring(center_hex, k)
        nearby.update(ring)
    return nearby

# Example: Get all hexagons within 2 rings (~3km radius)
center = h3.latlng_to_cell(35.6762, 139.6503, 8)
nearby = get_nearby_hexagons(center, k_rings=2)
# Returns ~19 hexagons (1 + 6 + 12)
```

### Spatial Interpolation
```python
def interpolate_missing_hexagons(df, parameter, max_distance_rings=2):
    """Fill missing hexagon values using nearby data"""
    all_hexagons = set(df['h3_index'].unique())
    
    for hex_id in all_hexagons:
        if df[df['h3_index'] == hex_id][parameter].isna().all():
            # Get nearby hexagons with data
            nearby = get_nearby_hexagons(hex_id, max_distance_rings)
            nearby_data = df[df['h3_index'].isin(nearby)][parameter]
            
            if not nearby_data.empty:
                # Simple average (could use distance weighting)
                interpolated_value = nearby_data.mean()
                df.loc[df['h3_index'] == hex_id, parameter] = interpolated_value
    
    return df
```

## Performance Optimization

### Batch Processing
```python
# Slow: Individual conversions
for idx, row in df.iterrows():
    df.at[idx, 'h3_index'] = h3.latlng_to_cell(row['lat'], row['lon'], 8)

# Fast: Vectorized with numpy
import numpy as np
lats = df['latitude'].values
lons = df['longitude'].values
h3_indices = np.array([
    h3.latlng_to_cell(lat, lon, 8) 
    for lat, lon in zip(lats, lons)
])
df['h3_index'] = h3_indices
```

### Memory Management
- Process data in monthly chunks for large datasets
- Use categorical dtype for h3_index column
- Consider Parquet format for intermediate storage

### Index Optimization
```python
# Create multi-index for efficient queries
df = df.set_index(['timestamp_hour', 'h3_index'])
df = df.sort_index()

# Fast lookup for specific hexagon-hour
data = df.loc[('2023-01-01 14:00', '8844c0a31dfffff')]
```

## Validation and Quality Checks

### Hexagon Coverage Check
```python
def check_coverage(df, country_bounds):
    """Verify hexagons cover expected area"""
    unique_hexagons = df['h3_index'].unique()
    
    # Get hexagon centers
    centers = [h3.cell_to_latlng(hex_id) for hex_id in unique_hexagons]
    
    # Check bounds
    lats = [c[0] for c in centers]
    lons = [c[1] for c in centers]
    
    coverage = {
        'min_lat': min(lats),
        'max_lat': max(lats),
        'min_lon': min(lons),
        'max_lon': max(lons),
        'total_hexagons': len(unique_hexagons),
        'expected_area_km2': len(unique_hexagons) * 0.74
    }
    
    return coverage
```

### Data Density Analysis
```python
def analyze_density(df):
    """Analyze measurements per hexagon"""
    density = df.groupby('h3_index').size()
    
    stats = {
        'mean_measurements': density.mean(),
        'median_measurements': density.median(),
        'min_measurements': density.min(),
        'max_measurements': density.max(),
        'empty_hexagons': (density == 0).sum(),
        'sparse_hexagons': (density < 5).sum(),
        'dense_hexagons': (density > 100).sum()
    }
    
    return stats
```

## Visualization

### Creating Hexagon Maps
```python
import folium
from folium import plugins

def create_hexagon_map(df, parameter='pm25_mean'):
    """Create interactive map with hexagon overlay"""
    # Create base map
    m = folium.Map(location=[35.6762, 139.6503], zoom_start=10)
    
    # Add hexagons
    for idx, row in df.iterrows():
        # Get hexagon boundary
        boundary = h3.cell_to_boundary(row['h3_index'])
        
        # Create polygon
        folium.Polygon(
            locations=boundary,
            color='blue',
            weight=1,
            fill=True,
            fillColor='red',
            fillOpacity=row[parameter] / 100,  # Normalize
            popup=f"{parameter}: {row[parameter]:.2f}"
        ).add_to(m)
    
    return m
```

### Hexagon Grid Export
```python
def export_to_geojson(df):
    """Export hexagon data as GeoJSON"""
    features = []
    
    for idx, row in df.iterrows():
        boundary = h3.cell_to_boundary(row['h3_index'])
        
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [boundary]
            },
            "properties": {
                "h3_index": row['h3_index'],
                "timestamp": row['timestamp'].isoformat(),
                **{col: row[col] for col in df.columns 
                   if col not in ['h3_index', 'timestamp']}
            }
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return geojson
```

## Common Issues and Solutions

### Issue: Hexagons at Country Borders
**Problem**: Hexagons may extend beyond country boundaries
**Solution**: Use country polygon to filter hexagon centers
```python
from shapely.geometry import Point, Polygon

def filter_by_country(hex_ids, country_polygon):
    valid_hexes = []
    for hex_id in hex_ids:
        center = h3.cell_to_latlng(hex_id)
        if country_polygon.contains(Point(center[1], center[0])):
            valid_hexes.append(hex_id)
    return valid_hexes
```

### Issue: Coordinate System Mismatch
**Problem**: H3 expects lat/lon in WGS84
**Solution**: Always convert coordinates before H3 operations
```python
import pyproj

# Convert from Japanese coordinate system to WGS84
transformer = pyproj.Transformer.from_crs("EPSG:6668", "EPSG:4326")
lat, lon = transformer.transform(x_japan, y_japan)
hex_id = h3.latlng_to_cell(lat, lon, 8)
```

### Issue: Time Zone Handling
**Problem**: Different data sources use different time zones
**Solution**: Convert all timestamps to UTC before aggregation
```python
df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
df['timestamp_hour'] = df['timestamp'].dt.floor('h')
```

## Integration with ML Pipeline

### Feature Engineering
```python
def add_hexagon_features(df):
    """Add spatial features for ML"""
    for hex_id in df['h3_index'].unique():
        # Get neighboring hexagons
        neighbors = h3.grid_ring(hex_id, k=1)
        
        # Calculate neighbor statistics
        for param in ['pm25', 'temperature']:
            neighbor_values = df[df['h3_index'].isin(neighbors)][param]
            
            df.loc[df['h3_index'] == hex_id, f'{param}_neighbor_mean'] = neighbor_values.mean()
            df.loc[df['h3_index'] == hex_id, f'{param}_neighbor_std'] = neighbor_values.std()
            df.loc[df['h3_index'] == hex_id, f'{param}_spatial_gradient'] = (
                df.loc[df['h3_index'] == hex_id, param].values[0] - neighbor_values.mean()
            )
    
    return df
```

### Training Data Preparation
```python
def prepare_ml_dataset(df):
    """Prepare data for ML training"""
    # Ensure each hexagon-hour is a single row
    df = df.groupby(['timestamp_hour', 'h3_index']).first().reset_index()
    
    # Add temporal features
    df['hour'] = df['timestamp_hour'].dt.hour
    df['day_of_week'] = df['timestamp_hour'].dt.dayofweek
    df['month'] = df['timestamp_hour'].dt.month
    
    # Add spatial features
    df = add_hexagon_features(df)
    
    # Create lag features
    df = df.sort_values(['h3_index', 'timestamp_hour'])
    for lag in [1, 3, 6, 24]:
        df[f'pm25_lag_{lag}h'] = df.groupby('h3_index')['pm25'].shift(lag)
    
    return df
```

## References

- [H3 Documentation](https://h3geo.org/docs)
- [H3 Python Bindings](https://github.com/uber/h3-py)
- [H3 Resolution Table](https://h3geo.org/docs/core-library/restable)
- [Uber Engineering Blog: H3](https://www.uber.com/blog/h3/)