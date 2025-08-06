# Comprehensive Air Pollution Enhancement Plan with Terrain Analysis

## Executive Summary

This plan outlines the implementation of advanced data sources to significantly improve air pollution prediction accuracy. The enhancements focus on three critical areas:
1. **Transboundary Transport**: Modeling pollution movement across borders (e.g., China to Japan)
2. **Terrain Effects**: Accounting for how mountains and valleys affect pollution accumulation
3. **Emission Sources**: Integrating fire detection, industrial emissions, and satellite data

Expected impact: Increase prediction accuracy from R² ~0.70 to ~0.90, with 24-48 hour advance warning for pollution episodes.

## Why These Enhancements Matter

### Transboundary Transport
- 50-70% of PM2.5 in Japan/Korea originates from China during winter
- Transport time: 1-3 days depending on altitude and season
- Current models miss these long-range events

### Terrain Effects
- Valleys trap pollution, increasing concentrations 2-5x
- Mountain barriers block or channel pollution
- Temperature inversions are 5-15°C stronger in valleys
- Critical for cities like Tokyo, Seoul, Beijing in basins

### Missing Emission Sources
- Fire emissions cause PM2.5 spikes up to 10x normal
- Industrial emissions are primary urban pollution drivers
- Satellite data fills gaps between ground stations

## Phase 1: High-Impact Quick Wins (Week 1-2)

### 1. NASA FIRMS Fire Detection (IMMEDIATE IMPACT)
**Why first**: Fire emissions cause sudden PM2.5 spikes up to 10x normal levels
- **Implementation**: New plugin `src/plugins/firms/`
- **API**: Free with registration at https://firms.modaps.eosdis.nasa.gov/
- **Data**: Real-time fire detections within 3 hours
- **Coverage**: Global MODIS (1km) and VIIRS (375m) resolution
- **Features**: Fire Radiative Power for emission estimation

### 2. ERA5 Planetary Boundary Layer Height
**Why critical**: PBL height inversely correlates with pollution (PM2.5 = A/PBL_height + B)
- **Implementation**: Extend existing ERA5 plugin with `boundary_layer_height` variable
- **API**: Already have CDS access from weather module
- **Impact**: Low PBL (<500m) can increase pollution 2-5x
- **Data**: Hourly, 0.25° resolution

### 3. Upwind Station Monitoring (NEW)
**Why essential**: Early warning for transboundary pollution
- **Implementation**: New module in `src/plugins/transport/upwind/`
- **Data**: Identify and monitor stations 500-1500km upwind
- **Method**: Apply 24-48 hour lag correlation based on wind speed
- **Example**: Monitor Shanghai, Seoul for Tokyo pollution

### 4. Terrain Analysis Module (CRITICAL)
**Why essential**: Valleys can trap pollution for days, increasing concentrations 2-5x
- **Implementation**: New plugin `src/plugins/terrain/`
- **Data Source**: SRTM 30m or ASTER GDEM via `elevation` Python package
- **Calculations**:
  - Elevation at each monitoring station
  - Terrain Ruggedness Index (TRI) - identifies valleys/basins
  - Slope and aspect - for drainage flow modeling
  - Sky View Factor - measures valley enclosure
  - Valley depth index - elevation difference from ridgelines

## Phase 2: Transport & Emission Sources (Week 3-4)

### 5. HYSPLIT Backward Trajectories
**Why valuable**: Identifies pollution source regions and transport pathways
- **Implementation**: New plugin `src/plugins/transport/hysplit/`
- **Data**: 96-hour backward trajectories at multiple altitudes
- **Libraries**: HyTraj or PySPLIT Python packages
- **Output**: Trajectory clusters showing common pollution pathways
- **Enhancement**: Include terrain-following trajectories

### 6. CAMS Global Reanalysis (EAC4)
**Why powerful**: Provides 3D chemical transport fields
- **Implementation**: New plugin `src/plugins/transport/cams/`
- **Access**: Copernicus Atmosphere Data Store API
- **Data**: 3-hourly PM2.5, NO2, SO2, dust at 80km resolution
- **Coverage**: Global coverage since 2003

### 7. Sentinel-5P TROPOMI Satellite Data
**Why important**: Captures pollution between ground stations
- **Implementation**: New plugin `src/plugins/sentinel5p/` using Google Earth Engine
- **Data**: Daily NO₂, SO₂, CO at 5.5×3.5 km resolution
- **Coverage**: Global with 3-5 day lag
- **Access**: Free through GEE (requires registration)

## Phase 3: Advanced Features (Week 5-6)

### 8. Industrial Emissions (CEMS for China/India)
**Why valuable**: Direct emission sources are primary pollution drivers
- **Implementation**: New plugin `src/plugins/cems/`
- **Data**: Chinese Industrial Emissions Database (10,933+ plants)
- **Access**: Figshare/provincial portals
- **Parameters**: Hourly SO₂, NOₓ, PM from stack monitors

### 9. Urban Form & Natural Sources
**Why useful**: Street canyons trap pollution, dust storms add PM
- **Implementation**: New plugins for urban morphology and dust
- **Data**: OpenStreetMap buildings, WMO dust forecasts
- **Features**: Sky View Factor, dust storm warnings

## Technical Architecture

```
src/plugins/
├── terrain/                # Topography analysis
│   ├── elevation_fetcher.py    # Download SRTM/ASTER data
│   ├── terrain_calculator.py   # Calculate terrain metrics
│   ├── valley_detector.py      # Identify valleys and basins
│   └── flow_analyzer.py        # Analyze drainage flows
├── transport/              # Transboundary transport
│   ├── upwind/            # Upwind station monitoring
│   │   ├── station_selector.py
│   │   └── lag_correlator.py
│   ├── hysplit/           # Trajectory analysis
│   │   ├── trajectory.py
│   │   └── cluster.py
│   └── cams/              # Chemical transport
│       ├── datasource.py
│       └── transport_analyzer.py
├── firms/                 # Fire detection
│   ├── datasource.py      # NASA FIRMS API client
│   └── processor.py       # Fire to emission conversion
├── sentinel5p/            # Satellite pollution
│   ├── datasource.py      # Google Earth Engine client
│   └── processor.py       # NO2/SO2 extraction
└── cems/                  # Industrial emissions
    └── datasource.py      # CEMS data fetcher

domain/models.py updates:
- Add TerrainFeatures, TransportEvent, FireEvent models
- Add new parameters: tri, valley_depth, sky_view_factor, 
  fire_radiative_power, boundary_layer_height, transport_flux
```

## Key Terrain Calculations

### 1. Terrain Ruggedness Index (TRI)
```
TRI = sqrt(Σ(elevation_center - elevation_neighbor)²) / 8
Values > 80m indicate rough terrain
```

### 2. Valley Depth Index
```
VDI = elevation_ridgeline - elevation_station
Values > 200m indicate deep valleys
```

### 3. Sky View Factor (SVF)
```
SVF = visible_sky_area / total_hemisphere
Values < 0.7 indicate enclosed valleys
```

### 4. Pollution Amplification Factor
```
PAF = (1 / SVF) × (valley_depth / 100m) × stability_index
Valleys with PAF > 3 are pollution hotspots
```

## ML Feature Engineering

### Transport Features
- **Transport Index**: Upwind concentration × wind speed × stability
- **Source Attribution**: % local vs % transported based on trajectories
- **Chemical Age**: Ratio of secondary to primary pollutants
- **Transport Height**: Weighted average altitude of trajectories

### Terrain Features
- **Ventilation Index**: wind_speed × effective_PBL_height × (1 - valley_confinement)
- **Drainage Risk**: slope × cos(wind_direction - aspect) × stability
- **Terrain-Modified PBL**: PBL_height × (1 - 0.5 × valley_confinement)

### Fire Features
- **Fire Proximity Index**: distance × FRP × wind_factor
- **Smoke Age**: Time since fire detection
- **Fire Intensity Class**: Based on Fire Radiative Power

## Expected Impact by Feature

### Fire Detection (FIRMS)
- **Current**: Missing 80% of biomass burning events
- **Enhanced**: Capture 95% of fire-related PM spikes
- **Lead time**: 3-6 hours for smoke plume arrival

### Transboundary Transport
- **Current**: No advance warning for long-range transport
- **Enhanced**: 24-48 hour forecast for incoming pollution
- **Attribution**: Quantify local (30%) vs transported (70%)

### Terrain Effects
- **Valley cities**: R² improvement from 0.65 to 0.92
- **Inversions**: Predict 90% of trapped pollution events
- **Mountain passes**: Track concentrated transport corridors

### Combined System
- **Overall accuracy**: R² from ~0.70 to ~0.90
- **Extreme events**: Capture 95% (up from 60%)
- **Operational value**: Automated terrain-aware forecasts

## Implementation Priority

1. **Terrain Analysis**: One-time calculation, permanent value
2. **NASA FIRMS**: Immediate impact on extreme events
3. **ERA5 PBL + Terrain**: Fundamental for dispersion
4. **Upwind Monitoring**: Quick win for transport
5. **HYSPLIT**: Essential for source identification
6. **CAMS/Sentinel-5P**: Comprehensive coverage
7. **CEMS/Urban Form**: Local refinements

## Required Python Libraries

```python
# requirements.txt additions
elevation>=1.1.3      # SRTM/ASTER download
rasterio>=1.3.0       # Raster processing
richdem>=2.3.0        # Terrain analysis
xarray>=2023.0        # Multi-dimensional arrays
scikit-image>=0.19    # Sky view factor calculation
hytraj>=0.1.0         # HYSPLIT trajectories
sentinelsat>=1.2.0    # Sentinel-5P access
earthengine-api>=0.1  # Google Earth Engine
```

## Data Storage Requirements

- Terrain data: ~5GB (one-time, covers all Asia)
- Fire data: ~100MB/day
- Transport trajectories: ~500MB/day
- Satellite data: ~1GB/day
- Total: ~50GB/month additional storage

## Performance Metrics

### Processing Time
- Terrain analysis: 30 minutes (one-time per station)
- Fire detection: Real-time API calls
- Trajectory calculation: 5 minutes per station per day
- Satellite processing: 20 minutes per day

### Accuracy Improvements
- Hourly predictions: +25% accuracy
- Daily averages: +30% accuracy
- Pollution episodes: +50% detection rate
- False alarms: -40% reduction

## Risk Mitigation

1. **API Availability**: Cache data locally, implement retry logic
2. **Processing Load**: Use parallel processing, optimize algorithms
3. **Data Gaps**: Multiple data sources for redundancy
4. **Terrain Complexity**: Start with simple metrics, refine gradually

## Success Criteria

- [ ] 90% of high pollution episodes predicted 24h in advance
- [ ] R² > 0.85 for daily PM2.5 predictions
- [ ] Operational system running automatically
- [ ] Clear attribution of local vs transported pollution
- [ ] Terrain effects quantified for all stations

This comprehensive enhancement plan transforms the air quality monitoring system into a state-of-the-art prediction platform capable of protecting public health through accurate, timely forecasts.

## Parallel Worker Plan

### Worker 1: Terrain & Local Effects
**Focus**: Ground-based analysis, topography, and local pollution factors

**Tasks**:
1. **Terrain Analysis Module** [CRITICAL]
   - Create `src/plugins/terrain/` plugin
   - Implement elevation fetching, TRI, valley depth, sky view factor
   - One-time calculation for all monitoring stations

2. **CAMS Chemical Transport**
   - Create `src/plugins/transport/cams/`
   - Integrate Copernicus atmospheric data
   - Download 3D chemical transport fields

3. **Industrial Emissions (CEMS)**
   - Create `src/plugins/cems/`
   - Research and implement data sources for China/India
   - Real-time stack monitoring data

4. **Urban Form Analysis**
   - Create `src/plugins/urban/`
   - Calculate street canyon effects
   - Use OpenStreetMap building data

---

### Worker 2: Transport & Remote Sensing
**Focus**: Transboundary transport, trajectories, and satellite data

**Tasks**:
1. **Upwind Monitoring System** [CRITICAL]
   - Create `src/plugins/transport/upwind/`
   - Identify and monitor stations 500-1500km upwind
   - Implement lag correlation analysis

2. **HYSPLIT Trajectory Analysis**
   - Create `src/plugins/transport/hysplit/`
   - Implement 96-hour backward trajectories
   - Add trajectory clustering for source identification

3. **Sentinel-5P Satellite Integration**
   - Create `src/plugins/sentinel5p/`
   - Set up Google Earth Engine access
   - Download daily NO2, SO2, CO data

4. **Natural Dust Sources**
   - Create `src/plugins/dust/`
   - Integrate dust storm forecasts
   - Implement early warning system
