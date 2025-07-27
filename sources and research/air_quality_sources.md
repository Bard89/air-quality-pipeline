# Granular Air Quality Datasets for Asian ML Projects

## Executive Summary

This comprehensive research identifies 50+ datasets and platforms providing granular air quality data for machine learning projects in Asia, with particular focus on high-pollution countries like China, India, Vietnam, and Indonesia. The analysis covers government monitoring networks, academic repositories, real-time APIs, satellite platforms, and regional initiatives, evaluating each for ML suitability based on data quality, temporal/spatial resolution, and accessibility.

## Top-Tier Datasets for Immediate ML Implementation

### 1. CHAP (ChinaHighAirPollutants) Dataset Series
**Best overall for China** - Comprehensive multi-pollutant coverage at unprecedented resolution
- **Parameters**: PM1, PM2.5, PM10, O3, NO2, SO2, CO, chemical composition (SO4²⁻, NO3⁻, NH4⁺, Cl⁻, BC, OM), plus 7 carcinogenic PAHs
- **Resolution**: 1km spatial, daily temporal (2000-present)
- **Access**: Free via Zenodo, GitHub, Chinese data centers
- **ML Advantages**: 400+ peer-reviewed papers, validated ground measurements, consistent methodology
- **Format**: NetCDF with Python/R/Matlab conversion codes

### 2. TAP (Tracking Air Pollution in China)
**Best for near real-time China data** - Machine learning fusion of multiple sources
- **Parameters**: Full-coverage PM2.5 with chemical composition
- **Resolution**: 10km spatial, daily updates
- **Coverage**: Complete China territory since 2000
- **ML Performance**: R² of 0.80-0.88 for PM2.5 estimates
- **Access**: tapdata.org.cn (free for research)

### 3. OpenAQ API
**Best open-source real-time platform** - Comprehensive Asian government data aggregation
- **Parameters**: PM2.5, PM10, SO2, NO2, O3, CO, BC
- **Coverage**: 25,000+ stations globally, extensive Asia coverage
- **Temporal**: Real-time updates, hourly data, historical from 2016
- **Access**: RESTful JSON API, bulk S3 downloads
- **ML Features**: Consistent format, geospatial queries, averaging capabilities
- **Cost**: Completely free

### 4. WUSTL Global PM2.5 Dataset
**Best for multi-country studies** - Satellite-derived with ground calibration
- **Parameters**: PM2.5 total and compositional mass
- **Resolution**: Global coverage including all Asian countries
- **Temporal**: Annual/monthly (1998-2023)
- **ML Validation**: Combines satellite AOD with GEOS-Chem, calibrated to ground observations
- **Access**: sites.wustl.edu/acag/datasets/

## High-Performance APIs for Real-Time ML Applications

### Google Air Quality API
**Enterprise-grade reliability**
- **Resolution**: 500x500m globally
- **Features**: 70+ AQ indexes, health recommendations, 96-hour forecasts
- **Coverage**: 100+ countries including major Asian nations
- **ML Integration**: Google Cloud ecosystem, scalable infrastructure
- **Pricing**: Pay-per-use model

### BreezoMeter API
**Hyperlocal precision**
- **Resolution**: Down to 10m in major cities, 500x500m standard
- **ML Enhancement**: Proprietary models combining satellite, traffic, weather
- **Features**: Multiple AQI scales, demographic-specific health recommendations
- **Coverage**: Global with excellent Asian urban coverage

### PurpleAir API
**Highest temporal resolution**
- **Updates**: 2-minute intervals
- **Parameters**: PM1.0, PM2.5, PM10, temperature, humidity
- **ML Advantage**: Dual-channel validation, extensive historical data (2016+)
- **Access**: Free with API key
- **Coverage**: Community-driven, concentrated in urban Asia

## Satellite Platforms for Comprehensive Coverage

### Sentinel-5P TROPOMI
**Best current resolution for trace gases**
- **Parameters**: NO2, SO2, CO, O3, HCHO, CH4, aerosols
- **Resolution**: 5.5km x 3.5km (improved from 7km x 3.5km)
- **Coverage**: Daily global, 13:30 local time
- **ML Applications**: Urban emission monitoring, trend analysis
- **Validation**: R² 0.7-0.8 for NO2 vs Asian ground stations

### Korean GEMS
**Revolutionary hourly monitoring**
- **Parameters**: NO2, SO2, O3, HCHO, CHOCHO, aerosols
- **Resolution**: ~3.5km x 8km over Seoul
- **Coverage**: Asia (5°S-45°N, 75°E-145°E)
- **Unique Feature**: Up to 10 observations/day for diurnal patterns
- **ML Applications**: Pollution episode analysis, emission verification

### MODIS Aerosol Products
**Foundation for PM2.5 estimation**
- **Resolution**: 1km (MAIAC), 3km, 10km products
- **Parameters**: AOD as PM2.5/PM10 proxy
- **ML Performance**: 0.6-0.8 correlation with surface PM2.5 in Asia
- **Access**: NASA Worldview, Giovanni, Google Earth Engine

## Government Monitoring Networks by Country

### China
- **Network Size**: 1,600+ stations, 113 major cities
- **Best Access**: TAP database for ML applications
- **Temporal**: Hourly updates, historical since 2000
- **Parameters**: PM2.5, PM10, SO2, NO2, O3, CO

### India
- **SAFAR System**: Research-grade data, 15-minute resolution
- **CPCB Network**: Large coverage but variable quality
- **API Access**: APISetu platform
- **Coverage**: Major cities with expanding network

### Vietnam
- **Current Status**: Limited but rapidly expanding
- **Future**: Complete national system by 2025
- **Access**: CEM portal, AQICN integration
- **ML Limitation**: Limited historical data currently

### Indonesia
- **Challenges**: Maintenance issues limit continuity
- **Coverage**: 56 monitoring posts, 12 additional planned
- **Best Access**: Through IQAir platform

### Thailand
- **PCD Network**: Good real-time access
- **API**: Available through AQICN
- **Features**: Mobile monitoring stations
- **Apps**: Air4Thai for public access

## Academic and Research Datasets

### LGHAP Dataset
**Gap-free China coverage**
- **Parameters**: AOD, PM2.5, PM10
- **Resolution**: 1km, daily (2000-2020)
- **ML Feature**: Machine learning regression with sensor integration
- **Access**: Zenodo record 5655797

### Beijing Multi-Site Dataset
**ML competition favorite**
- **Parameters**: 6 pollutants + 6 meteorological variables
- **Sites**: 12 monitoring locations
- **Temporal**: Hourly data
- **Access**: UCI Repository, Kaggle

### Vietnam PM2.5 Dataset
**First comprehensive Vietnam coverage**
- **Period**: 2012-2020
- **Validation**: R²=0.75, RMSE=11.76 μg/m³
- **Method**: Mixed effect model with satellite data

## Regional Networks and Initiatives

### EANET
**Most stable regional network**
- **Countries**: 13 East Asian nations
- **Sites**: 59 monitoring locations
- **Expansion**: Now includes PM2.5, O3, VOCs (2021+)
- **Quality**: Annual inter-laboratory comparisons
- **Sustainability**: Government backing, UNEP support

### ASEAN Networks
- **ASMC**: Transboundary haze monitoring
- **Coverage**: All 10 ASEAN countries
- **Focus**: PM10, real-time alerts
- **Integration**: National monitoring systems

### Discontinued: US Embassy Monitoring
- **Status**: Suspended March 2025
- **Impact**: Loss of 89 stations in 61 countries
- **Legacy**: Historical data valuable for ML training
- **Alternative**: Data still available through WAQI integration

## ML Implementation Recommendations

### Tiered Data Strategy

**Foundation Layer**
1. CHAP/TAP for China (most comprehensive)
2. WUSTL PM2.5 for multi-country baselines
3. OpenAQ for real-time updates

**Enhancement Layer**
1. Sentinel-5P for trace gases
2. MODIS MAIAC for high-resolution AOD
3. PurpleAir for temporal granularity
4. GEMS for East Asian diurnal patterns

**Gap-Filling Layer**
1. BreezoMeter/Google AQ for sparse areas
2. MERRA-2/CAMS with bias correction
3. Regional research datasets

### Data Integration Best Practices

**Quality Hierarchy**
1. Government reference monitors (ground truth)
2. Research-grade networks (EANET, SAFAR)
3. Satellite with ground calibration
4. ML-enhanced products
5. Community sensors (with validation)

**Preprocessing Requirements**
- Meteorological normalization for satellite data
- Bias correction for reanalysis products
- Quality control for community sensors
- Temporal alignment across sources
- Missing data imputation strategies

### Access Optimization

**For Prototyping**
- Google Earth Engine (integrated satellite data)
- OpenAQ API (standardized ground data)
- Kaggle datasets (preprocessed, competition-ready)

**For Production**
- Direct API connections (reliability)
- Bulk downloads for historical training
- Cloud storage for large satellite datasets
- Version control for reproducibility

### Platform-Specific Considerations

**High-Pollution Episodes**
- Combine GEMS hourly + ground monitors
- Use ASMC for transboundary haze
- Integrate fire detection satellites

**Long-Term Trends**
- CHAP/TAP for China (20+ years)
- EANET for regional consistency
- MODIS for pre-2013 periods

**Urban Applications**
- BreezoMeter for street-level
- PurpleAir for neighborhood variation
- Government monitors for validation

## Cost and Licensing Summary

### Free/Open Access
- OpenAQ, CHAP, TAP, WUSTL datasets
- NASA/ESA satellite products
- Most government data (varies by country)
- EANET (with registration)

### Commercial/Paid
- Google Air Quality API (usage-based)
- BreezoMeter (subscription tiers)
- IQAir API (premium features)
- Some processed satellite products

### Academic Licensing
- Most research datasets freely available
- Citation requirements
- Some require collaboration agreements
- Special pricing for academic use (commercial APIs)

## Future Developments

### Upcoming Enhancements
- Vietnam national network (2025)
- EANET API development
- GOSAT-GW with NO2 (2024)
- Improved satellite algorithms

### Emerging Technologies
- AI-enhanced gap filling
- IoT sensor standardization
- Blockchain for data validation
- Edge computing for real-time ML

This comprehensive dataset landscape provides excellent opportunities for developing robust ML models for air quality prediction in Asia, with particular strength in China and growing capabilities across Southeast Asia and India.