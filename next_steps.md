# Japan Traffic Data Integration Plan

**Created Date:** 2025-07-26  
**Current Branch:** add-traffic-data  
**Target Data Period:** 2023-2025 (to align with existing air quality data)  
**Last Updated:** 2025-07-27

## Overview
This document outlines the plan to integrate free traffic data sources for Japan (2023-2025) into the existing air quality data collection system.

## Implementation Status

### ✅ COMPLETED
- **Phase 1: Domain Model Extensions** - DONE
  - Added `TRAFFIC_VOLUME` and `VEHICLE_SPEED` to ParameterType enum
  - Added traffic-related measurement units
  
- **Phase 2: JARTIC Archive Plugin** - DONE
  - Created plugin structure in `src/plugins/jartic/`
  - Implemented `datasource.py` with DataSource interface
  - Implemented `archive_downloader.py` for downloading from compusophia.com
  - Implemented `data_parser.py` for parsing JARTIC CSV formats
  - Plugin registered in plugin system and accessible via CLI

### 🚧 IN PROGRESS
- **Phase 3: Data Integration Strategy**
  - Location matching between air quality and traffic stations
  - Combined data download capabilities
  - CSV output format for combined air quality + traffic data

### 📋 TODO
- **Phase 4: Full Integration**
  - Update checkpoint system for archive-based downloads
  - Create dedicated `download_traffic.py` script
  - Implement location mapping logic
  - Add combined data analysis capabilities
- **Phase 5: Additional Plugins**
  - JARTIC API plugin for recent data
  - ODPT transit data plugin

## Next Immediate Steps

1. **Complete Location Mapping** (Priority 1)
   - Implement proper coordinate extraction from JARTIC data
   - Create mapping between JARTIC station IDs and geographic coordinates
   - Store location data in a reusable format

2. **Enhance Checkpoint System** (Priority 2)
   - Add support for tracking downloaded archive files
   - Implement resume capability for partial archive downloads
   - Integrate with existing checkpoint infrastructure

3. **Create Combined Output** (Priority 3)
   - Design CSV schema that includes both air quality and traffic data
   - Implement location matching algorithm (find nearest traffic station to each air quality station)
   - Create combined data download workflow

4. **Build Dedicated Traffic CLI** (Priority 4)
   - Create `download_traffic.py` with traffic-specific options
   - Add support for different time aggregations (5-min vs hourly)
   - Implement batch download for date ranges

## Test Commands Available Now

The JARTIC plugin is integrated into the main CLI and can be tested with:

```bash
# Test JARTIC plugin functionality
pytest tests/test_jartic_plugin.py -v

# Download JARTIC archive data (uses existing CLI with --source jartic)
python download_air_quality.py --source jartic --country JP --start-date 2024-01-01 --end-date 2024-01-31

# View available locations from JARTIC
python download_air_quality.py --source jartic --country JP --list-locations

# Download specific JARTIC location data
python download_air_quality.py --source jartic --location "Tokyo-Shibuya" --start-date 2024-01-01
```

Note: The JARTIC plugin currently downloads archive files from compusophia.com and extracts traffic data. However, the following features still need implementation:
- Checkpoint support for resuming large archive downloads
- Location coordinate mapping (currently returns placeholder coordinates)
- Integration with air quality data for combined analysis
- CSV output formatting for traffic-specific parameters

## Available Free Traffic Data Sources

1. **Compusophia JARTIC Archives** (PRIMARY SOURCE)
   - Historical general road traffic data from 2023-2025
   - 5-minute and hourly intervals
   - Covers ~2,600 observation points nationwide
   - Free download of archived monthly zip files
   - URL: https://www.compusophia.com/en/notes/1

2. **JARTIC Open Traffic API** (SUPPLEMENTARY)
   - Real-time + 3 months historical data
   - Requires registration but free to use
   - Same observation points as archives
   - URL: https://www.jartic-open-traffic.org/

3. **ODPT Public Transit API** (PROXY DATA)
   - Transit ridership data (inversely correlates with road traffic)
   - Historical data availability unclear
   - Requires free developer registration
   - URL: https://www.odpt.org/

## Implementation Plan

### ✅ Phase 1: Domain Model Extensions (COMPLETED)

1. **Extended ParameterType enum** in `src/domain/models.py`:
   - ✅ Added `TRAFFIC_VOLUME = "traffic_volume"`
   - ✅ Added `VEHICLE_SPEED = "vehicle_speed"`
   - TODO: `OCCUPANCY_RATE = "occupancy_rate"`
   - TODO: `TRANSIT_RIDERSHIP = "transit_ridership"`

2. **Extended MeasurementUnit enum**:
   - ✅ Added traffic-related units
   - TODO: Verify all units are properly mapped

### ✅ Phase 2: Create Traffic Data Plugins (JARTIC COMPLETED)

1. **JARTIC Archive Plugin** (`src/plugins/jartic/`) - ✅ COMPLETED:
   - ✅ `datasource.py`: Implements DataSource interface
   - ✅ `archive_downloader.py`: Downloads and extracts monthly archives
   - ✅ `data_parser.py`: Parses JARTIC CSV formats
   - ✅ Downloads historical archives from compusophia.com
   - ✅ Parses traffic volume data (5-min/hourly)
   - 🚧 TODO: Proper location coordinate mapping
   - ✅ Handles missing data gracefully

2. **JARTIC API Plugin** (`src/plugins/jartic_api/`) - TODO:
   - `api_datasource.py`: Implements DataSource interface
   - Uses existing API client pattern
   - For recent data (last 3 months)

3. **ODPT Transit Plugin** (`src/plugins/odpt/`) - TODO:
   - `transit_datasource.py`: Implements DataSource interface
   - `gtfs_parser.py`: Parse GTFS format data
   - Maps transit stations to nearby traffic monitoring points

### Phase 3: Data Integration Strategy

1. **Location Matching**:
   - Create mapping between air quality stations and traffic monitoring points
   - Use geographic proximity (within 1-5km radius)
   - Store mappings in `config/location_mappings.json`

2. **Temporal Alignment**:
   - Aggregate 5-minute traffic data to hourly
   - Align timestamps with air quality measurements
   - Handle timezone conversions (JST to UTC)

3. **Data Storage**:
   - Extend CSV output to include traffic columns
   - Separate traffic data files with location cross-references
   - Update checkpoint system for traffic downloads

### Phase 4: Implementation Steps (UPDATED TIMELINE)

1. **✅ Setup & Configuration** (COMPLETED):
   - ✅ JARTIC archive access configured (no API key needed)
   - TODO: Register for JARTIC API access
   - TODO: Register for ODPT developer account
   - ✅ Configuration structure in place
   - TODO: Document additional API keys in `.env.example`

2. **✅ Core Extensions** (COMPLETED):
   - ✅ Extended domain models
   - ✅ Created JARTIC traffic plugin structure
   - ✅ Added traffic-specific error handling

3. **✅ JARTIC Archive Plugin** (COMPLETED):
   - ✅ Implemented archive downloader
   - ✅ Created data parser for JARTIC CSV formats
   - 🚧 TODO: Complete location coordinate mapping
   - ✅ Basic testing complete

4. **🚧 Data Integration** (CURRENT FOCUS - Week 1-2):
   - Implement proper location coordinate mapping
   - Update checkpoint system for archive downloads
   - Create combined CSV output format
   - Build location matching between air quality and traffic stations

5. **📋 JARTIC API Plugin** (Week 2):
   - Implement API client
   - Add rate limiting (if needed)
   - Test real-time data collection

6. **📋 ODPT Plugin** (Week 3):
   - Implement GTFS parser
   - Create transit-traffic correlation logic
   - Test proxy data accuracy

7. **📋 Full Integration & Testing** (Week 3-4):
   - Create dedicated `download_traffic.py` script
   - Create combined air quality + traffic reports
   - Performance testing with parallel downloads
   - Documentation updates

### Phase 5: CLI Commands

**Currently Available** (via existing CLI):
```bash
# Download JARTIC archive data
python download_air_quality.py --source jartic --country JP --start-date 2024-01-01 --end-date 2024-01-31

# List JARTIC locations
python download_air_quality.py --source jartic --country JP --list-locations
```

**To Be Implemented**:
```bash
# Dedicated traffic download script
python download_traffic.py --source jartic-archive --country JP --year 2023

# Download recent JARTIC data via API
python download_traffic.py --source jartic-api --country JP --days 90

# Download ODPT transit data
python download_traffic.py --source odpt --country JP --start-date 2023-01-01

# Combined download (air quality + traffic)
python download_all_data.py --country JP --include-traffic --start-date 2023-01-01
```

## Technical Considerations

1. **Data Volume**: 
   - JARTIC archives can be large (>4GB per month)
   - Implement streaming decompression
   - Consider disk space requirements

2. **Performance**:
   - Reuse existing parallel download infrastructure
   - Archive downloads don't need rate limiting
   - Cache parsed location mappings

3. **Data Quality**:
   - Handle missing traffic data gracefully
   - Log data quality issues
   - Provide data completeness metrics

4. **Alignment with Air Quality Data**:
   - Ensure temporal coverage matches (2023-2025)
   - Use same coordinate system
   - Maintain consistent file naming

## No-Cost Verification

This plan uses **only free data sources**:
- Compusophia archives: Free community resource
- JARTIC API: Free with registration
- ODPT API: Free with registration
- No commercial APIs or paid services required

---

## Claude Code Prompt for Implementation

Use the following prompt with Claude Code to implement this plan:

```
I have an air quality data collection system that uses Domain-Driven Design (DDD) architecture to download data from OpenAQ. The system is in /Users/vojtech/Code/Bard89/Project-Data/.

I need to add support for downloading free traffic data from Japan to correlate with air quality data. The plan is in next_steps.md.

Please implement Phase 1 and Phase 2 of the plan:

1. First, extend the domain models in src/domain/models.py to add the new ParameterType and MeasurementUnit enums for traffic data

2. Create the JARTIC Archive Plugin structure in src/plugins/jartic/ with:
   - __init__.py
   - archive_datasource.py that implements the DataSource interface
   - archive_downloader.py for downloading from compusophia.com
   - data_parser.py for parsing JARTIC data formats

3. The plugin should:
   - Download historical traffic archives from https://www.compusophia.com/en/notes/1
   - Parse the traffic volume data (both 5-minute and hourly intervals)
   - Convert JARTIC location data to our Location model with proper coordinates
   - Handle missing or invalid data gracefully
   - Follow the same patterns as the existing OpenAQ plugin

4. Make sure to:
   - Reuse existing infrastructure (retry logic, logging, etc.)
   - Follow the DDD architecture pattern
   - Add proper error handling for large file downloads
   - Use type hints and follow the existing code style

Please start with the domain model extensions and then create the basic plugin structure.
```

## Next Implementation Steps

1. Review and approve this plan
2. Use the prompt above with Claude Code to begin implementation
3. Test with small date ranges first
4. Gradually expand to full 2023-2025 coverage
5. Create combined analysis scripts for air quality + traffic correlation