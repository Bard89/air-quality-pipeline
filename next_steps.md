# Japan Traffic Data Integration Plan

**Created Date:** 2025-07-26  
**Current Branch:** add-traffic-data  
**Target Data Period:** 2023-2025 (to align with existing air quality data)

## Overview
This document outlines the plan to integrate free traffic data sources for Japan (2023-2025) into the existing air quality data collection system.

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

### Phase 1: Domain Model Extensions

1. **Extend ParameterType enum** in `src/domain/models.py`:
   ```python
   # Traffic parameters
   TRAFFIC_VOLUME = "traffic_volume"
   VEHICLE_SPEED = "vehicle_speed"
   OCCUPANCY_RATE = "occupancy_rate"
   # Transit proxy parameters
   TRANSIT_RIDERSHIP = "transit_ridership"
   ```

2. **Extend MeasurementUnit enum**:
   ```python
   VEHICLES_PER_HOUR = "vehicles/hour"
   VEHICLES_PER_5MIN = "vehicles/5min"
   KILOMETERS_PER_HOUR = "km/h"
   PERCENT_OCCUPANCY = "%occupancy"
   PASSENGERS_PER_HOUR = "passengers/hour"
   ```

### Phase 2: Create Traffic Data Plugins

1. **JARTIC Archive Plugin** (`src/plugins/jartic/`):
   - `archive_datasource.py`: Implements DataSource interface
   - `archive_downloader.py`: Downloads and extracts monthly archives
   - `data_parser.py`: Parses JARTIC CSV/JSON formats
   - Features:
     - Download historical archives from compusophia.com
     - Parse traffic volume data (5-min/hourly)
     - Map JARTIC locations to coordinates
     - Handle missing data gracefully

2. **JARTIC API Plugin** (`src/plugins/jartic_api/`):
   - `api_datasource.py`: Implements DataSource interface
   - Uses existing API client pattern
   - For recent data (last 3 months)

3. **ODPT Transit Plugin** (`src/plugins/odpt/`):
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

### Phase 4: Implementation Steps

1. **Setup & Configuration** (Week 1):
   - Register for JARTIC API access
   - Register for ODPT developer account
   - Create configuration files for each data source
   - Document API keys in `.env.example`

2. **Core Extensions** (Week 1):
   - Extend domain models
   - Create base traffic plugin structure
   - Add traffic-specific error handling

3. **JARTIC Archive Plugin** (Week 2):
   - Implement archive downloader
   - Create data parser for JARTIC formats
   - Build location mapping logic
   - Test with 2023-2024 data

4. **JARTIC API Plugin** (Week 3):
   - Implement API client
   - Add rate limiting (if needed)
   - Test real-time data collection

5. **ODPT Plugin** (Week 3-4):
   - Implement GTFS parser
   - Create transit-traffic correlation logic
   - Test proxy data accuracy

6. **Integration & Testing** (Week 4):
   - Update CLI to support traffic data sources
   - Create combined air quality + traffic reports
   - Performance testing with parallel downloads
   - Documentation updates

### Phase 5: CLI Commands

New commands to add:

```bash
# Download historical JARTIC archives
python download_traffic.py --source jartic-archive --country JP --year 2023

# Download recent JARTIC data via API
python download_traffic.py --source jartic-api --country JP --days 90

# Download ODPT transit data
python download_traffic.py --source odpt --country JP --start-date 2023-01-01

# Combined download (air quality + traffic)
python download_all_data.py --country JP --include-traffic
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