# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Weather data collection support for Japan
- JMA (Japan Meteorological Agency) plugin for AMeDAS stations and JRA-55 reanalysis
- ERA5 weather data plugin with 0.25° resolution and atmospheric layers
- NASA POWER weather data plugin with free API access
- Weather-specific parameter types and measurement units
- download_weather_data.py CLI for weather data collection
- Support for multi-level atmospheric data (surface to 100hPa)
- Comprehensive weather parameters including temperature, humidity, wind, precipitation, solar radiation, visibility, cloud cover, and dew point
- Architecture documentation with Mermaid diagrams
  - High-level component overview
  - Sequence diagrams for download flow
  - Class diagrams for domain models
  - State diagrams for checkpoint system
- ARCHITECTURE.md file with comprehensive system design documentation
- Visual representation of:
  - DDD layer interactions
  - API client evolution (single → multi-key → parallel)
  - Data flow from API to CSV storage
  - Parallel download decision logic
  - Checkpoint/resume mechanism
- generate_diagrams.py script for creating dependency graphs

### Changed
- Enhanced project documentation structure

## [1.0.0] - 2025-01-26

### Added
- Japan air traffic data sources documentation
- Comprehensive test suite with domain model tests
- Application layer with modern CLI interface
- Plugin architecture for extensible data sources
- Comprehensive metrics collection system
- Async storage and caching layers
- Retry mechanisms and circuit breakers
- Configuration files and documentation
- Comprehensive validation for OpenAQ data source
- Async-safe logging with contextvars
- Deque-based histogram storage for memory efficiency

### Fixed
- Enforce zero-padded API key numbering format
- Clean up CLI and remove unused imports
- Improve domain models and exception handling
- Optimize configuration and API key loading
- Improve CSV handling and checkpoint accumulation
- Enhance retry mechanisms and circuit breaker implementation
- Improve metrics collection and thread safety

### Changed
- Refactored entire codebase following Domain-Driven Design (DDD) principles
- Major architectural overhaul for scale and expansion
- Improved code organization with clear layer separation (domain, application, infrastructure)
- Enhanced sensor test coverage