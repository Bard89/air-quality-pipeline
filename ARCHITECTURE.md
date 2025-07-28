# Architecture Documentation

## Overview

This air quality data collection system follows Domain-Driven Design (DDD) principles with clear separation of concerns across layers.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Entry Points"
        CLI[download_air_quality.py]
        TRANS[transform_to_wide.py]
        VIEW[view_checkpoints.py]
    end
    
    subgraph "Application Layer"
        APP[application/]
        DNLD[downloader.py]
        JOB[job_manager.py]
        CLIAPP[cli.py]
    end
    
    subgraph "Domain Layer"
        DOM[domain/]
        MODELS[models.py]
        INTF[interfaces.py]
        EXC[exceptions.py]
    end
    
    subgraph "Infrastructure Layer"
        INFRA[infrastructure/]
        CACHE[cache.py]
        STORAGE[storage.py]
        RETRY[retry.py]
        METRICS[metrics.py]
    end
    
    subgraph "Core Components"
        CORE[core/]
        API1[api_client.py]
        API2[api_client_multi_key.py]
        API3[api_client_parallel.py]
        CHKPT[checkpoint_manager.py]
        DSTOR[data_storage.py]
    end
    
    subgraph "OpenAQ Plugin"
        OAQ[openaq/]
        CLIENT[client.py]
        INCR1[incremental_downloader_all.py]
        INCR2[incremental_downloader_parallel.py]
        LOC[location_finder.py]
    end
    
    CLI --> APP
    CLI --> OAQ
    TRANS --> CORE
    VIEW --> CORE
    
    APP --> DOM
    APP --> INFRA
    
    OAQ --> CORE
    OAQ --> DOM
    
    CORE --> DOM
    CORE --> INFRA
```

## Component Interaction Flow

```mermaid
sequenceDiagram
    participant User
    participant CLI
    participant OpenAQClient
    participant APIClient
    participant Downloader
    participant CheckpointManager
    participant DataStorage
    participant CSV
    
    User->>CLI: download_air_quality.py --country IN
    CLI->>OpenAQClient: Initialize with API keys
    OpenAQClient->>APIClient: Create API client (single/multi/parallel)
    
    CLI->>Downloader: Start download process
    Downloader->>CheckpointManager: Check for existing download
    
    alt Resume existing download
        CheckpointManager-->>Downloader: Return checkpoint
        Downloader->>DataStorage: Open existing CSV
    else New download
        CheckpointManager-->>Downloader: No checkpoint
        Downloader->>DataStorage: Create new CSV
    end
    
    loop For each location
        Downloader->>OpenAQClient: Get location sensors
        loop For each sensor
            Downloader->>OpenAQClient: Fetch measurements
            OpenAQClient->>APIClient: API request with rate limiting
            APIClient-->>OpenAQClient: Measurement data
            OpenAQClient-->>Downloader: Process measurements
            Downloader->>DataStorage: Write to CSV
        end
        Downloader->>CheckpointManager: Save checkpoint
    end
    
    Downloader-->>CLI: Download complete
    CLI-->>User: Success + analysis
```

## API Client Architecture

```mermaid
graph LR
    subgraph "API Client Evolution"
        A[api_client.py<br/>Basic Rate Limited<br/>60 req/min]
        B[api_client_multi_key.py<br/>Key Rotation<br/>N × 60 req/min]
        C[api_client_parallel.py<br/>Concurrent Requests<br/>All keys in parallel]
        
        A -->|Single Key| B
        B -->|Multiple Keys| C
    end
    
    subgraph "Features"
        F1[Rate Limiting]
        F2[Retry Logic]
        F3[Key Rotation]
        F4[Parallel Execution]
        F5[Error Handling]
    end
    
    A --> F1
    A --> F2
    B --> F3
    C --> F4
    C --> F5
```

## Data Flow

```mermaid
graph TD
    subgraph "Data Sources"
        API[OpenAQ API v3]
    end
    
    subgraph "Download Process"
        LOC[Location Finder]
        SENS[Sensor Discovery]
        MEAS[Measurement Fetcher]
    end
    
    subgraph "Data Processing"
        BATCH[Batch Processor]
        VALID[Data Validator]
        TRANS[Format Transformer]
    end
    
    subgraph "Storage"
        CSV1[Long Format CSV]
        CSV2[Wide Format CSV]
        CHKP[Checkpoint JSON]
    end
    
    API --> LOC
    LOC --> SENS
    SENS --> MEAS
    MEAS --> BATCH
    BATCH --> VALID
    VALID --> CSV1
    CSV1 --> TRANS
    TRANS --> CSV2
    
    MEAS -.-> CHKP
    CHKP -.-> MEAS
```

## Domain Model Structure

```mermaid
classDiagram
    class Coordinates {
        +Decimal latitude
        +Decimal longitude
        +validate()
    }
    
    class Location {
        +str id
        +str name
        +Coordinates coordinates
        +Optional~str~ city
        +Optional~str~ country
        +Dict metadata
    }
    
    class Sensor {
        +str id
        +Location location
        +ParameterType parameter
        +MeasurementUnit unit
        +bool is_active
        +Dict metadata
    }
    
    class Measurement {
        +Sensor sensor
        +datetime timestamp
        +Decimal value
        +Optional~str~ quality_flag
        +Dict metadata
    }
    
    class ParameterType {
        <<enumeration>>
        PM25
        PM10
        NO2
        O3
        CO
        SO2
        TEMPERATURE
        HUMIDITY
    }
    
    class MeasurementUnit {
        <<enumeration>>
        MICROGRAMS_PER_CUBIC_METER
        PARTS_PER_MILLION
        CELSIUS
        PERCENT
    }
    
    Location --> Coordinates
    Sensor --> Location
    Sensor --> ParameterType
    Sensor --> MeasurementUnit
    Measurement --> Sensor
```

## Download Strategy

```mermaid
graph TD
    subgraph "Download Mode Decision"
        START[Start Download]
        CHECK{Parallel Mode?}
        SEQUENTIAL[Sequential Download]
        PARALLEL[Parallel Download]
    end
    
    subgraph "Sequential Processing"
        SEQ1[Location 1 → Sensors → Measurements]
        SEQ2[Location 2 → Sensors → Measurements]
        SEQN[Location N → Sensors → Measurements]
    end
    
    subgraph "Parallel Processing"
        subgraph "Batch Decision"
            CHECK2{Sensors < 10?}
            BATCH_LOC[Batch by Location]
            BATCH_SENS[Batch by Sensor Pages]
        end
        
        subgraph "Location Batching"
            L1[Location 1<br/>All sensors]
            L2[Location 2<br/>All sensors]
            L3[Location N<br/>All sensors]
        end
        
        subgraph "Sensor Page Batching"
            S1[Sensor 1<br/>Pages 1-4]
            S2[Sensor 2<br/>Pages 1-4]
            S3[Sensor N<br/>Pages 1-4]
        end
    end
    
    START --> CHECK
    CHECK -->|No| SEQUENTIAL
    CHECK -->|Yes| PARALLEL
    
    SEQUENTIAL --> SEQ1
    SEQ1 --> SEQ2
    SEQ2 --> SEQN
    
    PARALLEL --> CHECK2
    CHECK2 -->|Yes| BATCH_LOC
    CHECK2 -->|No| BATCH_SENS
    
    BATCH_LOC --> L1
    BATCH_LOC --> L2
    BATCH_LOC --> L3
    
    BATCH_SENS --> S1
    BATCH_SENS --> S2
    BATCH_SENS --> S3
```

## Checkpoint System

```mermaid
stateDiagram-v2
    [*] --> CheckForExisting
    
    CheckForExisting --> LoadCheckpoint: Checkpoint exists
    CheckForExisting --> CreateNew: No checkpoint
    
    LoadCheckpoint --> ResumeDownload
    CreateNew --> StartDownload
    
    ResumeDownload --> DownloadLocation
    StartDownload --> DownloadLocation
    
    DownloadLocation --> SaveCheckpoint: After each location
    SaveCheckpoint --> DownloadLocation: More locations
    
    DownloadLocation --> Complete: All done
    Complete --> [*]
    
    DownloadLocation --> Interrupted: Ctrl+C or Error
    Interrupted --> [*]
    
    note right of SaveCheckpoint
        Checkpoint includes:
        - Current location index
        - Completed locations list
        - Output file path
        - Timestamp
    end note
```

## Directory Structure

```
project-root/
├── src/
│   ├── domain/           # Core business logic
│   │   ├── models.py     # Domain entities
│   │   ├── interfaces.py # Abstract interfaces
│   │   └── exceptions.py # Domain exceptions
│   │
│   ├── application/      # Application services
│   │   ├── cli.py        # CLI interface
│   │   ├── downloader.py # Download orchestration
│   │   └── job_manager.py # Job management
│   │
│   ├── infrastructure/   # Technical implementations
│   │   ├── cache.py      # Caching layer
│   │   ├── storage.py    # Storage implementations
│   │   ├── retry.py      # Retry mechanisms
│   │   └── metrics.py    # Metrics collection
│   │
│   ├── core/            # Shared components
│   │   ├── api_client*.py # API client variants
│   │   ├── checkpoint_manager.py
│   │   └── data_storage.py
│   │
│   ├── openaq/          # OpenAQ-specific
│   │   ├── client.py     # OpenAQ client
│   │   ├── location_finder.py
│   │   └── incremental_downloader*.py
│   │
│   └── utils/           # Utilities
│       ├── data_analyzer.py
│       └── csv_to_wide_format.py
│
├── data/                # Data directory
│   └── openaq/
│       ├── checkpoints/ # Checkpoint files
│       ├── processed/   # Output CSV files
│       └── raw/         # Raw data (if any)
│
├── config/              # Configuration files
├── tests/               # Test files
└── *.py                 # Entry point scripts
```

## Key Design Patterns

1. **Repository Pattern**: Abstract data access through interfaces
2. **Strategy Pattern**: Multiple API client implementations
3. **Observer Pattern**: Progress tracking and metrics
4. **Command Pattern**: CLI command handling
5. **Factory Pattern**: Dynamic client selection based on API keys

## Performance Characteristics

### Throughput
- **Single Key**: 60 requests/minute
- **Multi Key (N keys)**: N × 60 requests/minute
- **Parallel Mode**: Concurrent requests across all keys (requires multiple API keys)
- **Checkpoint Saves**: After each location (not sensor)
- **CSV Writes**: Incremental after each sensor (append mode)
- **API Limit**: Max 16 pages (16,000 measurements) per sensor
- **Download Mode**: All downloads are incremental (resumable)

### Memory Usage
- **Peak Memory**: ~200-500MB during parallel downloads (varies by concurrent keys)
- **Memory Scaling**: Linear with number of concurrent API keys
- **Deque Optimization**: Histogram storage uses collections.deque for O(1) append/pop
- **CSV Buffering**: Incremental writes prevent memory accumulation
- **Typical Usage**:
  - 1 key: ~100MB baseline
  - 10 keys: ~300MB (concurrent request buffers)
  - 100 keys: ~1-2GB (high concurrency overhead)

## Future Extension Points

1. **New Data Sources**: Implement new plugins following the domain interfaces
2. **Storage Backends**: Add database storage by implementing Storage interface
3. **Export Formats**: Extend data transformation utilities
4. **Monitoring**: Integrate with metrics collectors
5. **Scheduling**: Add job queue for automated downloads