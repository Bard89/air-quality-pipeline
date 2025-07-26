import asyncio
import argparse
from pathlib import Path
from datetime import datetime
import sys
from typing import List, Optional
from ..infrastructure.config import ApplicationConfig
from ..infrastructure.container import get_container
from ..infrastructure.logging import setup_logging
from ..infrastructure.cache import MemoryCache
from ..infrastructure.storage import CSVStorage
from ..infrastructure.metrics import PrometheusMetrics, MetricsReporter
from ..plugins import get_registry
from ..domain.models import ParameterType
from .downloader import AirQualityDownloader
from .job_manager import InMemoryJobManager


async def main():
    parser = argparse.ArgumentParser(
        description="Download air quality data from multiple sources"
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml"),
        help="Configuration file path"
    )
    
    parser.add_argument(
        "--country", "-c",
        type=str,
        help="Country code (e.g., US, IN, JP)"
    )
    
    parser.add_argument(
        "--parameters", "-p",
        type=str,
        help="Comma-separated parameters (e.g., pm25,pm10,no2)"
    )
    
    parser.add_argument(
        "--country-wide",
        action="store_true",
        required=True,
        help="Download all data from country (required due to API v3 limitation)"
    )
    
    parser.add_argument(
        "--max-locations",
        type=int,
        help="Maximum number of locations to download"
    )
    
    parser.add_argument(
        "--source",
        type=str,
        default="openaq",
        help="Data source to use"
    )
    
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Enable parallel downloads"
    )
    
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore checkpoints"
    )
    
    parser.add_argument(
        "--list-countries",
        action="store_true",
        help="List available countries"
    )
    
    args = parser.parse_args()
    
    if args.config.exists():
        config = ApplicationConfig.from_file(args.config)
    else:
        config = ApplicationConfig.from_env()
    
    setup_logging(
        level=config.logging.level,
        format_type=config.logging.format,
        log_file=config.logging.file_path
    )
    
    container = get_container()
    
    registry = get_registry()
    registry.auto_discover()
    
    source_config = next(
        (ds for ds in config.data_sources if ds.name == args.source),
        None
    )
    
    if not source_config or not source_config.enabled:
        print(f"Error: Data source '{args.source}' not configured or disabled")
        sys.exit(1)
    
    if not source_config.api_keys:
        print(f"Error: No API keys configured for '{args.source}'")
        sys.exit(1)
    
    data_source_class = registry.get(args.source)
    async with data_source_class(
        api_keys=source_config.api_keys,
        **source_config.config
    ) as data_source:
        
        if args.list_countries:
            await list_countries(data_source)
            return
        
        if not args.country:
            print("Error: --country is required")
            sys.exit(1)
        
        parameters = []
        if args.parameters:
            param_names = [p.strip() for p in args.parameters.split(",")]
            for param in param_names:
                try:
                    parameters.append(ParameterType(param))
                except ValueError:
                    print(f"Error: Invalid parameter '{param}'")
                    sys.exit(1)
        
        output_file = config.storage.base_path / f"{args.source}/processed/{args.country.lower()}_airquality_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        checkpoint_dir = config.storage.checkpoint_dir / args.source
        
        async with CSVStorage(
            output_file=output_file,
            batch_size=config.storage.batch_size,
            checkpoint_dir=checkpoint_dir
        ) as storage:
            
            cache = MemoryCache(
                max_size=config.cache.max_size,
                default_ttl=config.cache.ttl
            )
            
            metrics = PrometheusMetrics(
                namespace=config.metrics.namespace,
                port=config.metrics.port
            )
            
            job_manager = InMemoryJobManager()
            
            downloader = AirQualityDownloader(
                data_source=data_source,
                storage=storage,
                job_manager=job_manager,
                metrics=metrics,
                max_concurrent_locations=config.max_concurrent_locations if args.parallel else 1,
                max_concurrent_sensors=config.max_concurrent_sensors if args.parallel else 1
            )
            
            metrics_reporter = MetricsReporter(
                metrics=metrics,
                interval=config.metrics.flush_interval
            )
            
            await metrics_reporter.start()
            
            try:
                job_id = await downloader.download_country(
                    country_code=args.country,
                    parameters=parameters,
                    max_locations=args.max_locations,
                    resume=not args.no_resume
                )
                
                print(f"\nDownload completed! Job ID: {job_id}")
                print(f"Output file: {output_file}")
                
            finally:
                await metrics_reporter.stop()


async def list_countries(data_source):
    countries = await data_source.list_countries()
    
    print("\nAvailable countries:")
    print("-" * 50)
    
    for country in sorted(countries, key=lambda x: x['name']):
        print(f"{country['code']:5} {country['name']:30} "
              f"({country.get('locations', 0):,} locations, "
              f"{country.get('sensors', 0):,} sensors)")


if __name__ == "__main__":
    asyncio.run(main())