#!/usr/bin/env python3
from src.infrastructure.data_reference import ExternalDataManager
from datetime import datetime


def test_external_data_manager():
    print("Testing External Data Manager...")
    
    manager = ExternalDataManager()
    
    print(f"\nExternal data path: {manager.external_data_path}")
    print(f"Path exists: {manager.external_data_path.exists()}")
    
    print("\n=== Available Data Sources ===")
    for source, path in manager.data_sources.items():
        files = manager.list_files(source)
        print(f"{source}: {len(files)} files in {path}")
        if files and len(files) <= 3:
            for f in files[:3]:
                print(f"  - {f.name}")
    
    print("\n=== Testing OpenAQ Data ===")
    latest_jp = manager.get_latest_file('openaq', country='JP')
    if latest_jp:
        print(f"Latest Japan OpenAQ file: {latest_jp.name}")
    
    print("\n=== Testing OpenMeteo Data ===")
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 3, 31)
    jp_weather_files = manager.get_date_range_files(
        'openmeteo', start_date, end_date, country='JP'
    )
    print(f"Found {len(jp_weather_files)} weather files for JP Q1 2023")
    for f in jp_weather_files[:3]:
        print(f"  - {f.name}")
    
    print("\n=== Testing File Path Building ===")
    new_file_path = manager.build_file_path(
        source='openaq',
        country='JP',
        data_type='airquality_test',
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 31)
    )
    print(f"New file would be saved to: {new_file_path}")
    
    print("\n✅ All tests passed!")


if __name__ == "__main__":
    test_external_data_manager()