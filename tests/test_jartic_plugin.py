import json
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.domain.models import (
    Coordinates,
    Location,
    Measurement,
    MeasurementUnit,
    ParameterType,
    Sensor,
)
from src.plugins.jartic.archive_downloader import JARTICArchiveDownloader
from src.plugins.jartic.data_parser import JARTICDataParser
from src.plugins.jartic.datasource import JARTICDataSource


class TestJARTICArchiveDownloader:
    @pytest.fixture
    def downloader(self, tmp_path):
        return JARTICArchiveDownloader(
            base_url="https://test.example.com",
            cache_dir=tmp_path / "cache"
        )
    
    @pytest.mark.asyncio
    async def test_get_archive_index(self, downloader):
        mock_html = """
        <html>
            <body>
                <a href="/data/jartic_2024_01.zip">2024年1月</a>
                <a href="/data/jartic_2024_02.zip">2024年2月</a>
                <a href="/data/jartic_2023_12.zip">2023年12月</a>
            </body>
        </html>
        """
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value=mock_html)
            mock_get.return_value.__aenter__.return_value = mock_response
            
            index = await downloader.get_archive_index()
            
            assert len(index) == 3
            assert index[0]['year'] == 2024
            assert index[0]['month'] == 2
            assert index[1]['year'] == 2024
            assert index[1]['month'] == 1
            assert index[2]['year'] == 2023
            assert index[2]['month'] == 12
    
    @pytest.mark.asyncio
    async def test_download_archive_cached(self, downloader):
        cache_file = downloader.cache_dir / "jartic_2024_01.zip"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(cache_file, 'w') as zf:
            zf.writestr('test.txt', 'cached content')
        
        async with downloader:
            result = await downloader.download_archive(2024, 1)
        
        assert result == cache_file
        assert result.exists()
    
    @pytest.mark.asyncio
    async def test_verify_zip(self, downloader, tmp_path):
        valid_zip = tmp_path / "valid.zip"
        with zipfile.ZipFile(valid_zip, 'w') as zf:
            zf.writestr('test.txt', 'content')
        
        assert downloader._verify_zip(valid_zip) is True
        
        invalid_zip = tmp_path / "invalid.zip"
        invalid_zip.write_text("not a zip file")
        
        assert downloader._verify_zip(invalid_zip) is False


class TestJARTICDataParser:
    @pytest.fixture
    def parser(self):
        return JARTICDataParser()
    
    @pytest.fixture
    def sample_location(self):
        return Location(
            id="001",
            name="Test Station",
            coordinates=Coordinates(
                latitude=Decimal("35.6762"),
                longitude=Decimal("139.6503")
            ),
            city="Tokyo",
            country="JP",
            provider="JARTIC"
        )
    
    @pytest.fixture
    def sample_sensor(self, sample_location):
        return Sensor(
            id="001_traffic_volume",
            location=sample_location,
            parameter=ParameterType.TRAFFIC_VOLUME,
            unit=MeasurementUnit.VEHICLES_PER_HOUR,
            is_active=True
        )
    
    def test_create_location_from_json(self, parser):
        data = {
            "id": "123",
            "name": "Test Location",
            "latitude": 35.6762,
            "longitude": 139.6503,
            "city": "Tokyo",
            "prefecture": "Tokyo",
            "road_type": "National",
            "has_5min_data": True
        }
        
        location = parser._create_location_from_json(data)
        
        assert location is not None
        assert location.id == "123"
        assert location.name == "Test Location"
        assert float(location.coordinates.latitude) == 35.6762
        assert float(location.coordinates.longitude) == 139.6503
        assert location.city == "Tokyo"
        assert location.metadata['prefecture'] == "Tokyo"
        assert location.metadata['has_5min_data'] is True
    
    def test_create_location_from_csv(self, parser):
        row = {
            "観測地点ID": "456",
            "地点名": "CSV Location",
            "緯度": "35.6762",
            "経度": "139.6503",
            "市区町村": "Shibuya",
            "都道府県": "Tokyo",
            "道路種別": "Prefectural"
        }
        
        location = parser._create_location_from_csv(row)
        
        assert location is not None
        assert location.id == "456"
        assert location.name == "CSV Location"
        assert location.city == "Shibuya"
        assert location.metadata['prefecture'] == "Tokyo"
    
    def test_parse_timestamp(self, parser):
        test_cases = [
            ("2024-01-15 10:30:00", datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)),
            ("2024/01/15 10:30:00", datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)),
            ("2024-01-15T10:30:00Z", datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)),
            ("2024年1月15日 10時30分", datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone(timedelta(hours=9))))
        ]
        
        for input_str, expected in test_cases:
            result = parser._parse_timestamp(input_str)
            assert result == expected
    
    def test_parse_traffic_value(self, parser):
        assert parser._parse_traffic_value("1234", ParameterType.TRAFFIC_VOLUME) == 1234.0
        assert parser._parse_traffic_value("1,234", ParameterType.TRAFFIC_VOLUME) == 1234.0
        assert parser._parse_traffic_value("1234台", ParameterType.TRAFFIC_VOLUME) == 1234.0
        assert parser._parse_traffic_value("-", ParameterType.TRAFFIC_VOLUME) is None
        assert parser._parse_traffic_value("", ParameterType.TRAFFIC_VOLUME) is None
        assert parser._parse_traffic_value("-100", ParameterType.TRAFFIC_VOLUME) is None
        
        assert parser._parse_traffic_value("85.5", ParameterType.OCCUPANCY_RATE) == 85.5
        assert parser._parse_traffic_value("150", ParameterType.OCCUPANCY_RATE) is None
    
    @pytest.mark.asyncio
    async def test_parse_measurements_from_csv(self, parser, sample_sensor, tmp_path):
        csv_content = """時刻,交通量,品質
2024-01-15 10:00:00,1234,good
2024-01-15 11:00:00,2345,good
2024-01-15 12:00:00,-,missing
2024-01-15 13:00:00,3456,good
"""
        
        archive_path = tmp_path / "test.zip"
        with zipfile.ZipFile(archive_path, 'w') as zf:
            zf.writestr("traffic_001.csv", csv_content)
        
        measurements = []
        async for m in parser.parse_measurements(
            archive_path,
            sample_sensor,
            datetime(2024, 1, 15),
            datetime(2024, 1, 16)
        ):
            measurements.append(m)
        
        assert len(measurements) == 3
        assert measurements[0].value == Decimal("1234")
        assert measurements[1].value == Decimal("2345")
        assert measurements[2].value == Decimal("3456")


class TestJARTICDataSource:
    @pytest.fixture
    def datasource(self, tmp_path):
        return JARTICDataSource(
            api_keys=["dummy_key"],
            base_url="https://test.example.com",
            cache_dir=tmp_path / "cache"
        )
    
    @pytest.mark.asyncio
    async def test_list_countries(self, datasource):
        async with datasource:
            countries = await datasource.list_countries()
        
        assert len(countries) == 1
        assert countries[0]['code'] == 'JP'
        assert countries[0]['name'] == 'Japan'
    
    @pytest.mark.asyncio
    async def test_get_sensors(self, datasource):
        location = Location(
            id="001",
            name="Test Station",
            coordinates=Coordinates(
                latitude=Decimal("35.6762"),
                longitude=Decimal("139.6503")
            ),
            metadata={
                'has_5min_data': True,
                'has_speed_data': True,
                'has_occupancy_data': False
            }
        )
        
        async with datasource:
            sensors = await datasource.get_sensors(location)
        
        assert len(sensors) == 3
        
        sensor_types = {s.parameter for s in sensors}
        assert ParameterType.TRAFFIC_VOLUME in sensor_types
        assert ParameterType.VEHICLE_SPEED in sensor_types
        assert ParameterType.OCCUPANCY_RATE not in sensor_types
        
        units = {s.unit for s in sensors}
        assert MeasurementUnit.VEHICLES_PER_HOUR in units
        assert MeasurementUnit.VEHICLES_PER_5MIN in units
        assert MeasurementUnit.KILOMETERS_PER_HOUR in units
    
    @pytest.mark.asyncio
    async def test_get_metadata(self, datasource):
        async with datasource:
            metadata = await datasource.get_metadata()
        
        assert metadata['name'] == 'JARTIC Archive'
        assert metadata['coverage'] == 'Japan nationwide'
        assert metadata['observation_points'] == 2600
        assert 'historical_range' in metadata