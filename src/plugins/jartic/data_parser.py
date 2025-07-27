import csv
import json
import logging
import re
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

from ...domain.models import (
    Coordinates,
    Location,
    Measurement,
    MeasurementUnit,
    ParameterType,
    Sensor,
)


logger = logging.getLogger(__name__)


class JARTICDataParser:
    def __init__(self):
        self.traffic_data_patterns = {
            'volume': re.compile(r'traffic_volume_(\d+)\.csv', re.IGNORECASE),
            'speed': re.compile(r'speed_data_(\d+)\.csv', re.IGNORECASE),
            'occupancy': re.compile(r'occupancy_(\d+)\.csv', re.IGNORECASE),
            'cross_section': re.compile(r'cross_section_traffic\.csv', re.IGNORECASE),
            'hourly': re.compile(r'hourly_traffic_.*\.csv', re.IGNORECASE),
            '5min': re.compile(r'5min_traffic_.*\.csv', re.IGNORECASE)
        }

        self.location_patterns = {
            'locations': re.compile(r'observation_points\.csv', re.IGNORECASE),
            'stations': re.compile(r'traffic_stations\.json', re.IGNORECASE),
            'metadata': re.compile(r'station_metadata\.json', re.IGNORECASE)
        }

    async def parse_locations(self, archive_path: Path) -> List[Location]:
        locations = []
        location_map = {}

        try:
            with zipfile.ZipFile(archive_path, 'r') as zf:
                file_list = zf.namelist()

                location_files = self._find_location_files(file_list)

                for file_name in location_files:
                    try:
                        with zf.open(file_name) as f:
                            content = f.read().decode('utf-8', errors='ignore')

                            if file_name.endswith('.json'):
                                locations.extend(
                                    self._parse_json_locations(content, file_name)
                                )
                            elif file_name.endswith('.csv'):
                                locations.extend(
                                    self._parse_csv_locations(content, file_name)
                                )

                    except Exception as e:
                        logger.warning(f"Failed to parse location file {file_name}: {e}")

                if not locations:
                    locations = self._generate_locations_from_data_files(file_list)

        except Exception as e:
            logger.error(f"Failed to parse locations from archive: {e}")
            raise

        seen_ids = set()
        unique_locations = []
        for loc in locations:
            if loc.id not in seen_ids:
                seen_ids.add(loc.id)
                unique_locations.append(loc)

        logger.info(f"Parsed {len(unique_locations)} unique locations")
        return unique_locations

    async def parse_measurements(
        self,
        archive_path: Path,
        sensor: Sensor,
        start_date: datetime,
        end_date: datetime
    ) -> AsyncIterator[Measurement]:
        try:
            with zipfile.ZipFile(archive_path, 'r') as zf:
                file_list = zf.namelist()

                relevant_files = self._find_measurement_files(
                    file_list,
                    sensor.location.id,
                    sensor.parameter
                )

                for file_name in relevant_files:
                    try:
                        with zf.open(file_name) as f:
                            content = f.read().decode('utf-8', errors='ignore')

                            async for measurement in self._parse_measurement_file(
                                content,
                                file_name,
                                sensor,
                                start_date,
                                end_date
                            ):
                                yield measurement

                    except Exception as e:
                        logger.warning(f"Failed to parse measurement file {file_name}: {e}")

        except Exception as e:
            logger.error(f"Failed to parse measurements from archive: {e}")
            raise

    def _find_location_files(self, file_list: List[str]) -> List[str]:
        location_files = []

        for file_name in file_list:
            for pattern_type, pattern in self.location_patterns.items():
                if pattern.search(file_name):
                    location_files.append(file_name)
                    break

        if not location_files:
            for file_name in file_list:
                if 'location' in file_name.lower() or 'station' in file_name.lower():
                    if file_name.endswith(('.csv', '.json')):
                        location_files.append(file_name)

        return location_files

    def _find_measurement_files(
        self,
        file_list: List[str],
        location_id: str,
        parameter: ParameterType
    ) -> List[str]:
        relevant_files = []

        if parameter == ParameterType.TRAFFIC_VOLUME:
            patterns = ['volume', 'cross_section', 'hourly', '5min']
        elif parameter == ParameterType.VEHICLE_SPEED:
            patterns = ['speed']
        elif parameter == ParameterType.OCCUPANCY_RATE:
            patterns = ['occupancy']
        else:
            return []

        for file_name in file_list:
            if location_id in file_name:
                relevant_files.append(file_name)
                continue

            for pattern_key in patterns:
                if pattern_key in self.traffic_data_patterns:
                    pattern = self.traffic_data_patterns[pattern_key]
                    if pattern.search(file_name):
                        relevant_files.append(file_name)
                        break

        return relevant_files

    def _parse_json_locations(self, content: str, file_name: str) -> List[Location]:
        locations = []

        try:
            data = json.loads(content)

            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and 'locations' in data:
                items = data['locations']
            elif isinstance(data, dict) and 'stations' in data:
                items = data['stations']
            else:
                items = [data]

            for item in items:
                location = self._create_location_from_json(item)
                if location:
                    locations.append(location)

        except Exception as e:
            logger.warning(f"Failed to parse JSON locations: {e}")

        return locations

    def _parse_csv_locations(self, content: str, file_name: str) -> List[Location]:
        locations = []

        try:
            reader = csv.DictReader(StringIO(content))

            for row in reader:
                location = self._create_location_from_csv(row)
                if location:
                    locations.append(location)

        except Exception as e:
            logger.warning(f"Failed to parse CSV locations: {e}")

        return locations

    def _create_location_from_json(self, data: Dict[str, Any]) -> Optional[Location]:
        try:
            location_id = str(
                data.get('id') or
                data.get('station_id') or
                data.get('point_id') or
                data.get('code')
            )

            if not location_id:
                return None

            lat = float(data.get('latitude') or data.get('lat') or 0)
            lon = float(data.get('longitude') or data.get('lon') or data.get('lng') or 0)

            if lat == 0 or lon == 0:
                return None

            return Location(
                id=location_id,
                name=data.get('name') or data.get('station_name') or f"Station {location_id}",
                coordinates=Coordinates(
                    latitude=Decimal(str(lat)),
                    longitude=Decimal(str(lon))
                ),
                city=data.get('city') or data.get('municipality'),
                country="JP",
                provider="JARTIC",
                metadata={
                    'prefecture': data.get('prefecture'),
                    'road_type': data.get('road_type'),
                    'road_name': data.get('road_name'),
                    'direction': data.get('direction'),
                    'has_5min_data': data.get('has_5min_data', False),
                    'has_speed_data': data.get('has_speed_data', False),
                    'has_occupancy_data': data.get('has_occupancy_data', False)
                }
            )
        except Exception as e:
            logger.debug(f"Failed to create location from JSON: {e}")
            return None

    def _create_location_from_csv(self, row: Dict[str, str]) -> Optional[Location]:
        try:
            location_id = (
                row.get('観測地点ID') or
                row.get('station_id') or
                row.get('point_id') or
                row.get('id')
            )

            if not location_id:
                return None

            lat = float(row.get('緯度') or row.get('latitude') or row.get('lat') or 0)
            lon = float(row.get('経度') or row.get('longitude') or row.get('lon') or 0)

            if lat == 0 or lon == 0:
                return None

            return Location(
                id=location_id,
                name=(
                    row.get('地点名') or
                    row.get('name') or
                    row.get('station_name') or
                    f"Station {location_id}"
                ),
                coordinates=Coordinates(
                    latitude=Decimal(str(lat)),
                    longitude=Decimal(str(lon))
                ),
                city=row.get('市区町村') or row.get('city'),
                country="JP",
                provider="JARTIC",
                metadata={
                    'prefecture': row.get('都道府県') or row.get('prefecture'),
                    'road_type': row.get('道路種別') or row.get('road_type'),
                    'road_name': row.get('路線名') or row.get('road_name'),
                    'direction': row.get('方向') or row.get('direction')
                }
            )
        except Exception as e:
            logger.debug(f"Failed to create location from CSV: {e}")
            return None

    def _generate_locations_from_data_files(self, file_list: List[str]) -> List[Location]:
        locations = []
        location_ids = set()

        for file_name in file_list:
            match = re.search(r'(\d{3,6})', file_name)
            if match:
                location_id = match.group(1)
                if location_id not in location_ids:
                    location_ids.add(location_id)

                    locations.append(Location(
                        id=location_id,
                        name=f"Traffic Station {location_id}",
                        coordinates=Coordinates(
                            latitude=Decimal("35.6762"),
                            longitude=Decimal("139.6503")
                        ),
                        city="Unknown",
                        country="JP",
                        provider="JARTIC",
                        metadata={
                            'generated_from_filename': True,
                            'source_file': file_name
                        }
                    ))

        return locations

    async def _parse_measurement_file(
        self,
        content: str,
        file_name: str,
        sensor: Sensor,
        start_date: datetime,
        end_date: datetime
    ) -> AsyncIterator[Measurement]:
        if file_name.endswith('.json'):
            async for measurement in self._parse_json_measurements(
                content, sensor, start_date, end_date
            ):
                yield measurement
        elif file_name.endswith('.csv'):
            async for measurement in self._parse_csv_measurements(
                content, sensor, start_date, end_date
            ):
                yield measurement

    async def _parse_json_measurements(
        self,
        content: str,
        sensor: Sensor,
        start_date: datetime,
        end_date: datetime
    ) -> AsyncIterator[Measurement]:
        try:
            data = json.loads(content)

            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and 'measurements' in data:
                items = data['measurements']
            elif isinstance(data, dict) and 'data' in data:
                items = data['data']
            else:
                items = []

            for item in items:
                measurement = self._create_measurement_from_json(
                    item, sensor, start_date, end_date
                )
                if measurement:
                    yield measurement

        except Exception as e:
            logger.warning(f"Failed to parse JSON measurements: {e}")

    async def _parse_csv_measurements(
        self,
        content: str,
        sensor: Sensor,
        start_date: datetime,
        end_date: datetime
    ) -> AsyncIterator[Measurement]:
        try:
            reader = csv.DictReader(StringIO(content))

            for row in reader:
                measurement = self._create_measurement_from_csv(
                    row, sensor, start_date, end_date
                )
                if measurement:
                    yield measurement

        except Exception as e:
            logger.warning(f"Failed to parse CSV measurements: {e}")

    def _create_measurement_from_json(
        self,
        data: Dict[str, Any],
        sensor: Sensor,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[Measurement]:
        try:
            timestamp = self._parse_timestamp(
                data.get('timestamp') or
                data.get('datetime') or
                data.get('time')
            )

            if not timestamp:
                return None

            if not (start_date <= timestamp <= end_date):
                return None

            value = self._parse_traffic_value(
                data.get('value') or
                data.get('volume') or
                data.get('count'),
                sensor.parameter
            )

            if value is None:
                return None

            return Measurement(
                sensor=sensor,
                timestamp=timestamp,
                value=Decimal(str(value)),
                metadata={
                    'source': 'JARTIC',
                    'quality': data.get('quality', 'unknown')
                }
            )
        except Exception as e:
            logger.debug(f"Failed to create measurement from JSON: {e}")
            return None

    def _create_measurement_from_csv(
        self,
        row: Dict[str, str],
        sensor: Sensor,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[Measurement]:
        try:
            timestamp = self._parse_timestamp(
                row.get('時刻') or
                row.get('timestamp') or
                row.get('datetime') or
                row.get('date_time')
            )

            if not timestamp:
                return None

            if not (start_date <= timestamp <= end_date):
                return None

            value = self._parse_traffic_value(
                row.get('交通量') or
                row.get('台数') or
                row.get('volume') or
                row.get('count') or
                row.get('value'),
                sensor.parameter
            )

            if value is None:
                return None

            return Measurement(
                sensor=sensor,
                timestamp=timestamp,
                value=Decimal(str(value)),
                metadata={
                    'source': 'JARTIC',
                    'quality': row.get('品質', 'unknown')
                }
            )
        except Exception as e:
            logger.debug(f"Failed to create measurement from CSV: {e}")
            return None

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        if not timestamp_str:
            return None

        timestamp_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S+09:00',
            '%Y年%m月%d日 %H時%M分',
            '%Y年%m月%d日 %H:%M'
        ]

        for fmt in timestamp_formats:
            try:
                dt = datetime.strptime(timestamp_str.strip(), fmt)

                if '+09:00' in timestamp_str or fmt.endswith('時%M分'):
                    dt = dt.replace(tzinfo=timezone(timedelta(hours=9)))
                elif not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)

                return dt
            except ValueError:
                continue

        return None

    def _parse_traffic_value(
        self,
        value_str: Optional[str],
        parameter: ParameterType
    ) -> Optional[float]:
        if not value_str:
            return None

        try:
            value_str = value_str.strip().replace(',', '').replace('台', '')

            if value_str in ['', '-', 'N/A', 'null']:
                return None

            value = float(value_str)

            if value < 0:
                return None

            if parameter == ParameterType.OCCUPANCY_RATE and value > 100:
                return None

            return value

        except ValueError:
            return None
