from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict


@dataclass
class DataReference:
    source: str
    country: str
    parameter: str
    start_date: datetime
    end_date: datetime
    file_path: Path
    format: str
    size_mb: float
    row_count: Optional[int] = None

    def to_dict(self) -> Dict:
        result = asdict(self)
        result['file_path'] = str(result['file_path'])
        if isinstance(result['start_date'], datetime):
            result['start_date'] = result['start_date'].isoformat()
        else:
            result['start_date'] = result['start_date']
        if isinstance(result['end_date'], datetime):
            result['end_date'] = result['end_date'].isoformat()
        else:
            result['end_date'] = result['end_date']
        return result


class ExternalDataManager:
    def __init__(self, external_data_path: Optional[Path] = None):
        if external_data_path is None:
            current_dir = Path(__file__).resolve().parent.parent.parent
            self.external_data_path = current_dir.parent / "Project-Data" / "data"
        else:
            self.external_data_path = Path(external_data_path)

        if not self.external_data_path.exists():
            raise ValueError(
                f"External data path does not exist: {self.external_data_path}"
            )

        self.data_sources = {
            'openaq': self.external_data_path / 'openaq' / 'processed',
            'openmeteo': self.external_data_path / 'openmeteo' / 'processed',
            'nasapower': self.external_data_path / 'nasapower' / 'processed',
            'firms': self.external_data_path / 'firms' / 'processed',
            'era5': self.external_data_path / 'era5' / 'processed',
            'jartic': self.external_data_path / 'jartic' / 'processed',
            'terrain': self.external_data_path / 'terrain' / 'processed',
            'jma': self.external_data_path / 'jma' / 'processed'
        }

        self.raw_sources = {
            'openaq': self.external_data_path / 'openaq' / 'raw',
            'firms': self.external_data_path / 'firms' / 'raw',
            'era5': self.external_data_path / 'era5' / 'raw',
            'jartic': self.external_data_path / 'jartic' / 'cache'
        }

    def get_processed_path(self, source: str) -> Path:
        if source not in self.data_sources:
            raise ValueError(f"Unknown data source: {source}")
        return self.data_sources[source]

    def get_raw_path(self, source: str) -> Path:
        if source not in self.raw_sources:
            raise ValueError(f"No raw path configured for source: {source}")
        return self.raw_sources[source]

    def list_files(self, source: str, pattern: str = "*",
                   processed: bool = True) -> List[Path]:
        if processed:
            source_path = self.get_processed_path(source)
        else:
            source_path = self.get_raw_path(source)

        if not source_path.exists():
            return []

        return sorted(source_path.glob(pattern))

    def get_latest_file(self, source: str, country: Optional[str] = None,
                        parameter: Optional[str] = None) -> Optional[Path]:
        pattern = "*"
        if country:
            pattern = f"{country.lower()}_*"

        files = self.list_files(source, pattern)

        if parameter and source == 'openaq':
            files = [f for f in files if parameter in f.name]

        return files[-1] if files else None

    def get_date_range_files(self, source: str, start_date: datetime,
                              end_date: datetime,
                              country: Optional[str] = None) -> List[Path]:
        pattern = f"{country.lower()}_*" if country else "*"
        files = self.list_files(source, pattern)

        result = []
        for file in files:
            parts = file.stem.split('_')

            if source in ('openmeteo', 'nasapower'):
                try:
                    file_start = datetime.strptime(parts[-3], '%Y%m%d')
                    file_end = datetime.strptime(parts[-1], '%Y%m%d')

                    if not (file_end < start_date or file_start > end_date):
                        result.append(file)
                except (ValueError, IndexError):
                    continue

            elif source == 'era5':
                try:
                    year_month = '_'.join(parts[-2:])
                    file_date = datetime.strptime(year_month, '%Y_%m')

                    month_start = start_date.replace(day=1)
                    month_end = end_date.replace(day=1)
                    if month_start <= file_date <= month_end:
                        result.append(file)
                except (ValueError, IndexError):
                    continue

        return sorted(result)

    def build_file_path(self, source: str, country: str, data_type: str,
                        start_date: datetime,
                        end_date: Optional[datetime] = None,
                        extension: str = 'csv') -> Path:
        base_path = self.get_processed_path(source)

        if end_date:
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')
            date_str = f"{start_str}_to_{end_str}"
        else:
            date_str = start_date.strftime('%Y%m%d_%H%M%S')

        country_lower = country.lower()
        filename = f"{country_lower}_{source}_{data_type}_{date_str}.{extension}"
        return base_path / filename

    def get_checkpoint_path(self, source: str) -> Path:
        checkpoint_dir = self.external_data_path / source / 'checkpoints'
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        return checkpoint_dir

    def save_new_data_path(self, source: str, processed: bool = True) -> Path:
        if processed:
            save_path = self.get_processed_path(source)
        else:
            save_path = self.get_raw_path(source)

        save_path.mkdir(parents=True, exist_ok=True)
        return save_path