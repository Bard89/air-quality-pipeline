from typing import List, Optional
from pathlib import Path
from datetime import datetime
import logging
from .storage import CSVStorage
from .data_reference import ExternalDataManager


logger = logging.getLogger(__name__)


class ExternalDataStorage(CSVStorage):
    def __init__(
        self,
        source: str,
        country: str,
        data_type: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        batch_size: int = 1000,
        external_data_manager: Optional[ExternalDataManager] = None
    ):
        self.external_data_manager = (
            external_data_manager or ExternalDataManager()
        )
        self.source = source
        self.country = country
        self.data_type = data_type
        self.start_date = start_date
        self.end_date = end_date

        output_file = self.external_data_manager.build_file_path(
            source=source,
            country=country,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date
        )

        checkpoint_dir = self.external_data_manager.get_checkpoint_path(source)

        super().__init__(
            output_file=output_file,
            batch_size=batch_size,
            checkpoint_dir=checkpoint_dir
        )

        logger.info(
            "Initialized ExternalDataStorage for %s/%s at %s",
            source, country, output_file
        )

    def get_existing_data_files(self) -> List[Path]:
        return self.external_data_manager.get_date_range_files(
            source=self.source,
            start_date=self.start_date,
            end_date=self.end_date or self.start_date,
            country=self.country
        )

    def get_latest_file(self) -> Optional[Path]:
        return self.external_data_manager.get_latest_file(
            source=self.source,
            country=self.country
        )
