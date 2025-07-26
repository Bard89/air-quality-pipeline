from typing import List, Optional, Dict, Any
import csv
import json
from pathlib import Path
from datetime import datetime
import asyncio
import aiofiles
from ..domain.interfaces import Storage
from ..domain.models import Measurement
from ..domain.exceptions import StorageException, CheckpointException
import logging


logger = logging.getLogger(__name__)


class CSVStorage(Storage):
    def __init__(
        self,
        output_file: Path,
        batch_size: int = 1000,
        checkpoint_dir: Optional[Path] = None
    ):
        self.output_file = Path(output_file)
        self.batch_size = batch_size
        self.checkpoint_dir = Path(checkpoint_dir) if checkpoint_dir else self.output_file.parent / "checkpoints"
        self._buffer: List[Measurement] = []
        self._file_handle = None
        self._csv_writer = None
        self._lock = asyncio.Lock()
        self._measurement_count = 0
        self._header_written = False

    async def __aenter__(self):
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        mode = 'a' if self.output_file.exists() else 'w'
        self._file_handle = await aiofiles.open(self.output_file, mode=mode, newline='')
        
        if mode == 'a' and self.output_file.stat().st_size > 0:
            self._header_written = True
            self._measurement_count = await self._count_existing_rows()
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.flush()
        if self._file_handle:
            await self._file_handle.close()

    async def save_measurement(self, measurement: Measurement) -> None:
        async with self._lock:
            self._buffer.append(measurement)
            if len(self._buffer) >= self.batch_size:
                await self.flush()

    async def save_measurements_batch(self, measurements: List[Measurement]) -> int:
        if not measurements:
            return 0
        
        async with self._lock:
            self._buffer.extend(measurements)
            await self.flush()
            return len(measurements)

    async def flush(self) -> None:
        if not self._buffer:
            return
        
        rows = []
        for measurement in self._buffer:
            row = {
                'datetime': measurement.timestamp.isoformat(),
                'value': float(measurement.value),
                'sensor_id': measurement.sensor.id,
                'location_id': measurement.sensor.location.id,
                'location_name': measurement.sensor.location.name,
                'latitude': float(measurement.sensor.location.coordinates.latitude),
                'longitude': float(measurement.sensor.location.coordinates.longitude),
                'parameter': measurement.sensor.parameter.value,
                'unit': measurement.sensor.unit.value,
                'city': measurement.sensor.location.city or '',
                'country': measurement.sensor.location.country
            }
            rows.append(row)
        
        if not self._header_written and rows:
            header = ','.join(rows[0].keys()) + '\n'
            await self._file_handle.write(header)
            self._header_written = True
        
        for row in rows:
            line = ','.join(str(v) for v in row.values()) + '\n'
            await self._file_handle.write(line)
        
        await self._file_handle.flush()
        self._measurement_count += len(self._buffer)
        self._buffer.clear()
        
        logger.debug(f"Flushed {len(rows)} measurements to {self.output_file}")

    async def get_checkpoint(self, job_id: str) -> Optional[Dict[str, Any]]:
        checkpoint_file = self.checkpoint_dir / f"checkpoint_{job_id}.json"
        
        if not checkpoint_file.exists():
            return None
        
        try:
            async with aiofiles.open(checkpoint_file, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            raise CheckpointException(f"Failed to read checkpoint: {e}")

    async def save_checkpoint(self, job_id: str, checkpoint: Dict[str, Any]) -> None:
        checkpoint_file = self.checkpoint_dir / f"checkpoint_{job_id}.json"
        
        checkpoint['timestamp'] = datetime.utcnow().isoformat()
        checkpoint['measurement_count'] = self._measurement_count
        checkpoint['output_file'] = str(self.output_file)
        
        try:
            async with aiofiles.open(checkpoint_file, 'w') as f:
                await f.write(json.dumps(checkpoint, indent=2))
            
            await self._update_checkpoint_history(job_id, checkpoint)
            
        except Exception as e:
            raise CheckpointException(f"Failed to save checkpoint: {e}")

    async def close(self) -> None:
        await self.flush()
        if self._file_handle:
            await self._file_handle.close()
            self._file_handle = None

    async def _count_existing_rows(self) -> int:
        count = 0
        async with aiofiles.open(self.output_file, 'r') as f:
            async for line in f:
                count += 1
        return max(0, count - 1)

    async def _update_checkpoint_history(self, job_id: str, checkpoint: Dict[str, Any]) -> None:
        history_file = self.checkpoint_dir / "checkpoint_history.json"
        
        history = {}
        if history_file.exists():
            async with aiofiles.open(history_file, 'r') as f:
                content = await f.read()
                history = json.loads(content) if content else {}
        
        output_file = str(self.output_file)
        if output_file not in history:
            history[output_file] = []
        
        history_entry = {
            'checkpoint_file': str(self.checkpoint_dir / f"checkpoint_{job_id}.json"),
            'location_index': checkpoint.get('location_index', 0),
            'timestamp': checkpoint['timestamp'],
            'total_locations': checkpoint.get('total_locations', 0),
            'measurements_count': checkpoint.get('measurement_count')
        }
        
        history[output_file].append(history_entry)
        
        async with aiofiles.open(history_file, 'w') as f:
            await f.write(json.dumps(history, indent=2))