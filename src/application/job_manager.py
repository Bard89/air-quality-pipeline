from typing import List, Optional, Dict, Any
from datetime import datetime
from ..domain.interfaces import JobManager
from ..domain.models import DownloadJob
import asyncio


class InMemoryJobManager(JobManager):
    def __init__(self):
        self._jobs: Dict[str, DownloadJob] = {}
        self._lock = asyncio.Lock()

    async def create_job(self, job: DownloadJob) -> str:
        async with self._lock:
            self._jobs[job.id] = job
            return job.id

    async def get_job(self, job_id: str) -> Optional[DownloadJob]:
        async with self._lock:
            return self._jobs.get(job_id)

    async def update_job_status(
        self, 
        job_id: str, 
        status: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        async with self._lock:
            if job_id in self._jobs:
                job = self._jobs[job_id]
                job.status = status
                if metadata:
                    job.metadata.update(metadata)
                job.metadata['updated_at'] = datetime.utcnow().isoformat()

    async def list_jobs(self, status: Optional[str] = None) -> List[DownloadJob]:
        async with self._lock:
            jobs = list(self._jobs.values())
            if status:
                jobs = [j for j in jobs if j.status == status]
            return jobs