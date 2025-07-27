import logging
import re
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm


logger = logging.getLogger(__name__)


class JARTICArchiveDownloader:
    def __init__(
        self,
        base_url: str = "http://storage.compusophia.com:1475/traffic",
        cache_dir: Path = Path("data/jartic/cache"),
        timeout: int = 3600,
        chunk_size: int = 1024 * 1024
    ):
        self.base_url = base_url.rstrip('/')
        self.cache_dir = Path(cache_dir)
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if not self._session:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def get_archive_index(self) -> List[Dict[str, Any]]:
        try:
            session = await self._ensure_session()
            index_url = f"{self.base_url}/typeB/"

            async with session.get(index_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch index: HTTP {response.status}")

                html = await response.text()

            soup = BeautifulSoup(html, 'html.parser')

            archives = []
            archive_pattern = re.compile(r'(\d{4})_(\d{2})\.zip')

            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)

                match = archive_pattern.search(href)
                if match:
                    year = int(match.group(1))
                    month = int(match.group(2))

                    archives.append({
                        'year': year,
                        'month': month,
                        'url': f"{index_url}{href}",
                        'filename': f"jartic_typeB_{year}_{month:02d}.zip",
                        'text': f"{year}年{month}月",
                        'type': 'typeB'
                    })

            archives.sort(key=lambda x: (x['year'], x['month']), reverse=True)

            logger.info(f"Found {len(archives)} JARTIC typeB archives")
            return archives

        except Exception as e:
            logger.error(f"Failed to get archive index: {e}")
            raise

    async def download_archive(self, year: int, month: int) -> Path:
        filename = f"jartic_typeB_{year}_{month:02d}.zip"
        local_path = self.cache_dir / filename

        if local_path.exists():
            file_size = local_path.stat().st_size / (1024 * 1024)
            logger.info(f"Using cached archive: {filename} ({file_size:.1f} MB)")
            return local_path

        archives = await self.get_archive_index()
        archive_info = None

        for archive in archives:
            if archive['year'] == year and archive['month'] == month:
                archive_info = archive
                break

        if not archive_info:
            raise ValueError(f"No archive found for {year}-{month:02d}")

        logger.info(f"Downloading JARTIC archive for {year}-{month:02d}")
        logger.info(f"URL: {archive_info['url']}")

        session = await self._ensure_session()
        temp_path = local_path.with_suffix('.tmp')

        resume_pos = 0
        if temp_path.exists():
            resume_pos = temp_path.stat().st_size
            logger.info(f"Resuming partial download from {resume_pos / 1024 / 1024:.1f} MB")

        try:
            start_time = time.time()

            headers = {}
            if resume_pos > 0:
                headers['Range'] = f'bytes={resume_pos}-'

            async with session.get(archive_info['url'], headers=headers) as response:
                if resume_pos > 0 and response.status == 206:
                    logger.info("Server supports resume, continuing download...")
                elif resume_pos > 0 and response.status == 200:
                    logger.warning("Server doesn't support resume, starting from beginning...")
                    resume_pos = 0
                    temp_path.unlink()
                elif response.status not in (200, 206):
                    raise Exception(f"Download failed: HTTP {response.status}")

                if response.status == 206:
                    content_range = response.headers.get('Content-Range', '')
                    if content_range:
                        total_size = int(content_range.split('/')[-1])
                    else:
                        total_size = int(response.headers.get('Content-Length', 0)) + resume_pos
                else:
                    total_size = int(response.headers.get('Content-Length', 0))

                downloaded = resume_pos

                progress_bar = tqdm(
                    total=total_size,
                    initial=resume_pos,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"📥 Download {year}-{month:02d}",
                    position=4,
                    leave=False,
                    bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                )

                mode = 'ab' if resume_pos > 0 else 'wb'
                with open(temp_path, mode) as f:
                    async for chunk in response.content.iter_chunked(self.chunk_size):
                        f.write(chunk)
                        chunk_size = len(chunk)
                        downloaded += chunk_size
                        progress_bar.update(chunk_size)

                progress_bar.close()

                elapsed = time.time() - start_time
                speed = ((downloaded - resume_pos) / 1024 / 1024) / elapsed if elapsed > 0 else 0
                logger.info(f"Downloaded {downloaded / 1024 / 1024:.1f} MB in {elapsed:.1f}s ({speed:.1f} MB/s)")

            if not self._verify_zip(temp_path):
                raise Exception("Downloaded file is not a valid ZIP archive")

            temp_path.rename(local_path)
            logger.info(f"Successfully downloaded archive to {local_path}")

            return local_path

        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            logger.error(f"Failed to download archive: {e}")
            raise

    async def download_archives_range(
        self,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int
    ) -> List[Path]:
        downloaded_paths = []

        current_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        while current_date <= end_date:
            try:
                path = await self.download_archive(
                    current_date.year,
                    current_date.month
                )
                downloaded_paths.append(path)
            except Exception as e:
                logger.warning(
                    f"Failed to download archive for "
                    f"{current_date.year}-{current_date.month:02d}: {e}"
                )

            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

        return downloaded_paths

    def _verify_zip(self, path: Path) -> bool:
        try:
            with zipfile.ZipFile(path, 'r') as zf:
                _ = zf.testzip()
                return True
        except Exception:
            return False

    async def get_archive_info(self, archive_path: Path) -> Dict[str, Any]:
        try:
            file_stats = archive_path.stat()

            with zipfile.ZipFile(archive_path, 'r') as zf:
                file_list = zf.namelist()
                total_uncompressed = sum(zf.getinfo(f).file_size for f in file_list)

            return {
                'path': str(archive_path),
                'size_mb': file_stats.st_size / 1024 / 1024,
                'uncompressed_size_mb': total_uncompressed / 1024 / 1024,
                'file_count': len(file_list),
                'modified': datetime.fromtimestamp(file_stats.st_mtime)
            }
        except Exception as e:
            logger.error(f"Failed to get archive info: {e}")
            return {}
