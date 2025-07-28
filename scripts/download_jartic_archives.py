#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
from tqdm import tqdm

from src.plugins.jartic.archive_downloader import JARTICArchiveDownloader


async def download_archives(start_year: int, start_month: int, end_year: int, end_month: int, cache_dir: Path):
    """Download JARTIC archives sequentially with clear progress"""
    
    downloader = JARTICArchiveDownloader(cache_dir=cache_dir)
    
    # Calculate months to download
    months_to_download = []
    current = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    
    while current <= end:
        months_to_download.append((current.year, current.month))
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    
    print(f"\n🚗 JARTIC Archive Downloader")
    print("="*60)
    print(f"Period: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    print(f"Archives to download: {len(months_to_download)}")
    print(f"Cache directory: {cache_dir}")
    print()
    
    # Progress bar for overall progress
    overall_progress = tqdm(
        total=len(months_to_download),
        desc="📊 Overall Progress",
        unit="archive",
        bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
    )
    
    downloaded_count = 0
    cached_count = 0
    failed_count = 0
    
    try:
        for year, month in months_to_download:
                month_str = f"{year}-{month:02d}"
                archive_path = cache_dir / f"jartic_typeB_{year}_{month:02d}.zip"
                
                try:
                    if archive_path.exists():
                        file_size_mb = archive_path.stat().st_size / (1024 * 1024)
                        tqdm.write(f"✓ Using cached {month_str} ({file_size_mb:.1f} MB)")
                        cached_count += 1
                    else:
                        tqdm.write(f"📥 Downloading {month_str}...")
                        # The download_archive method will show its own progress bar
                        await downloader.download_archive(year, month)
                        downloaded_count += 1
                        file_size_mb = archive_path.stat().st_size / (1024 * 1024)
                        tqdm.write(f"✅ Downloaded {month_str} ({file_size_mb:.1f} MB)")
                    
                    overall_progress.update(1)
                    
                except Exception as e:
                    failed_count += 1
                    tqdm.write(f"❌ Failed to download {month_str}: {str(e)}")
                    overall_progress.update(1)
    
    finally:
        overall_progress.close()
        await downloader.close()
    
    print(f"\n{'='*60}")
    print("DOWNLOAD SUMMARY")
    print(f"Total archives: {len(months_to_download)}")
    print(f"Downloaded: {downloaded_count}")
    print(f"Cached: {cached_count}")
    print(f"Failed: {failed_count}")
    print(f"Cache directory: {cache_dir}")
    print(f"{'='*60}")


def parse_date(date_str):
    try:
        dt = datetime.strptime(date_str, '%Y-%m')
        return dt.year, dt.month
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM")


def main():
    parser = argparse.ArgumentParser(
        description='Download JARTIC traffic archives sequentially',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --start 2024-01 --end 2024-12
  %(prog)s --start 2024-01 --end 2024-03 --cache-dir /custom/cache/path
        """
    )
    
    parser.add_argument('--start', '-s', type=parse_date, required=True,
                       help='Start date (YYYY-MM)')
    parser.add_argument('--end', '-e', type=parse_date, required=True,
                       help='End date (YYYY-MM)')
    parser.add_argument('--cache-dir', type=Path, default=Path("data/jartic/cache"),
                       help='Cache directory for archives (default: data/jartic/cache)')
    
    args = parser.parse_args()
    
    # Validate dates
    start_year, start_month = args.start
    end_year, end_month = args.end
    
    if (start_year, start_month) > (end_year, end_month):
        parser.error("Start date must be before or equal to end date")
    
    # Run the download
    asyncio.run(download_archives(start_year, start_month, end_year, end_month, args.cache_dir))


if __name__ == "__main__":
    main()