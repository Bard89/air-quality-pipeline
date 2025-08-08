#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import argparse
import zipfile
import io
import csv
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from tqdm import tqdm
import time
from src.infrastructure.data_reference import ExternalDataManager


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_prefecture_data(pref_data_tuple):
    import gc
    import tempfile
    pref_zip_name, pref_bytes = pref_data_tuple
    
    try:
        parts = pref_zip_name.split('_')
        prefecture = parts[1] if len(parts) >= 2 else "unknown"
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
        temp_path = temp_file.name
        writer = None
        record_count = 0
        
        with zipfile.ZipFile(io.BytesIO(pref_bytes)) as pref_zf:
            csv_files = [f for f in pref_zf.namelist() if f.endswith('.csv')]
            
            if not csv_files:
                return prefecture, []
            
            for csv_file in csv_files:
                with pref_zf.open(csv_file) as f:
                    content_bytes = f.read()
                    
                    for encoding in ['shift_jis', 'cp932', 'utf-8']:
                        try:
                            content = content_bytes.decode(encoding)
                            break
                        except:
                            continue
                    else:
                        continue
                    
                    lines = content.split('\n')
                    
                    for i, line in enumerate(lines):
                        if i == 0 or not line.strip():
                            continue
                        
                        cols = line.strip().split(',')
                        if len(cols) < 10:
                            continue
                        
                        try:
                            if writer is None:
                                writer = csv.writer(temp_file)
                            
                            row_data = [
                                cols[0] if len(cols) > 0 else '',  # timestamp
                                cols[1] if len(cols) > 1 else '',  # source_code
                                cols[2] if len(cols) > 2 else '',  # point_number
                                cols[3] if len(cols) > 3 else '',  # point_name
                                cols[4] if len(cols) > 4 else '',  # mesh_code
                                cols[5] if len(cols) > 5 else '',  # link_type
                                cols[6] if len(cols) > 6 else '',  # link_number
                                cols[7] if len(cols) > 7 else '',  # traffic_volume
                                cols[8] if len(cols) > 8 else '',  # distance
                                cols[9] if len(cols) > 9 else '',  # version
                                prefecture  # prefecture
                            ]
                            
                            writer.writerow(row_data)
                            record_count += 1
                            
                            if record_count % 10000 == 0:
                                temp_file.flush()
                        except Exception as e:
                            continue
        
        temp_file.close()
        gc.collect()
        return prefecture, temp_path, record_count
        
    except Exception as e:
        if 'temp_file' in locals():
            temp_file.close()
            import os
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
        gc.collect()
        return pref_zip_name, None, 0


def process_archive_parallel(archive_path: Path, num_workers: int = None):
    manager = ExternalDataManager()
    processed_path = manager.external_data_path / 'jartic' / 'processed'
    processed_path.mkdir(parents=True, exist_ok=True)
    
    year_month = archive_path.stem.replace('jartic_typeB_', '')
    output_file = processed_path / f"jartic_traffic_{year_month}.csv"
    
    if output_file.exists():
        logger.info(f"Removing existing file: {output_file}")
        output_file.unlink()
    
    if num_workers is None:
        num_workers = min(cpu_count() // 2, 4)
    
    logger.info(f"Processing {archive_path.name} with {num_workers} workers")
    logger.info(f"Output: {output_file.name}")
    
    start_time = time.time()
    total_records = 0
    processed_prefectures = 0
    failed_prefectures = []
    
    try:
        with zipfile.ZipFile(archive_path, 'r') as main_zf:
            prefecture_zips = [f for f in main_zf.namelist() if f.endswith('.zip')]
            total_prefectures = len(prefecture_zips)
            logger.info(f"Found {total_prefectures} prefecture archives")
            
            def read_prefecture_data(pref_zip_name):
                with main_zf.open(pref_zip_name) as pref_file:
                    return (pref_zip_name, pref_file.read())
            
            header_written = False
            batch_size = max(1, num_workers * 2)
            
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = None
                
                with ProcessPoolExecutor(max_workers=num_workers) as executor:
                    with tqdm(total=total_prefectures, desc="Processing prefectures") as pbar:
                        for i in range(0, total_prefectures, batch_size):
                            batch = prefecture_zips[i:i+batch_size]
                            
                            futures = {}
                            for pref_zip_name in batch:
                                data = read_prefecture_data(pref_zip_name)
                                future = executor.submit(process_prefecture_data, data)
                                futures[future] = pref_zip_name
                            
                            for future in as_completed(futures):
                                pref_name = futures[future]
                                
                                try:
                                    import os
                                    prefecture, temp_path, record_count = future.result(timeout=300)
                                    
                                    if temp_path and record_count > 0:
                                        if not header_written:
                                            writer = csv.writer(outfile)
                                            writer.writerow(['timestamp', 'source_code', 'point_number', 'point_name', 
                                                           'mesh_code', 'link_type', 'link_number', 'traffic_volume', 
                                                           'distance', 'version', 'prefecture'])
                                            header_written = True
                                        
                                        with open(temp_path, 'r', encoding='utf-8') as temp_file:
                                            temp_reader = csv.reader(temp_file)
                                            for row in temp_reader:
                                                if writer and len(row) >= 11:
                                                    writer.writerow(row)
                                        
                                        os.unlink(temp_path)
                                        
                                        total_records += record_count
                                        processed_prefectures += 1
                                        
                                        pbar.set_postfix({
                                            'Records': f'{total_records:,}',
                                            'Prefecture': prefecture[:10],
                                            'Success': processed_prefectures
                                        })
                                    else:
                                        failed_prefectures.append(pref_name)
                                    
                                except Exception as e:
                                    logger.error(f"Failed to process {pref_name}: {e}")
                                    failed_prefectures.append(pref_name)
                                
                                pbar.update(1)
                                
                                percent_complete = ((pbar.n / total_prefectures) * 100)
                                elapsed = time.time() - start_time
                                if pbar.n > 0:
                                    eta = (elapsed / pbar.n) * (total_prefectures - pbar.n)
                                    eta_min = int(eta / 60)
                                    eta_sec = int(eta % 60)
                                    pbar.set_description(
                                        f"Processing: {percent_complete:.1f}% ({pbar.n}/{total_prefectures}) | ETA: {eta_min}m {eta_sec}s"
                                    )
                            
                            del futures
    
    except Exception as e:
        logger.error(f"Failed to process archive: {e}")
        raise
    
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time / 60)
    seconds = int(elapsed_time % 60)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing Complete!")
    logger.info(f"{'='*60}")
    logger.info(f"Time taken: {minutes}m {seconds}s")
    logger.info(f"Prefectures processed: {processed_prefectures}/{total_prefectures}")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Output file: {output_file}")
    
    if output_file.exists():
        size_mb = output_file.stat().st_size / (1024 * 1024)
        logger.info(f"Output size: {size_mb:.2f} MB")
    
    if failed_prefectures:
        logger.warning(f"Failed prefectures ({len(failed_prefectures)}): {', '.join(failed_prefectures[:5])}")
    
    return total_records


def sample_archive(archive_path: Path):
    logger.info(f"Sampling {archive_path.name}")
    
    with zipfile.ZipFile(archive_path, 'r') as main_zf:
        prefecture_zips = [f for f in main_zf.namelist() if f.endswith('.zip')]
        
        if not prefecture_zips:
            logger.error("No prefecture archives found")
            return
        
        logger.info(f"Archive contains {len(prefecture_zips)} prefectures")
        logger.info(f"File size: {archive_path.stat().st_size / (1024**3):.2f} GB")
        
        first_pref = prefecture_zips[0]
        logger.info(f"\nSampling first prefecture: {first_pref}")
        
        with main_zf.open(first_pref) as pref_file:
            pref_data = pref_file.read()
        
        with zipfile.ZipFile(io.BytesIO(pref_data)) as pref_zf:
            csv_files = [f for f in pref_zf.namelist() if f.endswith('.csv')]
            
            if csv_files:
                with pref_zf.open(csv_files[0]) as f:
                    content_bytes = f.read()
                    
                    for encoding in ['shift_jis', 'cp932', 'utf-8']:
                        try:
                            content = content_bytes.decode(encoding)
                            break
                        except:
                            continue
                    
                    lines = content.split('\n')[:10]
                    
                    print("\nFirst 10 lines of data:")
                    print("-" * 60)
                    for i, line in enumerate(lines):
                        if i == 0:
                            cols = line.split(',')
                            print(f"Headers ({len(cols)} columns):")
                            for j, col in enumerate(cols[:10]):
                                print(f"  [{j}]: {col}")
                        else:
                            print(f"Line {i}: {line[:150]}...")


def main():
    parser = argparse.ArgumentParser(
        description='Process JARTIC traffic archives with parallel processing'
    )
    parser.add_argument('--archive', type=str, required=True,
                       help='Archive file to process (e.g., jartic_typeB_2023_01.zip)')
    parser.add_argument('--workers', type=int,
                       help='Number of parallel workers (default: auto-detect)')
    parser.add_argument('--sample', action='store_true',
                       help='Show sample data without processing')
    
    args = parser.parse_args()
    
    manager = ExternalDataManager()
    
    archive_path = Path(args.archive)
    if not archive_path.exists():
        archive_path = manager.external_data_path / 'jartic' / 'cache' / args.archive
    
    if not archive_path.exists():
        logger.error(f"Archive not found: {args.archive}")
        return 1
    
    if args.sample:
        sample_archive(archive_path)
        return 0
    
    try:
        records = process_archive_parallel(archive_path, args.workers)
        return 0 if records > 0 else 1
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())