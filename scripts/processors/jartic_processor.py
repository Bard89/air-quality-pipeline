from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from functools import partial
import h3
from .base_processor import BaseProcessor
import sys
sys.path.append(str(Path(__file__).parent.parent))
from utils.mesh_converter import mesh_to_latlng

logger = logging.getLogger(__name__)

def process_chunk_parallel(chunk_data: Tuple[int, pd.DataFrame], H3_RESOLUTION_FINE: int) -> pd.DataFrame:
    chunk_num, chunk = chunk_data
    
    try:
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], format='%Y/%m/%d %H:%M')
        chunk['timestamp'] = chunk['timestamp'].dt.tz_localize('Asia/Tokyo').dt.tz_convert('UTC')
        
        # Vectorized mesh code conversion
        mesh_codes = chunk['mesh_code'].astype(str).values
        coordinates = np.array([mesh_to_latlng(code) if code else (np.nan, np.nan) 
                                for code in mesh_codes])
        
        chunk['latitude'] = coordinates[:, 0]
        chunk['longitude'] = coordinates[:, 1]
        
        chunk = chunk.dropna(subset=['latitude', 'longitude'])
        
        if chunk.empty:
            return pd.DataFrame()
        
        # Add H3 index
        chunk[f'h3_index_res{H3_RESOLUTION_FINE}'] = chunk.apply(
            lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], H3_RESOLUTION_FINE),
            axis=1
        )
        
        hex_centers = chunk[f'h3_index_res{H3_RESOLUTION_FINE}'].apply(h3.cell_to_latlng)
        chunk[f'h3_lat_res{H3_RESOLUTION_FINE}'] = hex_centers.apply(lambda x: x[0])
        chunk[f'h3_lon_res{H3_RESOLUTION_FINE}'] = hex_centers.apply(lambda x: x[1])
        
        chunk['timestamp_hour'] = chunk['timestamp'].dt.floor('h')
        
        h3_col = f'h3_index_res{H3_RESOLUTION_FINE}'
        
        # Aggregate to hexagon-hour
        aggregated = chunk.groupby(['timestamp_hour', h3_col]).agg({
            'traffic_volume': ['mean', 'max', 'std', 'count'],
            'distance': 'mean',
            'link_number': 'nunique',
            f'h3_lat_res{H3_RESOLUTION_FINE}': 'first',
            f'h3_lon_res{H3_RESOLUTION_FINE}': 'first',
            'prefecture': 'first'
        }).reset_index()
        
        aggregated.columns = ['_'.join(col).strip() if col[1] else col[0] 
                             for col in aggregated.columns.values]
        
        aggregated = aggregated.rename(columns={
            'timestamp_hour': 'timestamp',
            'traffic_volume_mean': 'avg_traffic_volume',
            'traffic_volume_max': 'max_traffic_volume',
            'traffic_volume_std': 'traffic_volume_std',
            'traffic_volume_count': 'measurement_count',
            'distance_mean': 'avg_distance',
            'link_number_nunique': 'unique_links',
            f'h3_lat_res{H3_RESOLUTION_FINE}_first': f'h3_lat_res{H3_RESOLUTION_FINE}',
            f'h3_lon_res{H3_RESOLUTION_FINE}_first': f'h3_lon_res{H3_RESOLUTION_FINE}',
            'prefecture_first': 'prefecture'
        })
        
        # Add derived metrics
        aggregated['avg_speed_kmh'] = np.where(
            aggregated['avg_traffic_volume'] > 0,
            60 - (aggregated['avg_traffic_volume'] * 0.5),
            60
        )
        aggregated['avg_speed_kmh'] = aggregated['avg_speed_kmh'].clip(lower=0, upper=100)
        
        congestion_calc = np.where(
            aggregated['max_traffic_volume'] > 0,
            aggregated['max_traffic_volume'] / 10,
            0
        )
        aggregated['congestion_index'] = np.clip(congestion_calc, 0, 10)
        
        return aggregated
        
    except Exception as e:
        logger.error(f"Error in parallel chunk {chunk_num}: {e}")
        return pd.DataFrame()

class JARTICProcessor(BaseProcessor):
    
    def __init__(self, country: str = 'JP', data_dir: Optional[Path] = None, 
                 max_rows: Optional[int] = None, n_workers: Optional[int] = None,
                 input_file: Optional[str] = None, enable_checkpoints: bool = True):
        super().__init__(country, data_dir)
        self.max_rows = max_rows
        self.n_workers = n_workers or min(mp.cpu_count() - 1, 8)
        self.input_file = input_file  # Allow specifying specific input file
        self.enable_checkpoints = enable_checkpoints
        self.checkpoint_dir = self.processed_dir / 'checkpoints'
        if self.enable_checkpoints:
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        if self.max_rows:
            logger.info(f"Processing limited to {self.max_rows:,} rows")
        else:
            logger.info("Processing entire file (no row limit)")
        logger.info(f"Using {self.n_workers} parallel workers")
        if self.input_file:
            logger.info(f"Using specified input file: {self.input_file}")
        if self.enable_checkpoints:
            logger.info(f"Checkpointing enabled at: {self.checkpoint_dir}")
    
    def get_source_name(self) -> str:
        return 'JARTIC'
    
    def get_raw_data_paths(self, start_date: datetime, end_date: datetime) -> List[Path]:
        # If specific file is provided, use it
        if self.input_file:
            input_path = Path(self.input_file)
            if input_path.exists():
                logger.info(f"Using specified input file: {input_path}")
                return [input_path]
            else:
                logger.error(f"Specified input file not found: {input_path}")
                return []
        
        # Otherwise, look for files in the standard location
        jartic_dir = self.data_dir / 'jartic' / 'processed'
        
        if not jartic_dir.exists():
            logger.warning(f"JARTIC processed directory not found: {jartic_dir}")
            return []
        
        pattern = "jartic_traffic_*.csv"
        all_files = list(jartic_dir.glob(pattern))
        
        relevant_files = []
        for file_path in all_files:
            try:
                filename = file_path.stem
                parts = filename.split('_')
                if len(parts) >= 3:
                    # Try to parse year and month from filename
                    # Format: jartic_traffic_YYYY_MM.csv or jartic_traffic_YYYYMM.csv
                    year_month = parts[2]
                    if '_' in year_month:
                        year_str = year_month.split('_')[0]
                        month_str = year_month.split('_')[1] if len(year_month.split('_')) > 1 else '01'
                    elif len(year_month) == 7:  # Format: 2023_01
                        year_str = year_month[:4]
                        month_str = year_month[5:7]
                    elif len(year_month) == 6:  # Format: 202301
                        year_str = year_month[:4]
                        month_str = year_month[4:6]
                    else:
                        year_str = year_month[:4]
                        month_str = parts[3] if len(parts) > 3 else '01'
                    
                    file_year = int(year_str)
                    file_month = int(month_str)
                    
                    file_start = datetime(file_year, file_month, 1)
                    if file_month == 12:
                        file_end = datetime(file_year + 1, 1, 1)
                    else:
                        file_end = datetime(file_year, file_month + 1, 1)
                    
                    if not (file_end < start_date or file_start > end_date):
                        relevant_files.append(file_path)
                        logger.info(f"Including file: {file_path.name}")
            except Exception as e:
                logger.warning(f"Could not parse date from filename {file_path}: {e}")
        
        return sorted(relevant_files)
    
    def process_raw_file(self, file_path: Path) -> pd.DataFrame:
        logger.info(f"Processing JARTIC file with parallel processing: {file_path}")
        
        # Get total file size for progress tracking
        file_size_gb = file_path.stat().st_size / (1024**3)
        estimated_total_rows = self._estimate_total_rows(file_path)
        
        print(f"\n📊 File size: {file_size_gb:.2f} GB")
        print(f"📈 Estimated rows: {estimated_total_rows:,}")
        print(f"⚡ Workers: {self.n_workers} parallel processes")
        print("="*60)
        
        chunk_size = 1000000
        total_rows = 0
        processed_hexagons = set()
        batch_size = self.n_workers * 2  # Process batches of chunks in parallel
        checkpoint_every = 50  # Save checkpoint every 50M rows
        start_time = datetime.now()
        
        # Set up checkpoint file
        checkpoint_file = None
        checkpoint_counter = 0
        if self.enable_checkpoints:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            checkpoint_file = self.checkpoint_dir / f"jartic_checkpoint_{timestamp}.csv"
        
        # Read chunks and prepare for parallel processing
        chunks_to_process = []
        all_results = []
        
        try:
            for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size,
                                                         dtype={'source_code': str})):
                chunks_to_process.append((chunk_num, chunk))
                total_rows += len(chunk)
                
                # Process batch when we have enough chunks
                if len(chunks_to_process) >= batch_size:
                    self._print_progress(total_rows, estimated_total_rows, start_time, len(all_results))
                    
                    with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
                        process_func = partial(process_chunk_parallel, 
                                              H3_RESOLUTION_FINE=self.H3_RESOLUTION_FINE)
                        
                        futures = [executor.submit(process_func, chunk_data) 
                                  for chunk_data in chunks_to_process]
                        
                        for future in as_completed(futures):
                            result = future.result()
                            if not result.empty:
                                all_results.append(result)
                    
                    chunks_to_process = []
                    
                    # Memory management: consolidate results periodically
                    if len(all_results) >= 20:
                        consolidated = self._consolidate_results(all_results)
                        all_results = [consolidated]
                        
                        # Update hexagon count
                        if 'h3_index_res8' in consolidated.columns:
                            processed_hexagons.update(consolidated['h3_index_res8'].unique())
                        
                        # Save checkpoint if enabled
                        if self.enable_checkpoints and total_rows >= (checkpoint_counter + 1) * checkpoint_every * 1000000:
                            self._save_checkpoint(consolidated, checkpoint_file, checkpoint_counter)
                            checkpoint_counter += 1
                            print(f"\n💾 Checkpoint #{checkpoint_counter} saved ({len(processed_hexagons):,} unique hexagons so far)")
                
                if self.max_rows and total_rows >= self.max_rows:
                    logger.info(f"Reached max_rows limit of {self.max_rows:,} rows")
                    break
                
            
            # Process remaining chunks
            if chunks_to_process:
                logger.info(f"Processing final batch of {len(chunks_to_process)} chunks...")
                
                with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
                    process_func = partial(process_chunk_parallel, 
                                          H3_RESOLUTION_FINE=self.H3_RESOLUTION_FINE)
                    
                    futures = [executor.submit(process_func, chunk_data) 
                              for chunk_data in chunks_to_process]
                    
                    for future in as_completed(futures):
                        result = future.result()
                        if not result.empty:
                            all_results.append(result)
                
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            import traceback
            traceback.print_exc()
        
        if not all_results:
            logger.warning(f"No data processed from {file_path}")
            return pd.DataFrame()
        
        # Final consolidation
        print("\n" + "="*60)
        print("🎯 FINAL CONSOLIDATION")
        print("="*60)
        final_df = self._consolidate_results(all_results)
        
        # Calculate final statistics
        elapsed_time = (datetime.now() - start_time).total_seconds()
        rows_per_second = total_rows / elapsed_time if elapsed_time > 0 else 0
        
        # Save final checkpoint if we have data
        if self.enable_checkpoints and not final_df.empty and checkpoint_file:
            self._save_checkpoint(final_df, checkpoint_file, checkpoint_counter)
            
            # Rename checkpoint to indicate it's complete
            final_checkpoint = checkpoint_file.parent / f"{checkpoint_file.stem}_COMPLETE.csv"
            checkpoint_file.rename(final_checkpoint)
            print(f"✅ Processing complete. Final data saved at: {final_checkpoint}")
        
        final_df['data_source'] = 'jartic'
        final_df['country'] = self.country
        
        is_valid, issues = self.validate_data(final_df)
        if not is_valid:
            logger.warning(f"⚠️  Data validation issues: {issues}")
        
        # Print final summary
        print("\n" + "="*60)
        print("📊 PROCESSING SUMMARY")
        print("="*60)
        print(f"✅ Total rows processed: {total_rows:,}")
        print(f"✅ Output rows: {len(final_df):,}")
        print(f"✅ Compression ratio: {total_rows/len(final_df):.1f}:1")
        print(f"✅ Unique hexagons: {final_df['h3_index_res8'].nunique() if 'h3_index_res8' in final_df.columns else 0:,}")
        print(f"⏱️  Total time: {self._format_time(elapsed_time)}")
        print(f"⚡ Processing speed: {rows_per_second:.0f} rows/sec")
        print("="*60 + "\n")
        
        return final_df
    
    def _consolidate_results(self, results: List[pd.DataFrame]) -> pd.DataFrame:
        combined_df = pd.concat(results, ignore_index=True)
        
        # Remove duplicates before final aggregation
        h3_col = f'h3_index_res{self.H3_RESOLUTION_FINE}'
        combined_df = combined_df.drop_duplicates(subset=['timestamp', h3_col], keep='first')
        
        final_aggregated = combined_df.groupby(['timestamp', h3_col]).agg({
            'avg_traffic_volume': 'mean',
            'max_traffic_volume': 'max',
            'traffic_volume_std': 'mean',
            'measurement_count': 'sum',
            'avg_distance': 'mean',
            'unique_links': 'sum',
            'avg_speed_kmh': 'mean',
            'congestion_index': 'mean',
            f'h3_lat_res{self.H3_RESOLUTION_FINE}': 'first',
            f'h3_lon_res{self.H3_RESOLUTION_FINE}': 'first',
            'prefecture': 'first'
        }).reset_index()
        
        return final_aggregated
    
    def _save_checkpoint(self, df: pd.DataFrame, checkpoint_file: Path, counter: int):
        """Save intermediate results to checkpoint file"""
        try:
            if counter == 0:
                # First checkpoint - write with header
                df.to_csv(checkpoint_file, index=False, mode='w')
                logger.info(f"Created checkpoint file: {checkpoint_file}")
            else:
                # Append to existing checkpoint
                df.to_csv(checkpoint_file, index=False, mode='a', header=False)
                logger.info(f"Updated checkpoint file (checkpoint #{counter + 1})")
            
            file_size_mb = checkpoint_file.stat().st_size / (1024 * 1024)
            logger.info(f"Checkpoint size: {file_size_mb:.1f} MB, {len(df):,} rows saved")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self, checkpoint_file: Path) -> Optional[pd.DataFrame]:
        """Load a previously saved checkpoint"""
        if checkpoint_file.exists():
            try:
                df = pd.read_csv(checkpoint_file)
                logger.info(f"Loaded checkpoint with {len(df):,} rows from {checkpoint_file}")
                return df
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                return None
        return None
    
    def _estimate_total_rows(self, file_path: Path) -> int:
        """Estimate total rows based on file size"""
        # Rough estimate: ~70 bytes per row in the CSV
        file_size = file_path.stat().st_size
        estimated_rows = int(file_size / 70)
        return estimated_rows
    
    def _print_progress(self, current_rows: int, total_rows: int, start_time: datetime, results_count: int):
        """Print detailed progress information"""
        if total_rows == 0:
            return
            
        percentage = (current_rows / total_rows) * 100
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if elapsed > 0:
            rows_per_sec = current_rows / elapsed
            eta_seconds = (total_rows - current_rows) / rows_per_sec if rows_per_sec > 0 else 0
        else:
            rows_per_sec = 0
            eta_seconds = 0
        
        # Create progress bar
        bar_length = 40
        filled_length = int(bar_length * current_rows // total_rows)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        print(f"\n{'='*60}")
        print(f"📊 PROGRESS: [{bar}] {percentage:.1f}%")
        print(f"📈 Rows: {current_rows:,} / {total_rows:,}")
        print(f"⚡ Speed: {rows_per_sec:.0f} rows/sec")
        print(f"⏱️  Elapsed: {self._format_time(elapsed)}")
        print(f"⏳ ETA: {self._format_time(eta_seconds)}")
        print(f"📦 Batches processed: {results_count}")
        print(f"{'='*60}")
    
    def _format_time(self, seconds: float) -> str:
        """Format seconds into human-readable time"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            mins = seconds / 60
            secs = seconds % 60
            return f"{mins:.0f}m {secs:.0f}s"
        else:
            hours = seconds / 3600
            mins = (seconds % 3600) / 60
            return f"{hours:.0f}h {mins:.0f}m"