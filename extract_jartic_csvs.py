#!/usr/bin/env python3
import argparse
import zipfile
from pathlib import Path
import shutil
from tqdm import tqdm
import io

def extract_month_csvs(archive_path: Path, output_dir: Path):
    """Extract all CSV files from a JARTIC archive to a directory"""
    
    # Get month from archive name (e.g., jartic_typeB_2024_02.zip)
    parts = archive_path.stem.split('_')
    if len(parts) >= 4:
        year = parts[2]
        month = parts[3]
        month_dir = output_dir / f"{year}_{month}"
    else:
        month_dir = output_dir / archive_path.stem
    
    # Create output directory
    month_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n📦 Extracting {archive_path.name} to {month_dir}")
    
    extracted_count = 0
    
    with zipfile.ZipFile(archive_path, 'r') as main_zip:
        # Get all prefecture ZIP files
        prefecture_files = [f for f in main_zip.namelist() if f.endswith('.zip') and not f.startswith('__MACOSX')]
        
        print(f"Found {len(prefecture_files)} prefecture archives")
        
        # Process each prefecture
        for pref_file in tqdm(prefecture_files, desc="Extracting prefectures"):
            # Extract prefecture name from path
            pref_name = pref_file.split('/')[-1].replace('.zip', '') if '/' in pref_file else pref_file.replace('.zip', '')
            
            # Read prefecture ZIP
            pref_data = main_zip.read(pref_file)
            
            # Extract CSVs from prefecture ZIP
            with zipfile.ZipFile(io.BytesIO(pref_data), 'r') as pref_zip:
                csv_files = [f for f in pref_zip.namelist() if f.endswith('.csv')]
                
                for csv_file in csv_files:
                    # Create output filename
                    csv_name = csv_file.split('/')[-1] if '/' in csv_file else csv_file
                    output_path = month_dir / f"{pref_name}_{csv_name}"
                    
                    # Extract CSV
                    csv_data = pref_zip.read(csv_file)
                    with open(output_path, 'wb') as f:
                        f.write(csv_data)
                    
                    extracted_count += 1
    
    print(f"✅ Extracted {extracted_count} CSV files to {month_dir}")
    
    # Show directory size
    total_size = sum(f.stat().st_size for f in month_dir.glob('*.csv'))
    print(f"📊 Total size: {total_size / 1024 / 1024:.1f} MB")
    
    return month_dir, extracted_count


def main():
    parser = argparse.ArgumentParser(
        description='Extract all CSV files from JARTIC archives'
    )
    
    parser.add_argument('--archive', '-a', type=Path,
                       help='Specific archive to extract')
    parser.add_argument('--cache-dir', type=Path, default=Path("data/jartic/cache"),
                       help='Cache directory with archives')
    parser.add_argument('--output-dir', '-o', type=Path, default=Path("data/jartic/extracted"),
                       help='Output directory for CSV files')
    parser.add_argument('--all', action='store_true',
                       help='Extract all archives in cache directory')
    
    args = parser.parse_args()
    
    # Find archives to process
    if args.archive:
        archives = [args.archive] if args.archive.exists() else []
    elif args.all:
        archives = sorted(args.cache_dir.glob("jartic_typeB_*.zip"))
    else:
        parser.error("Specify --archive or --all")
    
    if not archives:
        print("❌ No archives found")
        return
    
    print(f"🚗 JARTIC CSV Extractor")
    print("="*60)
    print(f"Archives to extract: {len(archives)}")
    print(f"Output directory: {args.output_dir}")
    
    # Process each archive
    total_extracted = 0
    for archive in archives:
        month_dir, count = extract_month_csvs(archive, args.output_dir)
        total_extracted += count
    
    print(f"\n{'='*60}")
    print(f"EXTRACTION COMPLETE")
    print(f"Total CSV files extracted: {total_extracted}")
    print(f"Output directory: {args.output_dir}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()