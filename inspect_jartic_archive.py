#!/usr/bin/env python3
import zipfile
import io
import sys
from pathlib import Path

def inspect_archive(archive_path):
    """Inspect JARTIC archive contents"""
    
    if not Path(archive_path).exists():
        print(f"Archive not found: {archive_path}")
        print("\nSearching for JARTIC archives...")
        
        # Look for any JARTIC archives
        for p in Path('.').rglob('jartic*.zip'):
            print(f"Found: {p}")
        for p in Path('.').rglob('jartic*.tmp'):
            print(f"Found (partial): {p}")
        return
    
    print(f"Inspecting: {archive_path}\n")
    
    with zipfile.ZipFile(archive_path, 'r') as main_zf:
        file_list = main_zf.namelist()
        
        print(f"Total files: {len(file_list)}")
        print(f"Archive size: {Path(archive_path).stat().st_size / 1024 / 1024:.1f} MB\n")
        
        # JARTIC archives contain prefecture ZIPs
        prefecture_zips = [f for f in file_list if f.endswith('.zip') and 'typeB_' in f]
        
        print(f"Prefecture archives: {len(prefecture_zips)}")
        for pref_zip in prefecture_zips[:10]:
            info = main_zf.getinfo(pref_zip)
            prefecture = pref_zip.split('_')[1] if '_' in pref_zip else 'unknown'
            print(f"  {prefecture}: {info.file_size / 1024 / 1024:.1f} MB ({pref_zip})")
        
        if len(prefecture_zips) > 10:
            print(f"  ... and {len(prefecture_zips) - 10} more prefectures\n")
        
        # Sample content from first prefecture
        if prefecture_zips:
            first_pref = prefecture_zips[0]
            print(f"\nSampling from {first_pref}:")
            
            try:
                with main_zf.open(first_pref) as pref_file:
                    pref_data = pref_file.read()
                
                with zipfile.ZipFile(io.BytesIO(pref_data)) as pref_zf:
                    inner_files = pref_zf.namelist()
                    print(f"  Contains {len(inner_files)} files")
                    
                    for inner_file in inner_files:
                        if inner_file.endswith('.csv'):
                            print(f"\n  CSV file: {inner_file}")
                            with pref_zf.open(inner_file) as csv_file:
                                # Read first 2KB with Shift-JIS encoding
                                sample = csv_file.read(2048)
                                try:
                                    decoded = sample.decode('shift_jis')
                                except:
                                    decoded = sample.decode('utf-8', errors='ignore')
                                
                                lines = decoded.split('\n')[:10]
                                print("  First 10 lines:")
                                for i, line in enumerate(lines):
                                    if line.strip():
                                        print(f"    {i+1}: {line[:150]}")
                            break
            except Exception as e:
                print(f"  Error sampling: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        inspect_archive(sys.argv[1])
    else:
        # Try common locations
        inspect_archive("data/jartic/cache/jartic_typeB_2024_01.zip")