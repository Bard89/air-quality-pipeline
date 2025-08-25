#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import sys

def combine_jartic_csvs():
    base_dir = Path(__file__).parent.parent
    processed_dir = base_dir / 'data' / 'processed'
    
    file1 = processed_dir / 'jp_jartic_processed_20230101_to_20230630_full.csv'
    file2 = processed_dir / 'jp_jartic_processed_20230701_to_20231231_full.csv'
    output_file = processed_dir / 'jp_jartic_processed_20230101_to_20231231.csv'
    
    print("=" * 60)
    print("COMBINING JARTIC CSV FILES FOR FULL YEAR 2023")
    print("=" * 60)
    
    if not file1.exists():
        print(f"❌ File not found: {file1}")
        return 1
    
    if not file2.exists():
        print(f"❌ File not found: {file2}")
        return 1
    
    print(f"\n📂 Loading first half: {file1.name}")
    df1 = pd.read_csv(file1)
    print(f"   Rows: {len(df1):,}")
    print(f"   Date range: {df1['timestamp'].min()} to {df1['timestamp'].max()}")
    
    print(f"\n📂 Loading second half: {file2.name}")
    df2 = pd.read_csv(file2)
    print(f"   Rows: {len(df2):,}")
    print(f"   Date range: {df2['timestamp'].min()} to {df2['timestamp'].max()}")
    
    print("\n🔄 Combining datasets...")
    combined_df = pd.concat([df1, df2], ignore_index=True)
    
    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
    combined_df = combined_df.sort_values('timestamp')
    
    duplicate_check = combined_df.duplicated(subset=['timestamp', 'h3_index_res8'])
    if duplicate_check.any():
        print(f"\n⚠️  Found {duplicate_check.sum():,} duplicate timestamp-hexagon pairs")
        print("   Removing duplicates (keeping first occurrence)...")
        combined_df = combined_df.drop_duplicates(subset=['timestamp', 'h3_index_res8'], keep='first')
    
    print(f"\n✅ Combined dataset:")
    print(f"   Total rows: {len(combined_df):,}")
    print(f"   Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    print(f"   Unique hexagons: {combined_df['h3_index_res8'].nunique():,}")
    print(f"   Unique timestamps: {combined_df['timestamp'].nunique():,}")
    
    print(f"\n💾 Saving to: {output_file.name}")
    combined_df.to_csv(output_file, index=False)
    
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"   File size: {file_size_mb:.1f} MB")
    
    print("\n" + "=" * 60)
    print("✅ SUCCESSFULLY COMBINED JARTIC FILES FOR FULL YEAR 2023")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    sys.exit(combine_jartic_csvs())