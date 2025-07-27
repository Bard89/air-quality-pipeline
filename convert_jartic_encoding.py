#!/usr/bin/env python3
import argparse
from pathlib import Path
from tqdm import tqdm
import shutil

def convert_csv_encoding(input_path: Path, output_path: Path):
    """Convert a CSV file from Shift-JIS to UTF-8"""
    try:
        # Read as Shift-JIS
        with open(input_path, 'r', encoding='shift_jis') as f:
            content = f.read()
        
        # Write as UTF-8
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True
    except Exception as e:
        print(f"❌ Error converting {input_path.name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Convert JARTIC CSV files from Shift-JIS to UTF-8 encoding'
    )
    
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                       help='Directory with Shift-JIS CSV files')
    parser.add_argument('--output-dir', '-o', type=Path,
                       help='Output directory for UTF-8 files (default: input_dir_utf8)')
    parser.add_argument('--in-place', action='store_true',
                       help='Convert files in place (backup originals)')
    
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        print(f"❌ Input directory not found: {args.input_dir}")
        return
    
    # Find all CSV files
    csv_files = list(args.input_dir.glob('**/*.csv'))
    if not csv_files:
        print(f"❌ No CSV files found in {args.input_dir}")
        return
    
    print(f"\n🔄 Converting JARTIC CSV Encoding")
    print("="*60)
    print(f"Files to convert: {len(csv_files)}")
    print(f"From: Shift-JIS → To: UTF-8")
    
    if args.in_place:
        print("Mode: In-place conversion (with backups)")
        backup_dir = args.input_dir.parent / f"{args.input_dir.name}_backup"
        backup_dir.mkdir(exist_ok=True)
        print(f"Backup directory: {backup_dir}")
    else:
        output_dir = args.output_dir or args.input_dir.parent / f"{args.input_dir.name}_utf8"
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Output directory: {output_dir}")
    
    # Convert each file
    converted = 0
    failed = 0
    
    for csv_file in tqdm(csv_files, desc="Converting files"):
        if args.in_place:
            # Backup original
            backup_path = backup_dir / csv_file.relative_to(args.input_dir)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(csv_file, backup_path)
            
            # Convert in place
            temp_path = csv_file.with_suffix('.tmp')
            if convert_csv_encoding(csv_file, temp_path):
                temp_path.replace(csv_file)
                converted += 1
            else:
                temp_path.unlink(missing_ok=True)
                failed += 1
        else:
            # Convert to new directory
            relative_path = csv_file.relative_to(args.input_dir)
            output_path = output_dir / relative_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if convert_csv_encoding(csv_file, output_path):
                converted += 1
            else:
                failed += 1
    
    print(f"\n{'='*60}")
    print("CONVERSION COMPLETE")
    print(f"Successfully converted: {converted}")
    if failed > 0:
        print(f"Failed: {failed}")
    
    # Show sample of converted content
    if converted > 0:
        print("\n📋 Sample of converted content:")
        sample_file = csv_files[0] if args.in_place else output_dir / csv_files[0].relative_to(args.input_dir)
        if sample_file.exists():
            with open(sample_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()[:5]
                for i, line in enumerate(lines):
                    print(f"  Line {i+1}: {line.strip()}")
    
    print(f"{'='*60}")


if __name__ == "__main__":
    main()