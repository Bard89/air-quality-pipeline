#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path
import time

def process_jartic_archives():
    data_dir = "/Volumes/Extreme SSD/LeWagon Data 2025 Bootcamp Data. backup/backup 30 07 2025"
    
    archives_2024 = [
        "jartic_typeB_2024_01.zip",
        "jartic_typeB_2024_02.zip",
        "jartic_typeB_2024_03.zip",
        "jartic_typeB_2024_04.zip",
        "jartic_typeB_2024_05.zip",
        "jartic_typeB_2024_06.zip",
        "jartic_typeB_2024_07.zip",
        "jartic_typeB_2024_08.zip",
        "jartic_typeB_2024_09.zip",
        "jartic_typeB_2024_10.zip",
        "jartic_typeB_2024_11.zip",
        "jartic_typeB_2024_12.zip",
    ]
    
    archives_2025 = [
        "jartic_typeB_2025_01.zip",
        "jartic_typeB_2025_02.zip",
        "jartic_typeB_2025_03.zip",
        "jartic_typeB_2025_04.zip",
        "jartic_typeB_2025_05.zip",
        "jartic_typeB_2025_06.zip",
    ]
    
    all_archives = archives_2024 + archives_2025
    
    print(f"Processing {len(all_archives)} archives from 2024-2025")
    print(f"Data directory: {data_dir}")
    print("=" * 60)
    
    successful = []
    failed = []
    
    for i, archive in enumerate(all_archives, 1):
        print(f"\n[{i}/{len(all_archives)}] Processing {archive}")
        print("-" * 40)
        
        start_time = time.time()
        
        cmd = [
            "python", 
            "scripts/process_jartic_parallel.py",
            "--archive", archive,
            "--data-dir", data_dir,
            "--workers", "4"
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            elapsed = time.time() - start_time
            minutes = int(elapsed / 60)
            seconds = int(elapsed % 60)
            
            print(f"✓ Successfully processed {archive} in {minutes}m {seconds}s")
            successful.append(archive)
            
            output_lines = result.stdout.split('\n')
            for line in output_lines:
                if 'Total records:' in line or 'Output size:' in line:
                    print(f"  {line.strip()}")
                    
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to process {archive}")
            print(f"  Error: {e.stderr[:200] if e.stderr else 'Unknown error'}")
            failed.append(archive)
            
        except FileNotFoundError:
            print("Error: Could not find the process_jartic_parallel.py script")
            print("Make sure you're running this from the project root directory")
            return 1
    
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE")
    print("=" * 60)
    print(f"Successfully processed: {len(successful)}/{len(all_archives)} archives")
    
    if successful:
        print("\n✓ Successful:")
        for archive in successful:
            print(f"  - {archive}")
    
    if failed:
        print("\n✗ Failed:")
        for archive in failed:
            print(f"  - {archive}")
        print(f"\nTo retry failed archives, run them individually:")
        for archive in failed:
            print(f'python scripts/process_jartic_parallel.py --archive {archive} --data-dir "{data_dir}"')
    
    print("\n" + "=" * 60)
    print("Next step: Run the aggregation script")
    print(f'python scripts/process_jartic_full.py --start 2023-06-01 --end 2025-06-30 --data-dir "{data_dir}"')
    
    return 0 if not failed else 1

if __name__ == "__main__":
    sys.exit(process_jartic_archives())