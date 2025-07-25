#!/usr/bin/env python3
"""Fix checkpoint history to resume from existing CSV file"""

import json
from pathlib import Path
from datetime import datetime

def main():
    # The file you want to resume
    target_csv = "data/openaq/processed/jp_airquality_all_20250723_152024.csv"
    
    # Check if file exists and get its size
    csv_path = Path(target_csv)
    if not csv_path.exists():
        print(f"Error: CSV file {target_csv} not found!")
        return
    
    file_size_mb = csv_path.stat().st_size / 1024 / 1024
    print(f"Found CSV file: {target_csv}")
    print(f"File size: {file_size_mb:.1f} MB")
    
    # Count lines to estimate progress
    print("Counting records...")
    with open(csv_path, 'r') as f:
        line_count = sum(1 for line in f) - 1  # Subtract header
    print(f"Records in file: {line_count:,}")
    
    # Based on your earlier info: ~16M records after 225 locations
    # That's roughly 71,000 records per location
    estimated_locations = min(225, int(line_count / 71000))
    
    print(f"Estimated locations completed: {estimated_locations}")
    
    # Update checkpoint file
    checkpoint_file = Path("data/openaq/checkpoints/checkpoint_jp_all_parallel.json")
    checkpoint_data = {
        "country_code": "JP",
        "location_index": estimated_locations,
        "total_locations": 1607,
        "completed_locations": [],  # Empty for now, will be filled as processing continues
        "output_file": target_csv,
        "current_location_id": None,
        "timestamp": datetime.now().isoformat()
    }
    
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)
    
    print(f"\nUpdated checkpoint to resume from location {estimated_locations}")
    
    # Create/update history file
    history_file = Path("data/openaq/checkpoints/checkpoint_history.json")
    history = {}
    
    if history_file.exists():
        with open(history_file, 'r') as f:
            history = json.load(f)
    
    # Add entry for this file
    if target_csv not in history:
        history[target_csv] = []
    
    history[target_csv].append({
        "checkpoint_file": str(checkpoint_file),
        "location_index": estimated_locations,
        "timestamp": datetime.now().isoformat(),
        "total_locations": 1607,
        "measurements_count": line_count
    })
    
    with open(history_file, 'w') as f:
        json.dump(history, f, indent=2)
    
    print(f"Updated checkpoint history")
    print(f"\nYou can now run:")
    print(f"python3 download_air_quality.py --country JP --country-wide --parallel")
    print(f"It will resume from location {estimated_locations + 1} and append to {target_csv}")

if __name__ == '__main__':
    main()