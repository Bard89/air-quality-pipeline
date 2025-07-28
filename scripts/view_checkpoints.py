#!/usr/bin/env python3
"""View checkpoint history for air quality downloads"""

import argparse
from pathlib import Path
from datetime import datetime
from src.core.checkpoint_manager import CheckpointManager

def format_size(size_mb: float) -> str:
    """Format file size in human-readable format"""
    if size_mb < 1024:
        return f"{size_mb:.1f} MB"
    else:
        return f"{size_mb/1024:.2f} GB"

def format_timestamp(timestamp: str) -> str:
    """Format ISO timestamp to readable format"""
    dt = datetime.fromisoformat(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def main():
    parser = argparse.ArgumentParser(description='View air quality download checkpoint history')
    parser.add_argument('--country', help='Filter by country code (e.g., JP)')
    parser.add_argument('--file', help='Show details for specific output file')
    
    args = parser.parse_args()
    
    checkpoint_dir = Path('data/openaq/checkpoints')
    manager = CheckpointManager(checkpoint_dir)
    
    if args.file:
        # Show details for specific file
        checkpoint = manager.find_checkpoint_for_file(args.file)
        if checkpoint:
            print(f"\nCheckpoint details for: {args.file}")
            print(f"Country: {checkpoint['country_code']}")
            print(f"Progress: {checkpoint['location_index']}/{checkpoint['total_locations']} locations")
            print(f"Last update: {format_timestamp(checkpoint['timestamp'])}")
            print(f"Current location ID: {checkpoint.get('current_location_id', 'N/A')}")
        else:
            print(f"No checkpoint found for file: {args.file}")
    else:
        # List all downloads
        downloads = manager.list_downloads(args.country)
        
        if not downloads:
            print("No downloads found")
            return
        
        print("\n=== Air Quality Download History ===")
        print(f"{'File':<50} {'Size':<10} {'Progress':<15} {'Last Update':<20}")
        print("-" * 100)
        
        for dl in downloads:
            filename = Path(dl['output_file']).name
            size = format_size(dl['size_mb']) if dl['exists'] else "N/A"
            progress = f"{dl['last_location']}/{dl['total_locations']} ({dl['progress_pct']:.1f}%)"
            last_update = format_timestamp(dl['last_update'])
            
            status = "✓" if dl['exists'] else "✗"
            print(f"{status} {filename:<48} {size:<10} {progress:<15} {last_update:<20}")
        
        print(f"\nTotal downloads: {len(downloads)}")
        existing = sum(1 for dl in downloads if dl['exists'])
        print(f"Existing files: {existing}")

if __name__ == '__main__':
    main()