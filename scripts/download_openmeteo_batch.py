#!/usr/bin/env python3
import subprocess
import sys
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path

def download_openmeteo_with_rate_limit(start_month=None, end_month=None, wait_minutes=90):
    WAIT_TIME_SECONDS = wait_minutes * 60
    
    # Define all available months
    all_months = [
        ("2024-01-01", "2024-01-31", "January 2024", "2024-01"),
        ("2024-02-01", "2024-02-29", "February 2024", "2024-02"),
        ("2024-03-01", "2024-03-31", "March 2024", "2024-03"),
        ("2024-04-01", "2024-04-30", "April 2024", "2024-04"),
        ("2024-05-01", "2024-05-31", "May 2024", "2024-05"),
        ("2024-06-01", "2024-06-30", "June 2024", "2024-06"),
        ("2024-07-01", "2024-07-31", "July 2024", "2024-07"),
        ("2024-08-01", "2024-08-31", "August 2024", "2024-08"),
        ("2024-09-01", "2024-09-30", "September 2024", "2024-09"),
        ("2024-10-01", "2024-10-31", "October 2024", "2024-10"),
        ("2024-11-01", "2024-11-30", "November 2024", "2024-11"),
        ("2024-12-01", "2024-12-31", "December 2024", "2024-12"),
        ("2025-01-01", "2025-01-31", "January 2025", "2025-01"),
        ("2025-02-01", "2025-02-28", "February 2025", "2025-02"),
        ("2025-03-01", "2025-03-31", "March 2025", "2025-03"),
    ]
    
    # Create month lookup dictionary
    month_lookup = {month[3]: idx for idx, month in enumerate(all_months)}
    
    # Determine which months to download
    if start_month and end_month:
        if start_month not in month_lookup:
            print(f"Error: Invalid start month '{start_month}'. Use format YYYY-MM (e.g., 2024-06)")
            return 1
        if end_month not in month_lookup:
            print(f"Error: Invalid end month '{end_month}'. Use format YYYY-MM (e.g., 2024-12)")
            return 1
        
        start_idx = month_lookup[start_month]
        end_idx = month_lookup[end_month] + 1
        months_to_download = [m[:3] for m in all_months[start_idx:end_idx]]
    else:
        # Default to June-December 2024 if no parameters provided
        months_to_download = [m[:3] for m in all_months[5:12]]
    
    print("=" * 70)
    print("OpenMeteo Weather Data Download - Rate Limited")
    print("=" * 70)
    print(f"Downloading {len(months_to_download)} months:")
    for _, _, month_name in months_to_download:
        print(f"  - {month_name}")
    print(f"\nWait time between downloads: {WAIT_TIME_SECONDS//60} minutes")
    print(f"Estimated total time: ~{len(months_to_download) * (WAIT_TIME_SECONDS//60)} minutes")
    print("=" * 70)
    
    successful = []
    failed = []
    
    for i, (start_date, end_date, month_name) in enumerate(months_to_download, 1):
        print(f"\n[{i}/{len(months_to_download)}] Processing {month_name}")
        print(f"  Period: {start_date} to {end_date}")
        print("-" * 50)
        
        batch_start_time = time.time()
        
        cmd = [
            "python", 
            "scripts/download_weather_incremental.py",
            "--source", "openmeteo",
            "--country", "JP",
            "--start", start_date,
            "--end", end_date
        ]
        
        print(f"  Starting download at {datetime.now().strftime('%H:%M:%S')}")
        print(f"  Command: {' '.join(cmd)}")
        
        try:
            # Run subprocess with real-time output
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Print output in real-time
            for line in process.stdout:
                print(f"    {line.rstrip()}")
            
            # Wait for process to complete
            return_code = process.wait(timeout=3600)  # 1 hour timeout
            
            download_elapsed = time.time() - batch_start_time
            minutes = int(download_elapsed / 60)
            seconds = int(download_elapsed % 60)
            
            if return_code == 0:
                print(f"  ✓ Successfully downloaded {month_name} in {minutes}m {seconds}s")
                successful.append(month_name)
            else:
                print(f"  ✗ Failed to download {month_name} (exit code: {return_code})")
                failed.append(month_name)
                    
        except subprocess.TimeoutExpired:
            print(f"  ✗ Download timeout for {month_name} (exceeded 1 hour)")
            process.kill()
            failed.append(month_name)
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Download interrupted by user")
            print(f"  Completed: {len(successful)} months")
            print(f"  Remaining: {len(months_to_download) - i} months")
            return 1
            
        except Exception as e:
            print(f"  ✗ Unexpected error for {month_name}: {str(e)}")
            failed.append(month_name)
        
        # Wait before next download (except for the last one)
        if i < len(months_to_download):
            elapsed_since_start = time.time() - batch_start_time
            remaining_wait = max(0, WAIT_TIME_SECONDS - elapsed_since_start)
            
            if remaining_wait > 0:
                next_start = datetime.now() + timedelta(seconds=remaining_wait)
                print(f"\n  ⏳ Waiting {int(remaining_wait/60)}m {int(remaining_wait%60)}s until next download")
                print(f"     Next download will start at: {next_start.strftime('%H:%M:%S')}")
                
                # Show countdown every 5 minutes
                wait_intervals = int(remaining_wait / 300) + 1
                for interval in range(wait_intervals):
                    if interval > 0:
                        time.sleep(min(300, remaining_wait - (interval * 300)))
                        time_left = remaining_wait - (interval * 300) - 300
                        if time_left > 0:
                            print(f"     {int(time_left/60)}m {int(time_left%60)}s remaining...", end='\r')
                
                # Final short sleep for any remaining seconds
                final_wait = remaining_wait % 300
                if final_wait > 0:
                    time.sleep(final_wait)
            else:
                print(f"  Download took longer than wait period, proceeding immediately")
    
    print("\n" + "=" * 70)
    print("DOWNLOAD COMPLETE")
    print("=" * 70)
    print(f"Successfully downloaded: {len(successful)}/{len(months_to_download)} months")
    
    if successful:
        print("\n✓ Successful downloads:")
        for month in successful:
            print(f"  - {month}")
    
    if failed:
        print("\n✗ Failed downloads:")
        for month in failed:
            print(f"  - {month}")
        
        print(f"\nTo retry failed months individually:")
        for start_date, end_date, month_name in months_to_download:
            if month_name in failed:
                print(f'python scripts/download_weather_incremental.py --source openmeteo --country JP --start {start_date} --end {end_date}')
    
    print("\n" + "=" * 70)
    print("Next steps:")
    print("1. Check the data in: data/openmeteo/processed/")
    print("2. Process the data with the appropriate processor script")
    
    return 0 if not failed else 1

def main():
    parser = argparse.ArgumentParser(
        description='Download OpenMeteo weather data with rate limiting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download June to December 2024 (default)
  python scripts/download_openmeteo_batch.py
  
  # Download specific range
  python scripts/download_openmeteo_batch.py --start 2024-01 --end 2024-03
  
  # Download with custom wait time (minutes)
  python scripts/download_openmeteo_batch.py --start 2024-06 --end 2024-12 --wait 60
  
  # Download single month
  python scripts/download_openmeteo_batch.py --start 2024-11 --end 2024-11
        """
    )
    
    parser.add_argument('--start', type=str, default='2024-06',
                       help='Start month in YYYY-MM format (default: 2024-06)')
    parser.add_argument('--end', type=str, default='2024-12',
                       help='End month in YYYY-MM format (default: 2024-12)')
    parser.add_argument('--wait', type=int, default=90,
                       help='Wait time in minutes between downloads (default: 90)')
    
    args = parser.parse_args()
    
    return download_openmeteo_with_rate_limit(
        start_month=args.start,
        end_month=args.end,
        wait_minutes=args.wait
    )

if __name__ == "__main__":
    sys.exit(main())