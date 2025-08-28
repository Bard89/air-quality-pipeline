#!/usr/bin/env python3
import subprocess
import sys
import time
import argparse
import re
import json
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional, List

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.mullvad_manager import MullvadManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OpenMeteoMullvadDownloader:
    def __init__(self, start_month: str, end_month: str, vpn_enabled: bool = True, 
                 servers: Optional[List[str]] = None):
        self.start_month = start_month
        self.end_month = end_month
        self.vpn_enabled = vpn_enabled
        self.vpn_manager = MullvadManager() if vpn_enabled else None
        
        if servers and self.vpn_manager:
            # Use custom server list if provided (country codes)
            self.vpn_manager.servers = servers
        
        self.rate_limit_pattern = re.compile(r'(Rate limit hit!|HTTP.*429|429.*Too Many|Too Many Requests)', re.IGNORECASE)
        self.progress_pattern = re.compile(r'Processing.*?(\d+)/(\d+)')
        self.completed_pattern = re.compile(r'Download complete!', re.IGNORECASE)
        
        self.current_process = None
        self.checkpoint_file = Path('data/openmeteo/checkpoint_mullvad.json')
        self.completed_months = set()
        self.month_progress = {}  # Track progress within each month
        self.current_month = None
        
    def load_checkpoint(self):
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
                self.completed_months = set(data.get('completed_months', []))
                self.month_progress = data.get('month_progress', {})
                logger.info(f"Loaded checkpoint: {len(self.completed_months)} months completed")
                if self.month_progress:
                    logger.info(f"Partial progress: {self.month_progress}")
    
    def save_checkpoint(self):
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.checkpoint_file, 'w') as f:
            json.dump({
                'completed_months': list(self.completed_months),
                'month_progress': self.month_progress,
                'last_update': datetime.now().isoformat()
            }, f, indent=2)
    
    def download_month(self, month_str: str, resume_from: Optional[int] = None) -> tuple[bool, Optional[int]]:
        # Parse month string (e.g., "2024-06")
        year, month = month_str.split('-')
        
        # Determine last day of month
        if month == '02':
            last_day = '29' if int(year) % 4 == 0 else '28'
        elif month in ['04', '06', '09', '11']:
            last_day = '30'
        else:
            last_day = '31'
        
        start_date = f"{year}-{month}-01"
        end_date = f"{year}-{month}-{last_day}"
        
        cmd = [
            'python', 
            'scripts/download_weather_incremental.py',
            '--source', 'openmeteo',
            '--country', 'JP',
            '--start', start_date,
            '--end', end_date
        ]
        
        # Add resume parameter if we need to resume
        if resume_from is not None:
            cmd.extend(['--resume-from', str(resume_from)])
        
        logger.info(f"Starting download for {month_str}")
        if resume_from:
            logger.info(f"Resuming from location {resume_from}")
        logger.info(f"Command: {' '.join(cmd)}")
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            self.current_process = process
            rate_limit_hit = False
            last_progress = None
            last_location_index = resume_from or 0
            
            for line in process.stdout:
                print(f"  {line.rstrip()}")
                
                # Check for rate limit errors
                if self.rate_limit_pattern.search(line):
                    logger.warning(f"Rate limit detected: {line.strip()}")
                    rate_limit_hit = True
                    # Extract current progress before killing
                    progress_match = self.progress_pattern.search(line)
                    if progress_match:
                        last_location_index = int(progress_match.group(1))
                    process.terminate()
                    break
                
                # Track progress
                progress_match = self.progress_pattern.search(line)
                if progress_match:
                    last_progress = f"{progress_match.group(1)}/{progress_match.group(2)}"
                    last_location_index = int(progress_match.group(1))
                
                # Check for completion
                if self.completed_pattern.search(line):
                    logger.info(f"Month {month_str} completed successfully")
            
            return_code = process.wait(timeout=10) if not rate_limit_hit else -1
            
            if rate_limit_hit:
                # Save progress for this month
                self.month_progress[month_str] = last_location_index
                self.save_checkpoint()
                return False, last_location_index  # Need VPN switch, return progress
            elif return_code == 0:
                self.completed_months.add(month_str)
                # Clear progress for completed month
                if month_str in self.month_progress:
                    del self.month_progress[month_str]
                self.save_checkpoint()
                return True, None
            else:
                logger.error(f"Download failed with return code: {return_code}")
                return False, last_location_index
                
        except subprocess.TimeoutExpired:
            logger.error("Process timeout")
            process.kill()
            return False, last_location_index
        except Exception as e:
            logger.error(f"Error during download: {e}")
            return False, last_location_index
        finally:
            self.current_process = None
    
    def get_months_to_download(self):
        # Generate list of months between start and end
        months = []
        
        start_year, start_month = map(int, self.start_month.split('-'))
        end_year, end_month = map(int, self.end_month.split('-'))
        
        current_year = start_year
        current_month = start_month
        
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            month_str = f"{current_year:04d}-{current_month:02d}"
            if month_str not in self.completed_months:
                months.append(month_str)
            
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
        
        return months
    
    def run(self):
        logger.info("=" * 70)
        logger.info("OpenMeteo Download with Mullvad VPN Auto-Switching")
        logger.info("=" * 70)
        
        if self.vpn_enabled:
            if not self.vpn_manager.check_installation():
                logger.error("Mullvad VPN not installed!")
                logger.error("Install with: brew install --cask mullvad-vpn")
                logger.error("Then login with: mullvad account login [account-number]")
                return 1
            
            logger.info("VPN switching: ENABLED")
            logger.info(f"Available VPN servers: {len(self.vpn_manager.servers)}")
            
            # Get initial IP
            initial_ip = self.vpn_manager.get_current_ip()
            logger.info(f"Starting IP: {initial_ip}")
        else:
            logger.info("VPN switching: DISABLED")
        
        # Load checkpoint
        self.load_checkpoint()
        
        # Get months to download
        months = self.get_months_to_download()
        logger.info(f"Months to download: {len(months)}")
        
        if not months:
            logger.info("All months already completed!")
            return 0
        
        for month in months:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing month: {month}")
            logger.info(f"Progress: {len(self.completed_months)}/{len(months) + len(self.completed_months)} months")
            
            # Check if we have partial progress for this month
            resume_from = self.month_progress.get(month)
            if resume_from:
                logger.info(f"Resuming from location {resume_from}")
            
            attempts = 0
            max_attempts = len(self.vpn_manager.servers) if self.vpn_enabled else 1
            
            while attempts < max_attempts:
                attempts += 1
                
                if self.vpn_enabled and attempts > 1:
                    # Switch VPN after first attempt
                    logger.info(f"\nAttempt {attempts}/{max_attempts} - Switching VPN...")
                    
                    new_server = self.vpn_manager.switch_server()
                    if not new_server:
                        logger.error("No more VPN servers available!")
                        break
                    
                    logger.info(f"Connected to: {new_server}")
                    logger.info(f"New IP: {self.vpn_manager.get_current_ip()}")
                    time.sleep(5)  # Wait for connection to stabilize
                
                # Try download with resume support
                success, last_progress = self.download_month(month, resume_from=resume_from)
                
                if success:
                    logger.info(f"✓ Successfully downloaded {month}")
                    break
                else:
                    # Update resume point for next attempt
                    if last_progress:
                        resume_from = last_progress
                        logger.info(f"Will resume from location {resume_from} on next attempt")
                    
                    if not self.vpn_enabled:
                        logger.error(f"✗ Failed to download {month} (VPN switching disabled)")
                        break
                    logger.warning(f"Download failed, will retry with different VPN...")
            
            if attempts >= max_attempts:
                logger.error(f"Failed to download {month} after {attempts} attempts")
                logger.error("All VPN servers exhausted. Stopping.")
                break
        
        # Final cleanup
        if self.vpn_enabled:
            logger.info("\nDisconnecting VPN...")
            self.vpn_manager.disconnect()
        
        # Report
        logger.info("\n" + "=" * 70)
        logger.info("DOWNLOAD SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Completed months: {len(self.completed_months)}")
        logger.info(f"Remaining months: {len(months) - len([m for m in months if m in self.completed_months])}")
        
        if self.vpn_enabled:
            status = self.vpn_manager.get_status()
            logger.info(f"VPN servers used: {len(self.vpn_manager.server_attempts)}")
            logger.info(f"VPN servers exhausted: {status['exhausted_servers']}")
        
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Download OpenMeteo data with automatic Mullvad VPN switching on rate limits',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Prerequisites:
  1. Install Mullvad: brew install --cask mullvad-vpn
  2. Login: mullvad account login [account-number]

Examples:
  # Download with automatic VPN switching
  python scripts/download_openmeteo_mullvad.py --start 2024-06 --end 2024-12
  
  # Use specific VPN servers (country codes)
  python scripts/download_openmeteo_mullvad.py --start 2024-06 --end 2024-12 --servers "us,gb,ca,de,jp"
  
  # Disable VPN switching
  python scripts/download_openmeteo_mullvad.py --start 2024-06 --end 2024-12 --no-vpn
  
  # Test VPN functionality
  python scripts/download_openmeteo_mullvad.py --test-vpn
  
  # List available VPN locations
  python scripts/download_openmeteo_mullvad.py --list-locations
        """
    )
    
    parser.add_argument('--start', type=str, default='2024-06',
                       help='Start month in YYYY-MM format (default: 2024-06)')
    parser.add_argument('--end', type=str, default='2024-12',
                       help='End month in YYYY-MM format (default: 2024-12)')
    parser.add_argument('--servers', type=str,
                       help='Comma-separated list of country codes (e.g., "us,gb,ca,de")')
    parser.add_argument('--no-vpn', action='store_true',
                       help='Disable automatic VPN switching')
    parser.add_argument('--test-vpn', action='store_true',
                       help='Test VPN switching functionality')
    parser.add_argument('--list-locations', action='store_true',
                       help='List available Mullvad VPN locations')
    
    args = parser.parse_args()
    
    # List locations mode
    if args.list_locations:
        manager = MullvadManager()
        manager.list_available_locations()
        return 0
    
    # Test mode
    if args.test_vpn:
        logger.info("Testing Mullvad VPN functionality...")
        manager = MullvadManager()
        success = manager.test_switching(3)
        return 0 if success else 1
    
    # Parse servers if provided
    servers = None
    if args.servers:
        servers = [s.strip().lower() for s in args.servers.split(',')]
    
    # Run downloader
    downloader = OpenMeteoMullvadDownloader(
        start_month=args.start,
        end_month=args.end,
        vpn_enabled=not args.no_vpn,
        servers=servers
    )
    
    return downloader.run()


if __name__ == "__main__":
    sys.exit(main())