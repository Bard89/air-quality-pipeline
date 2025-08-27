#!/usr/bin/env python3
import subprocess
import time
import logging
import json
import requests
import random
from typing import Optional, List, Set, Dict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MullvadManager:
    def __init__(self):
        # Country codes for Mullvad servers
        self.servers = [
            "us",  # United States
            "gb",  # United Kingdom  
            "ca",  # Canada
            "de",  # Germany
            "fr",  # France
            "nl",  # Netherlands
            "ch",  # Switzerland
            "se",  # Sweden
            "no",  # Norway
            "dk",  # Denmark
            "fi",  # Finland
            "pl",  # Poland
            "es",  # Spain
            "it",  # Italy
            "at",  # Austria
            "be",  # Belgium
            "cz",  # Czech Republic
            "ro",  # Romania
            "gr",  # Greece
            "pt",  # Portugal
            "ie",  # Ireland
            "au",  # Australia
            "nz",  # New Zealand
            "jp",  # Japan
            "sg",  # Singapore
            "hk",  # Hong Kong
        ]
        
        self.exhausted_servers: Set[str] = set()
        self.server_attempts: Dict[str, int] = {}
        self.server_last_used: Dict[str, datetime] = {}
        self.max_attempts_per_server = 3
        self.cooldown_minutes = 65
        self.current_server: Optional[str] = None
        
        # Check if mullvad is installed
        self.check_installation()
    
    def check_installation(self) -> bool:
        try:
            result = subprocess.run(
                ['mullvad', 'version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                logger.info(f"Mullvad CLI found: {result.stdout.strip()}")
                return True
            else:
                logger.error("Mullvad CLI not found. Install with: brew install --cask mullvad-vpn")
                return False
        except FileNotFoundError:
            logger.error("Mullvad CLI not found. Install with: brew install --cask mullvad-vpn")
            return False
        except Exception as e:
            logger.error(f"Error checking Mullvad installation: {e}")
            return False
    
    def get_current_ip(self) -> Optional[str]:
        try:
            response = requests.get('https://api.ipify.org?format=json', timeout=5)
            if response.status_code == 200:
                return response.json()['ip']
        except Exception as e:
            logger.warning(f"Failed to get current IP: {e}")
        return None
    
    def is_connected(self) -> bool:
        try:
            result = subprocess.run(
                ['mullvad', 'status'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                logger.debug(f"Status output: {output}")
                return 'Connected' in output or 'connected' in output
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check connection status: {e}")
            return False
    
    def get_current_location(self) -> Optional[str]:
        try:
            result = subprocess.run(
                ['mullvad', 'status'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                # Parse location from status output
                # Example: "Connected to us-nyc-wg-301 in New York, NY, USA"
                if 'Connected to' in output:
                    parts = output.split('Connected to')[1].strip()
                    server_name = parts.split(' ')[0]
                    # Extract country code from server name (e.g., "us" from "us-nyc-wg-301")
                    country_code = server_name.split('-')[0]
                    return country_code
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get current location: {e}")
            return None
    
    def disconnect(self) -> bool:
        try:
            logger.info("Disconnecting from Mullvad...")
            
            result = subprocess.run(
                ['mullvad', 'disconnect'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                time.sleep(2)
                self.current_server = None
                logger.info("Successfully disconnected")
                return True
            else:
                logger.warning(f"Disconnect failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")
            return False
    
    def connect(self, country_code: str) -> bool:
        try:
            logger.info(f"Connecting to {country_code}...")
            
            # First set the relay location
            result = subprocess.run(
                ['mullvad', 'relay', 'set', 'location', country_code],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.warning(f"Failed to set location {country_code}: {result.stderr}")
                return False
            
            # Then connect
            result = subprocess.run(
                ['mullvad', 'connect'],
                capture_output=True,
                text=True,
                timeout=15
            )
            
            if result.returncode == 0:
                # Wait for connection to establish
                time.sleep(5)
                
                if self.is_connected():
                    self.current_server = country_code
                    new_ip = self.get_current_ip()
                    logger.info(f"Connected to {country_code} with IP: {new_ip}")
                    return True
                else:
                    logger.warning(f"Connection to {country_code} reported success but not connected")
                    return False
            else:
                logger.warning(f"Failed to connect to {country_code}: {result.stderr}")
                return False
            
        except subprocess.TimeoutExpired:
            logger.error(f"Connection to {country_code} timed out")
            return False
        except Exception as e:
            logger.error(f"Error connecting to {country_code}: {e}")
            return False
    
    def switch_server(self) -> Optional[str]:
        available_servers = []
        current_time = datetime.now()
        
        for server in self.servers:
            # Skip exhausted servers
            if server in self.exhausted_servers:
                continue
                
            # Check cooldown
            if server in self.server_last_used:
                time_since_used = current_time - self.server_last_used[server]
                if time_since_used.total_seconds() < self.cooldown_minutes * 60:
                    continue
            
            # Check attempt count
            if self.server_attempts.get(server, 0) >= self.max_attempts_per_server:
                self.exhausted_servers.add(server)
                continue
                
            available_servers.append(server)
        
        if not available_servers:
            logger.error("All VPN servers exhausted!")
            return None
        
        # Randomize server selection to avoid patterns
        next_server = random.choice(available_servers)
        
        logger.info(f"Switching to server: {next_server}")
        logger.info(f"Available servers remaining: {len(available_servers)}")
        
        # Disconnect first if connected
        if self.is_connected():
            self.disconnect()
            time.sleep(2)
        
        if self.connect(next_server):
            self.server_attempts[next_server] = self.server_attempts.get(next_server, 0) + 1
            self.server_last_used[next_server] = current_time
            return next_server
        else:
            # Mark as exhausted if connection fails
            self.exhausted_servers.add(next_server)
            # Try next server recursively
            return self.switch_server()
    
    def reset_exhausted_servers(self):
        logger.info(f"Resetting {len(self.exhausted_servers)} exhausted servers")
        self.exhausted_servers.clear()
        self.server_attempts.clear()
    
    def get_status(self) -> Dict:
        return {
            'installed': self.check_installation(),
            'connected': self.is_connected(),
            'current_server': self.get_current_location(),
            'current_ip': self.get_current_ip() if self.is_connected() else None,
            'exhausted_servers': len(self.exhausted_servers),
            'total_servers': len(self.servers),
            'available_servers': len(self.servers) - len(self.exhausted_servers)
        }
    
    def list_available_locations(self):
        try:
            result = subprocess.run(
                ['mullvad', 'relay', 'list'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                print("Available Mullvad locations:")
                print(result.stdout)
            else:
                logger.error(f"Failed to list locations: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Error listing locations: {e}")
    
    def test_switching(self, num_switches: int = 3):
        logger.info(f"Testing VPN switching with {num_switches} switches")
        
        if not self.check_installation():
            logger.error("Mullvad not installed!")
            return False
        
        original_ip = self.get_current_ip()
        logger.info(f"Original IP: {original_ip}")
        
        successful_switches = 0
        
        for i in range(num_switches):
            logger.info(f"\n--- Switch {i+1}/{num_switches} ---")
            
            server = self.switch_server()
            if server:
                new_ip = self.get_current_ip()
                if new_ip != original_ip:
                    logger.info(f"✓ Successfully switched to {server}")
                    logger.info(f"  New IP: {new_ip}")
                    successful_switches += 1
                    time.sleep(5)  # Brief pause between switches
                else:
                    logger.warning(f"✗ IP didn't change after switch")
            else:
                logger.error("✗ Failed to switch server")
                break
        
        # Disconnect after testing
        self.disconnect()
        
        logger.info(f"\nTest complete: {successful_switches}/{num_switches} successful switches")
        return successful_switches == num_switches


if __name__ == "__main__":
    manager = MullvadManager()
    
    # Check status
    print("\nMullvad VPN Status:")
    print("-" * 40)
    status = manager.get_status()
    for key, value in status.items():
        print(f"{key}: {value}")
    
    # Offer to test switching
    if status['installed']:
        if input("\nTest VPN switching? (y/n): ").lower() == 'y':
            manager.test_switching(3)
    else:
        print("\nPlease install Mullvad first:")
        print("brew install --cask mullvad-vpn")
        print("Then login with: mullvad account login [account-number]")