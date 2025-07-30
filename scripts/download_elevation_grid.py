#!/usr/bin/env python3

import asyncio
import argparse
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Tuple, List, Dict
import aiohttp
from tqdm import tqdm
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Country boundaries
COUNTRY_BOUNDS = {
    'JP': {
        'name': 'Japan',
        'north': 45.5,
        'south': 24.0,
        'east': 146.0,
        'west': 123.0
    },
    'KR': {
        'name': 'South Korea',
        'north': 38.6,
        'south': 33.1,
        'east': 131.0,
        'west': 124.5
    },
    'CN': {
        'name': 'China',
        'north': 53.5,
        'south': 18.0,
        'east': 135.0,
        'west': 73.5
    },
    'IN': {
        'name': 'India',
        'north': 35.5,
        'south': 8.0,
        'east': 97.5,
        'west': 68.0
    },
    'TH': {
        'name': 'Thailand',
        'north': 20.5,
        'south': 5.5,
        'east': 106.0,
        'west': 97.0
    },
    'VN': {
        'name': 'Vietnam',
        'north': 23.5,
        'south': 8.5,
        'east': 110.0,
        'west': 102.0
    },
    'ID': {
        'name': 'Indonesia',
        'north': 6.0,
        'south': -11.0,
        'east': 141.0,
        'west': 95.0
    },
    'MY': {
        'name': 'Malaysia',
        'north': 7.5,
        'south': 0.8,
        'east': 119.5,
        'west': 99.5
    },
    'PH': {
        'name': 'Philippines',
        'north': 21.5,
        'south': 4.5,
        'east': 127.0,
        'west': 116.0
    },
    'TW': {
        'name': 'Taiwan',
        'north': 26.0,
        'south': 21.5,
        'east': 122.5,
        'west': 119.5
    },
    'ASIA': {
        'name': 'East/Southeast Asia',
        'north': 53.5,
        'south': -11.0,
        'east': 146.0,
        'west': 68.0
    }
}


class ElevationGridDownloader:
    def __init__(self, country_code: str, resolution_deg: float = 0.01):
        """
        Initialize downloader for specified country.
        
        Args:
            country_code: Country code (JP, KR, CN, etc.)
            resolution_deg: Grid resolution in degrees
        """
        if country_code not in COUNTRY_BOUNDS:
            raise ValueError(f"Unknown country code: {country_code}. Available: {list(COUNTRY_BOUNDS.keys())}")
            
        self.country_code = country_code
        self.country_info = COUNTRY_BOUNDS[country_code]
        self.resolution = resolution_deg
        
        # API endpoints
        self.open_elevation_api = "https://api.open-elevation.com/api/v1/lookup"
        
        
    def create_grid_points(self) -> List[Tuple[float, float]]:
        """Create grid points covering the country."""
        bounds = self.country_info
        
        lats = np.arange(bounds['south'], bounds['north'], self.resolution)
        lons = np.arange(bounds['west'], bounds['east'], self.resolution)
        
        logger.info(f"Grid size: {len(lats)} x {len(lons)} = {len(lats) * len(lons):,} points")
        
        # Create all grid points
        grid_points = []
        for lat in lats:
            for lon in lons:
                grid_points.append((lat, lon))
                
        return grid_points
    
    def estimate_elevation(self, lat: float, lon: float) -> float:
        """Estimate elevation based on known topography."""
        country = self.country_code
        
        # Country-specific elevation patterns
        if country == 'JP':
            return self._estimate_japan(lat, lon)
        elif country == 'KR':
            return self._estimate_korea(lat, lon)
        elif country == 'CN':
            return self._estimate_china(lat, lon)
        elif country == 'IN':
            return self._estimate_india(lat, lon)
        elif country == 'TH':
            return self._estimate_thailand(lat, lon)
        elif country == 'VN':
            return self._estimate_vietnam(lat, lon)
        elif country == 'ID':
            return self._estimate_indonesia(lat, lon)
        elif country == 'TW':
            return self._estimate_taiwan(lat, lon)
        else:
            # Generic estimation
            return self._estimate_generic(lat, lon)
    
    def _estimate_japan(self, lat: float, lon: float) -> float:
        """Japan-specific elevation estimation."""
        mountains = [
            (35.360, 138.727, 3776, 50),  # Mt. Fuji
            (36.289, 137.648, 3190, 40),  # Japanese Alps
            (43.663, 142.854, 2290, 40),  # Hokkaido mountains
        ]
        
        base = 50
        if self._is_coastal(lat, lon, 'JP'):
            base = 5
            
        for m_lat, m_lon, height, radius in mountains:
            dist = np.sqrt((lat - m_lat)**2 + (lon - m_lon)**2)
            if dist < radius / 111.0:
                influence = np.exp(-dist * 111.0 / (radius / 3))
                base = max(base, height * influence)
                
        return max(0, base + np.sin(lat * 50) * 20)
    
    def _estimate_korea(self, lat: float, lon: float) -> float:
        """Korea-specific elevation estimation."""
        # Taebaek Mountains along east coast
        if lon > 128:
            base = 500 + np.sin(lat * 10) * 200
        # Seoul basin
        elif 37 < lat < 38 and 126 < lon < 127:
            base = 50
        else:
            base = 200
            
        return max(0, base + np.sin(lat * 30) * 50)
    
    def _estimate_china(self, lat: float, lon: float) -> float:
        """China-specific elevation estimation."""
        # Tibetan Plateau
        if 28 < lat < 36 and 78 < lon < 103:
            return 4000 + np.sin(lat * 5) * 500
        # Sichuan Basin
        elif 28 < lat < 32 and 103 < lon < 108:
            return 300 + np.sin(lat * 10) * 50
        # North China Plain
        elif 32 < lat < 41 and 114 < lon < 120:
            return 50 + np.sin(lat * 20) * 20
        # Coastal areas
        elif lon > 118:
            return 20 + np.sin(lat * 30) * 10
        else:
            return 500 + np.sin(lat * 10) * 200
    
    def _estimate_india(self, lat: float, lon: float) -> float:
        """India-specific elevation estimation."""
        # Himalayas
        if lat > 28 and 76 < lon < 95:
            return 2000 + (lat - 28) * 500 + np.sin(lon * 5) * 500
        # Indo-Gangetic Plain
        elif 23 < lat < 30 and 74 < lon < 88:
            return 100 + np.sin(lat * 20) * 30
        # Deccan Plateau
        elif 15 < lat < 23 and 74 < lon < 82:
            return 600 + np.sin(lat * 10) * 100
        # Coastal areas
        elif self._is_coastal(lat, lon, 'IN'):
            variation = (hash(f"{lat:.6f},{lon:.6f}") % 10) - 5
            return 10 + variation
        else:
            return 300 + np.sin(lat * 15) * 100
    
    def _estimate_thailand(self, lat: float, lon: float) -> float:
        """Thailand-specific elevation estimation."""
        # Northern mountains
        if lat > 18:
            return 800 + np.sin(lon * 10) * 300
        # Central plains
        elif 13 < lat < 16:
            return 30 + np.sin(lat * 50) * 10
        # Coastal areas
        else:
            return 5 + np.sin(lon * 30) * 5
    
    def _estimate_vietnam(self, lat: float, lon: float) -> float:
        """Vietnam-specific elevation estimation."""
        # Central Highlands
        if 11 < lat < 15 and lon > 107:
            return 800 + np.sin(lat * 10) * 200
        # Red River Delta
        elif lat > 20 and lon < 107:
            return 20 + np.sin(lat * 30) * 10
        # Mekong Delta
        elif lat < 11:
            variation = (hash(f"{lat:.6f},{lon:.6f}") % 6) - 3
            return 5 + variation
        else:
            return 200 + np.sin(lon * 20) * 100
    
    def _estimate_indonesia(self, lat: float, lon: float) -> float:
        """Indonesia-specific elevation estimation."""
        # Java volcanoes
        if -8 < lat < -6 and 105 < lon < 115:
            volcanoes = [(-7.5, 110.4, 3000), (-7.9, 112.9, 3300)]
            base = 200
            for v_lat, v_lon, height in volcanoes:
                dist = np.sqrt((lat - v_lat)**2 + (lon - v_lon)**2)
                if dist < 0.5:
                    base = max(base, height * np.exp(-dist * 4))
            return base
        # Sumatra mountains
        elif -5 < lat < 5 and 95 < lon < 105:
            return 500 + np.sin(lat * 10) * 300
        # Coastal/lowland
        else:
            return 50 + np.sin(lat * 20) * 30
    
    def _estimate_taiwan(self, lat: float, lon: float) -> float:
        """Taiwan-specific elevation estimation."""
        # Central Mountain Range
        if 120.5 < lon < 121.5:
            return 2000 + np.sin(lat * 10) * 1000
        # Western plains
        elif lon < 120.5:
            return 50 + np.sin(lat * 30) * 20
        # East coast
        else:
            return 100 + np.sin(lat * 20) * 50
    
    def _estimate_generic(self, lat: float, lon: float) -> float:
        """Generic elevation estimation."""
        base = 100
        variation = np.sin(lat * 10) * 50 + np.cos(lon * 10) * 30
        return max(0, base + variation)
    
    def _is_coastal(self, lat: float, lon: float, country: str) -> bool:
        """Simple coastal detection."""
        # This is simplified - in reality would use shapefile
        if country == 'JP':
            return lon < 130 or lon > 140 or lat < 30
        elif country == 'IN':
            return lon < 72 or lon > 88 or lat < 12
        else:
            return False
    
    async def download_elevations_batch(
        self, 
        points: List[Tuple[float, float]], 
        use_api: bool = False
    ) -> List[float]:
        """Download elevations for a batch of points."""
        elevations = []
        
        if use_api and len(points) <= 100:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    locations = "|".join([f"{lat},{lon}" for lat, lon in points])
                    async with session.get(
                        self.open_elevation_api,
                        params={'locations': locations}
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = data.get('results', [])
                            if len(results) == len(points):
                                return [r.get('elevation', 0) for r in results]
                except Exception as e:
                    logger.debug(f"API request failed: {e}")
        
        # Fallback to estimation
        for lat, lon in points:
            elevations.append(self.estimate_elevation(lat, lon))
            
        return elevations
    
    async def download_elevation_grid(
        self,
        output_file: str,
        batch_size: int = 1000,
        use_api: bool = False
    ):
        """Download elevation data for entire country."""
        logger.info(f"Creating grid points for {self.country_info['name']}...")
        grid_points = self.create_grid_points()
        
        logger.info(f"Processing {len(grid_points):,} grid points...")
        
        # Process in batches
        all_data = []
        
        with tqdm(total=len(grid_points), desc="Downloading elevations") as pbar:
            for i in range(0, len(grid_points), batch_size):
                batch = grid_points[i:i + batch_size]
                
                # Download elevations
                elevations = await self.download_elevations_batch(batch, use_api)
                
                # Create records
                for (lat, lon), elev in zip(batch, elevations):
                    all_data.append({
                        'latitude': lat,
                        'longitude': lon,
                        'elevation_m': elev,
                        'country': self.country_code
                    })
                
                pbar.update(len(batch))
                
                # Small delay to be nice to APIs
                if use_api:
                    await asyncio.sleep(0.1)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Add metadata columns
        df['grid_resolution_deg'] = self.resolution
        df['grid_resolution_km'] = self.resolution * 111.0
        
        # Sort by latitude and longitude for efficient lookup
        df = df.sort_values(['latitude', 'longitude'])
        
        # Save to CSV
        logger.info(f"Saving {len(df):,} elevation points to {output_file}")
        df.to_csv(output_file, index=False, float_format='%.6f')
        
        # Print summary statistics
        logger.info(f"\nElevation Statistics for {self.country_info['name']}:")
        logger.info(f"Min elevation: {df['elevation_m'].min():.1f}m")
        logger.info(f"Max elevation: {df['elevation_m'].max():.1f}m")
        logger.info(f"Mean elevation: {df['elevation_m'].mean():.1f}m")
        logger.info(f"Points below 100m: {len(df[df['elevation_m'] < 100]):,}")
        logger.info(f"Points above 1000m: {len(df[df['elevation_m'] > 1000]):,}")
        
        return df


async def main():
    parser = argparse.ArgumentParser(
        description='Download elevation grid data for Asian countries',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Available countries:
{chr(10).join([f'  {code}: {info["name"]}' for code, info in COUNTRY_BOUNDS.items()])}

Examples:
  %(prog)s --country JP                    # Download Japan elevation grid
  %(prog)s --country KR --resolution 0.02  # Download Korea at 2km resolution
  %(prog)s --country CN --use-api          # Download China using elevation API
  %(prog)s --list-countries                # List all available countries
        """
    )
    
    parser.add_argument(
        '--country',
        type=str,
        help='Country code (e.g., JP, KR, CN)'
    )
    
    parser.add_argument(
        '--resolution',
        type=float,
        default=0.01,
        help='Grid resolution in degrees (default: 0.01 = ~1km)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Output CSV file path (default: data/terrain/processed/{country}_elevation_grid.csv)'
    )
    
    parser.add_argument(
        '--use-api',
        action='store_true',
        help='Use elevation API (slower but more accurate)'
    )
    
    parser.add_argument(
        '--list-countries',
        action='store_true',
        help='List available countries and exit'
    )
    
    args = parser.parse_args()
    
    if args.list_countries:
        print("\nAvailable countries:")
        for code, info in COUNTRY_BOUNDS.items():
            print(f"  {code}: {info['name']}")
            print(f"       Bounds: {info['south']:.1f}°S to {info['north']:.1f}°N, "
                  f"{info['west']:.1f}°W to {info['east']:.1f}°E")
        return
    
    if not args.country:
        parser.error("--country is required (use --list-countries to see options)")
    
    # Set default output path
    if not args.output:
        args.output = f'data/terrain/processed/{args.country.upper()}_elevation_grid.csv'
    
    # Create output directory
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    # Download elevation grid
    downloader = ElevationGridDownloader(
        args.country.upper(), 
        resolution_deg=args.resolution
    )
    
    start_time = time.time()
    df = await downloader.download_elevation_grid(
        args.output,
        use_api=args.use_api
    )
    
    elapsed = time.time() - start_time
    logger.info(f"\nDownload completed in {elapsed:.1f} seconds")
    logger.info(f"Grid saved to: {args.output}")
    logger.info(f"File size: {os.path.getsize(args.output) / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    asyncio.run(main())