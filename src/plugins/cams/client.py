"""CDS API client wrapper for CAMS data access."""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import tempfile
import xarray as xr
import numpy as np
from dotenv import load_dotenv

try:
    import cdsapi
    HAS_CDSAPI = True
except ImportError:
    HAS_CDSAPI = False
    
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class CAMSClient:
    """Client for accessing CAMS data via Copernicus Data Store API."""
    
    def __init__(self, api_uid: Optional[str] = None, api_key: Optional[str] = None):
        """Initialize CAMS client.
        
        Args:
            api_uid: CDS API UID. If None, reads from CAMS_API_UID env variable
            api_key: CDS API key. If None, reads from CAMS_API_KEY env variable
        """
        if not HAS_CDSAPI:
            raise ImportError("cdsapi package not installed. Run: pip install cdsapi")
            
        # Get credentials from arguments or environment
        uid = api_uid or os.getenv('CAMS_API_UID')
        key = api_key or os.getenv('CAMS_API_KEY')
        
        if uid and key:
            # Use provided credentials - NOTE: API key should NOT include UID prefix
            self.client = cdsapi.Client(
                url='https://ads.atmosphere.copernicus.eu/api',  # Updated URL without /v2
                key=key  # Key only, no UID prefix
            )
            logger.info("Using CAMS credentials from environment variables")
        else:
            # Fall back to ~/.cdsapirc if exists
            cdsapirc_path = Path.home() / '.cdsapirc'
            if cdsapirc_path.exists():
                self.client = cdsapi.Client()
                logger.info("Using CAMS credentials from ~/.cdsapirc")
            else:
                raise ValueError(
                    "No CAMS API credentials found. Please set CAMS_API_UID and CAMS_API_KEY "
                    "in your .env file or create ~/.cdsapirc"
                )
            
        # Define the Japan area matching OpenMeteo coverage
        self.japan_area = [46, 123, 24, 146]  # North, West, South, East
        
    def download_reanalysis(
        self,
        date: datetime,
        variables: List[str] = None,
        area: List[float] = None,
        output_path: Optional[Path] = None
    ) -> Path:
        """Download CAMS reanalysis data (EAC4).
        
        Args:
            date: Date to download data for
            variables: List of variables to download. Default: ['particulate_matter_2.5um']
            area: [North, West, South, East] boundaries. Default: Japan area
            output_path: Where to save the NetCDF file
            
        Returns:
            Path to downloaded NetCDF file
        """
        if variables is None:
            variables = ['particulate_matter_2.5um']
            
        if area is None:
            area = self.japan_area
            
        if output_path is None:
            output_dir = Path('data/cams/raw')
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"cams_eac4_{date.strftime('%Y%m%d')}.nc"
            
        # Format request for CDS API
        request = {
            'format': 'netcdf',
            'variable': variables,
            'date': date.strftime('%Y-%m-%d'),
            'time': [f'{h:02d}:00' for h in range(0, 24, 3)],  # 3-hourly: 00, 03, 06, ..., 21
            'area': area,
        }
        
        logger.info(f"Downloading CAMS reanalysis for {date.date()}")
        
        # Download data
        self.client.retrieve(
            'cams-global-reanalysis-eac4',
            request,
            str(output_path)
        )
        
        logger.info(f"Downloaded to {output_path}")
        return output_path
        
    def download_forecast(
        self,
        base_time: datetime,
        lead_times: List[int] = None,
        variables: List[str] = None,
        area: List[float] = None,
        output_path: Optional[Path] = None
    ) -> Path:
        """Download CAMS forecast data.
        
        Args:
            base_time: Forecast base time (must be 00:00 or 12:00 UTC)
            lead_times: Forecast lead times in hours. Default: [0, 3, 6]
            variables: List of variables to download
            area: [North, West, South, East] boundaries
            output_path: Where to save the NetCDF file
            
        Returns:
            Path to downloaded NetCDF file
        """
        if lead_times is None:
            lead_times = [0, 3, 6, 9, 12]  # Up to 12 hours ahead
            
        if variables is None:
            variables = ['particulate_matter_2.5um', 'particulate_matter_10um']
            
        if area is None:
            area = self.japan_area
            
        # Ensure base_time is 00:00 or 12:00 UTC
        if base_time.hour not in [0, 12]:
            base_time = base_time.replace(hour=0 if base_time.hour < 12 else 12)
            logger.warning(f"Adjusted base time to {base_time}")
            
        if output_path is None:
            output_dir = Path('data/cams/forecast')
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"cams_forecast_{base_time.strftime('%Y%m%d_%H')}.nc"
            
        request = {
            'format': 'netcdf',
            'type': 'forecast',
            'date': base_time.strftime('%Y-%m-%d'),
            'time': base_time.strftime('%H:%M'),
            'leadtime_hour': lead_times,
            'variable': variables,
            'area': area,
        }
        
        logger.info(f"Downloading CAMS forecast from {base_time} for {lead_times} hours ahead")
        
        self.client.retrieve(
            'cams-global-atmospheric-composition-forecasts',
            request,
            str(output_path)
        )
        
        logger.info(f"Downloaded to {output_path}")
        return output_path
        
    def load_and_process(self, nc_path: Path) -> xr.Dataset:
        """Load NetCDF file and process for easier use.
        
        Args:
            nc_path: Path to NetCDF file
            
        Returns:
            xarray Dataset with processed data
        """
        ds = xr.open_dataset(nc_path)
        
        # Rename dimensions for consistency
        if 'longitude' in ds.dims:
            ds = ds.rename({'longitude': 'lon'})
        if 'latitude' in ds.dims:
            ds = ds.rename({'latitude': 'lat'})
        # Rename valid_time to time for consistency
        if 'valid_time' in ds.dims:
            ds = ds.rename({'valid_time': 'time'})
            
        # Convert PM2.5 units if needed (kg/m³ to µg/m³)
        if 'pm2p5' in ds.variables:
            # CAMS uses kg/m³, convert to µg/m³
            ds['pm2p5'] = ds['pm2p5'] * 1e9
            ds['pm2p5'].attrs['units'] = 'µg/m³'
            
        if 'pm10' in ds.variables:
            ds['pm10'] = ds['pm10'] * 1e9
            ds['pm10'].attrs['units'] = 'µg/m³'
            
        return ds
        
    def interpolate_to_locations(
        self,
        ds: xr.Dataset,
        locations: List[Dict[str, float]],
        variable: str = 'pm2p5'
    ) -> Dict[str, np.ndarray]:
        """Interpolate gridded data to specific locations.
        
        Args:
            ds: xarray Dataset with gridded data
            locations: List of dicts with 'lat' and 'lon' keys
            variable: Variable to interpolate
            
        Returns:
            Dictionary mapping location to time series values
        """
        results = {}
        
        for loc in locations:
            # Interpolate to location
            loc_data = ds[variable].interp(
                lat=loc['lat'],
                lon=loc['lon'],
                method='linear'
            )
            
            # Extract time series
            loc_id = f"{loc.get('name', '')}_{loc['lat']:.2f}_{loc['lon']:.2f}"
            results[loc_id] = loc_data.values
            
        return results