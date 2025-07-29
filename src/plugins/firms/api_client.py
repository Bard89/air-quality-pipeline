from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import aiohttp
import asyncio
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)


class FIRMSAPIClient:
    def __init__(self, api_key: str, base_url: str = "https://firms.modaps.eosdis.nasa.gov"):
        self.api_key = api_key
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
        self.request_count = 0
        self.last_request_time = None
        self.transaction_limit = 5000  # per 10 minutes
        self.transaction_window = 600  # 10 minutes in seconds
        
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
        
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            
    async def check_api_status(self) -> Dict[str, Any]:
        """Check the current API key status"""
        status_url = f"https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY={self.api_key}"
        
        try:
            session = await self._get_session()
            async with session.get(status_url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return {"error": f"Status check failed: {response.status}"}
        except Exception as e:
            logger.error(f"Error checking API status: {e}")
            return {"error": str(e)}
            
    async def wait_if_rate_limited(self):
        """Check if we need to wait due to rate limits"""
        status = await self.check_api_status()
        
        if "current_transactions" in status:
            current_trans = int(status["current_transactions"])
            trans_limit = int(status.get("transaction_limit", 5000))
            
            # If we're at 80% of limit, wait
            if current_trans > trans_limit * 0.8:
                wait_time = 60  # Wait 1 minute
                logger.warning(f"Approaching transaction limit ({current_trans}/{trans_limit}). Waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                
        # Also implement local rate limiting
        if self.last_request_time:
            time_since_last = datetime.now(timezone.utc) - self.last_request_time
            if time_since_last.total_seconds() < 0.5:  # Min 0.5s between requests
                await asyncio.sleep(0.5)
            
    async def get_fire_data(
        self,
        satellite: str,  # "MODIS_NRT", "VIIRS_NOAA20_NRT", "VIIRS_SNPP_NRT", or archive versions
        area: Dict[str, float],  # {"west": lon, "east": lon, "north": lat, "south": lat}
        days_back: int = 1,
        date: Optional[datetime] = None,
        use_archive: bool = False
    ) -> List[Dict[str, Any]]:
        session = await self._get_session()
        
        # Format area bounds (west,south,east,north)
        area_str = f"{area['west']},{area['south']},{area['east']},{area['north']}"
        
        # Determine date range
        if date:
            end_date = date
        else:
            end_date = datetime.now(timezone.utc)
            
        start_date = end_date - timedelta(days=days_back)
        
        # Format dates for API
        # FIRMS expects: days_back/end_date (if date provided)
        # If no date, it returns most recent data
        if use_archive or (date and date != datetime.now(timezone.utc).date()):
            # For historical data, append the end date
            date_str = f"{days_back}/{end_date.strftime('%Y-%m-%d')}"
        else:
            # For recent data, just use days back
            date_str = str(days_back)
        
        # Construct API URL
        # Map satellite to product name based on whether we want archive or NRT
        if use_archive:
            # For historical data older than 2 months, just use NRT products
            # FIRMS automatically serves archive data when requesting older dates
            if satellite == "MODIS_NRT" or satellite == "MODIS":
                product = "MODIS_NRT"
            elif satellite == "VIIRS_NOAA20_NRT" or satellite == "VIIRS_NOAA20":
                product = "VIIRS_NOAA20_NRT"
            elif satellite == "VIIRS_SNPP_NRT" or satellite == "VIIRS_SNPP":
                product = "VIIRS_SNPP_NRT"
            else:
                product = satellite
        else:
            # Near Real-Time products
            if satellite == "MODIS_NRT" or satellite == "MODIS":
                product = "MODIS_NRT"
            elif satellite == "VIIRS_NOAA20_NRT" or satellite == "VIIRS_NOAA20":
                product = "VIIRS_NOAA20_NRT"
            elif satellite == "VIIRS_SNPP_NRT" or satellite == "VIIRS_SNPP":
                product = "VIIRS_SNPP_NRT"
            else:
                raise ValueError(f"Unsupported satellite: {satellite}")
            
        url = f"{self.base_url}/api/area/csv/{self.api_key}/{product}/{area_str}/{date_str}"
        
        # Apply rate limiting
        await self.wait_if_rate_limited()
        
        try:
            logger.debug(f"FIRMS API URL: {url}")
            self.last_request_time = datetime.now(timezone.utc)
            self.request_count += 1
            
            async with session.get(url) as response:
                if response.status == 429:  # Rate limit exceeded
                    logger.warning("Rate limit exceeded (429). Waiting 60 seconds...")
                    await asyncio.sleep(60)
                    # Retry once
                    async with session.get(url) as retry_response:
                        if retry_response.status != 200:
                            return []
                        text = await retry_response.text()
                        return self._parse_csv_response(text)
                elif response.status != 200:
                    error_text = await response.text()
                    logger.error(f"FIRMS API error: {response.status}")
                    logger.error(f"Error response: {error_text[:200]}")
                    if response.status >= 500:  # Server error
                        logger.info("Server error, waiting 10 seconds before continuing...")
                        await asyncio.sleep(10)
                    return []
                    
                text = await response.text()
                return self._parse_csv_response(text)
                
        except Exception as e:
            logger.error(f"Error fetching FIRMS data: {e}")
            return []
            
    def _parse_csv_response(self, csv_text: str) -> List[Dict[str, Any]]:
        lines = csv_text.strip().split('\n')
        if len(lines) < 2:
            return []
            
        headers = lines[0].split(',')
        fires = []
        
        for line in lines[1:]:
            values = line.split(',')
            if len(values) != len(headers):
                continue
                
            fire_data = {}
            for i, header in enumerate(headers):
                # Clean header name
                header = header.strip().lower()
                value = values[i].strip()
                
                # Convert numeric fields
                if header in ['latitude', 'longitude', 'brightness', 'bright_t31', 
                             'frp', 'scan', 'track']:
                    try:
                        fire_data[header] = float(value)
                    except ValueError:
                        fire_data[header] = None
                elif header == 'confidence':
                    # Handle confidence as percentage for MODIS or numeric for VIIRS
                    try:
                        if '%' in value:
                            fire_data[header] = int(value.replace('%', ''))
                        else:
                            fire_data[header] = int(value)
                    except ValueError:
                        fire_data[header] = None
                elif header == 'acq_date':
                    try:
                        # Make timezone aware
                        fire_data[header] = datetime.strptime(value, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    except ValueError:
                        fire_data[header] = None
                elif header == 'acq_time':
                    fire_data[header] = value
                else:
                    fire_data[header] = value
                    
            # Combine date and time
            if fire_data.get('acq_date') and fire_data.get('acq_time'):
                time_str = fire_data['acq_time']
                # Convert HHMM format to HH:MM
                if len(time_str) == 4 and time_str.isdigit():
                    time_str = f"{time_str[:2]}:{time_str[2:]}"
                try:
                    fire_data['detection_time'] = datetime.combine(
                        fire_data['acq_date'].date(),
                        datetime.strptime(time_str, '%H:%M').time(),
                        tzinfo=timezone.utc
                    )
                except:
                    fire_data['detection_time'] = fire_data['acq_date']
                    
            fires.append(fire_data)
            
        return fires
        
    async def get_historical_fire_data(
        self,
        satellite: str,
        area: Dict[str, float],
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical fire data for a specific date range.
        Note: FIRMS allows max 10 days per request, so we chunk if needed.
        """
        all_fires = []
        current_date = start_date
        chunk_count = 0
        
        while current_date < end_date:
            # Calculate days for this chunk (max 10)
            days_in_chunk = min(10, (end_date - current_date).days + 1)
            
            # Fetch data for this chunk
            try:
                fires = await self.get_fire_data(
                    satellite=satellite,
                    area=area,
                    days_back=days_in_chunk,
                    date=current_date + timedelta(days=days_in_chunk - 1),
                    use_archive=True
                )
            except Exception as e:
                logger.error(f"Error fetching chunk {current_date} to {current_date + timedelta(days=days_in_chunk)}: {e}")
                fires = []
            
            # Filter to ensure we only get data within our date range
            for fire in fires:
                if fire.get('detection_time'):
                    if start_date <= fire['detection_time'] <= end_date:
                        all_fires.append(fire)
                        
            # Move to next chunk
            current_date += timedelta(days=10)
            chunk_count += 1
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(1.0)
            
            # Check API status periodically
            if chunk_count % 5 == 0:  # Every 5 chunks
                status = await self.check_api_status()
                if "current_transactions" in status:
                    logger.info(f"API status: {status['current_transactions']}/{status.get('transaction_limit', 5000)} transactions")
            
        return all_fires
        
    async def get_active_fires_by_country(
        self, 
        country_code: str,
        days_back: int = 1,
        satellites: Optional[List[str]] = None,
        use_archive: bool = False
    ) -> List[Dict[str, Any]]:
        # Define country bounding boxes (simplified for major countries)
        country_bounds = {
            "JP": {"west": 122.0, "east": 146.0, "north": 46.0, "south": 24.0},
            "KR": {"west": 124.0, "east": 132.0, "north": 39.0, "south": 33.0},
            "CN": {"west": 73.0, "east": 135.0, "north": 54.0, "south": 18.0},
            "IN": {"west": 68.0, "east": 97.0, "north": 36.0, "south": 6.0},
            "TH": {"west": 97.0, "east": 106.0, "north": 21.0, "south": 5.0},
            "ID": {"west": 95.0, "east": 141.0, "north": 6.0, "south": -11.0},
            "MY": {"west": 99.0, "east": 119.0, "north": 8.0, "south": 0.0},
            "VN": {"west": 102.0, "east": 110.0, "north": 24.0, "south": 8.0},
        }
        
        if country_code not in country_bounds:
            logger.warning(f"Country {country_code} not in predefined bounds")
            return []
            
        bounds = country_bounds[country_code]
        
        if satellites is None:
            satellites = ["MODIS_NRT", "VIIRS_SNPP_NRT"]
            
        all_fires = []
        for satellite in satellites:
            fires = await self.get_fire_data(satellite, bounds, days_back, use_archive=use_archive)
            # Add satellite info to each fire
            for fire in fires:
                fire['satellite'] = satellite
            all_fires.extend(fires)
            
        # Remove duplicates based on location and time
        unique_fires = []
        seen = set()
        
        for fire in all_fires:
            if fire.get('latitude') and fire.get('longitude') and fire.get('detection_time'):
                key = (
                    round(fire['latitude'], 3),
                    round(fire['longitude'], 3),
                    fire['detection_time'].strftime('%Y-%m-%d %H')
                )
                if key not in seen:
                    seen.add(key)
                    unique_fires.append(fire)
                    
        return unique_fires