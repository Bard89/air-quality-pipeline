from typing import List, Optional, AsyncIterator, Dict, Any
from datetime import datetime
from decimal import Decimal
import aiohttp
import asyncio
from ...domain.interfaces import DataSource
from ...domain.models import Location, Sensor, Measurement, Coordinates, ParameterType, MeasurementUnit
from ...domain.exceptions import DataSourceException, RateLimitException, NetworkException
import logging


logger = logging.getLogger(__name__)


class OpenAQDataSource(DataSource):
    def __init__(
        self, 
        api_keys: List[str],
        base_url: str = "https://api.openaq.org/v3",
        rate_limit_per_key: int = 60,
        timeout: int = 30
    ):
        self.api_keys = api_keys
        self.base_url = base_url
        self.rate_limit_per_key = rate_limit_per_key
        self.timeout = timeout
        self._sessions: List[aiohttp.ClientSession] = []
        self._key_index = 0
        self._last_request_times = [0.0] * len(api_keys)
        self._request_delay = 60.0 / rate_limit_per_key

    async def __aenter__(self):
        for api_key in self.api_keys:
            session = aiohttp.ClientSession(
                headers={'X-API-Key': api_key},
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
            self._sessions.append(session)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.gather(*[session.close() for session in self._sessions])

    async def _get_session(self) -> tuple[aiohttp.ClientSession, int]:
        key_index = self._key_index
        self._key_index = (self._key_index + 1) % len(self.api_keys)
        return self._sessions[key_index], key_index

    async def _rate_limit(self, key_index: int):
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self._last_request_times[key_index]
        
        if time_since_last < self._request_delay:
            await asyncio.sleep(self._request_delay - time_since_last)
        
        self._last_request_times[key_index] = asyncio.get_event_loop().time()

    async def _request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        session, key_index = await self._get_session()
        await self._rate_limit(key_index)
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    raise RateLimitException(
                        f"Rate limit exceeded for key {key_index}",
                        retry_after=retry_after
                    )
                
                if response.status >= 400:
                    text = await response.text()
                    raise NetworkException(
                        f"HTTP {response.status}: {text}",
                        url=url,
                        status_code=response.status
                    )
                
                return await response.json()
        
        except asyncio.TimeoutError:
            raise NetworkException(f"Request timeout: {url}", url=url)
        except aiohttp.ClientError as e:
            raise NetworkException(f"Network error: {str(e)}", url=url)

    async def list_countries(self) -> List[Dict[str, str]]:
        response = await self._request("/countries", {"limit": 200})
        return [
            {
                "code": country["code"],
                "name": country["name"],
                "locations": country.get("locations", 0),
                "sensors": country.get("sensors", 0)
            }
            for country in response.get("results", [])
        ]

    async def find_locations(
        self,
        country_code: Optional[str] = None,
        parameter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Location]:
        params = {"limit": limit or 1000}
        if country_code:
            params["country"] = country_code
        if parameter:
            params["parameter"] = parameter
        
        locations = []
        page = 1
        
        while True:
            params["page"] = page
            response = await self._request("/locations", params)
            results = response.get("results", [])
            
            if not results:
                break
            
            for loc in results:
                try:
                    location = Location(
                        id=str(loc["id"]),
                        name=loc["name"],
                        coordinates=Coordinates(
                            latitude=Decimal(str(loc["coordinates"]["latitude"])),
                            longitude=Decimal(str(loc["coordinates"]["longitude"]))
                        ),
                        city=loc.get("city"),
                        country=loc.get("country", ""),
                        provider=loc.get("provider"),
                        metadata={
                            "sensors": loc.get("sensors", []),
                            "firstUpdated": loc.get("firstUpdated"),
                            "lastUpdated": loc.get("lastUpdated"),
                            "measurements": loc.get("measurements", 0)
                        }
                    )
                    locations.append(location)
                except (KeyError, ValueError) as e:
                    logger.warning(f"Skipping invalid location: {e}")
            
            if limit and len(locations) >= limit:
                break
            
            page += 1
            if page > 10:
                break
        
        return locations[:limit] if limit else locations

    async def get_sensors(self, location: Location) -> List[Sensor]:
        sensors = []
        
        for sensor_data in location.metadata.get("sensors", []):
            try:
                parameter = ParameterType(sensor_data["parameter"])
                unit = self._map_unit(sensor_data.get("unit", ""))
                
                sensor = Sensor(
                    id=str(sensor_data["id"]),
                    location=location,
                    parameter=parameter,
                    unit=unit,
                    is_active=sensor_data.get("isActive", True),
                    metadata={
                        "lastValue": sensor_data.get("lastValue"),
                        "coverage": sensor_data.get("coverage")
                    }
                )
                sensors.append(sensor)
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping invalid sensor: {e}")
        
        return sensors

    async def stream_measurements(
        self,
        sensor: Sensor,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> AsyncIterator[Measurement]:
        params = {
            "sensors_id": sensor.id,
            "limit": 1000
        }
        
        if start_date:
            params["date_from"] = start_date.isoformat()
        if end_date:
            params["date_to"] = end_date.isoformat()
        
        page = 1
        max_pages = 16
        
        while page <= max_pages:
            params["page"] = page
            
            try:
                response = await self._request("/measurements", params)
                results = response.get("results", [])
                
                if not results:
                    break
                
                for meas in results:
                    try:
                        measurement = Measurement(
                            sensor=sensor,
                            timestamp=datetime.fromisoformat(
                                meas["datetime"].replace("Z", "+00:00")
                            ),
                            value=Decimal(str(meas["value"])),
                            metadata={
                                "coverage": meas.get("coverage"),
                                "page": page
                            }
                        )
                        yield measurement
                    except (KeyError, ValueError) as e:
                        logger.warning(f"Skipping invalid measurement: {e}")
                
                page += 1
                
            except NetworkException as e:
                if e.status_code == 504 and page > 16:
                    logger.warning(f"Timeout on page {page}, stopping")
                    break
                raise

    async def get_metadata(self) -> Dict[str, Any]:
        return {
            "name": "OpenAQ",
            "version": "v3",
            "base_url": self.base_url,
            "rate_limit": self.rate_limit_per_key * len(self.api_keys),
            "api_keys_count": len(self.api_keys)
        }

    def _map_unit(self, unit_str: str) -> MeasurementUnit:
        unit_mapping = {
            "µg/m³": MeasurementUnit.MICROGRAMS_PER_CUBIC_METER,
            "ppm": MeasurementUnit.PARTS_PER_MILLION,
            "ppb": MeasurementUnit.PARTS_PER_BILLION,
            "c": MeasurementUnit.CELSIUS,
            "f": MeasurementUnit.FAHRENHEIT,
            "%": MeasurementUnit.PERCENT,
            "hpa": MeasurementUnit.HECTOPASCALS,
            "m/s": MeasurementUnit.METERS_PER_SECOND,
            "degrees": MeasurementUnit.DEGREES
        }
        return unit_mapping.get(unit_str.lower(), MeasurementUnit.MICROGRAMS_PER_CUBIC_METER)