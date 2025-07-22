import json
from typing import List, Dict, Optional
from src.openaq.client import OpenAQClient


class LocationFinder:
    def __init__(self, client: OpenAQClient):
        self.client = client
    
    def find_locations_in_country(self, country_code: str, country_mapping: Dict) -> List[Dict]:
        country_id = country_mapping.get(country_code, {}).get('id')
        if not country_id:
            raise ValueError(f"Country {country_code} not found")
        
        all_locations = []
        page = 1
        
        while page <= 5:
            response = self.client.get_locations(country_ids=[country_id], page=page)
            locations = response.get('results', [])
            
            if not locations:
                break
                
            all_locations.extend(locations)
            
            if len(locations) < 100:
                break
            
            page += 1
        
        return all_locations
    
    def extract_sensor_info(self, location: Dict) -> List[Dict]:
        sensors = []
        location_coords = location.get('coordinates', {})
        
        for sensor in location.get('sensors', []):
            sensor_info = {
                'sensor_id': sensor.get('id'),
                'location_id': location.get('id'),
                'location_name': location.get('name'),
                'city': location.get('locality'),
                'country': location.get('country', {}).get('code'),
                'latitude': location_coords.get('latitude'),
                'longitude': location_coords.get('longitude'),
                'parameter': sensor.get('parameter', {}).get('name'),
                'unit': sensor.get('parameter', {}).get('units'),
                'datetime_first': location.get('datetimeFirst', {}).get('utc') if location.get('datetimeFirst') else None,
                'datetime_last': location.get('datetimeLast', {}).get('utc') if location.get('datetimeLast') else None
            }
            sensors.append(sensor_info)
        
        return sensors
    
    def find_active_sensors(self, locations: List[Dict], parameter: Optional[str] = None, 
                           min_date: Optional[str] = None) -> List[Dict]:
        active_sensors = []
        
        for location in locations:
            sensors = self.extract_sensor_info(location)
            
            for sensor in sensors:
                if parameter is None or sensor['parameter'] == parameter:
                    if min_date is None or (sensor['datetime_last'] and sensor['datetime_last'] >= min_date):
                        active_sensors.append(sensor)
        
        return active_sensors