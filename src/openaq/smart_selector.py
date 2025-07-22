from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json


class SmartLocationSelector:
    def __init__(self, client):
        self.client = client
    
    def select_best_locations(self, locations: List[Dict], target_count: int = 20,
                            parameters: Optional[List[str]] = None,
                            min_date: Optional[str] = None) -> List[Dict]:
        """Select locations with best data coverage and sensor diversity"""
        
        scored_locations = []
        
        for location in locations:
            score = 0
            sensor_count = 0
            param_count = 0
            active_sensors = 0
            
            # Count sensors and parameters
            params_found = set()
            for sensor in location.get('sensors', []):
                param_name = sensor.get('parameter', {}).get('name')
                if parameters is None or param_name in parameters:
                    sensor_count += 1
                    params_found.add(param_name)
                    
                    # Check if sensor is active - look at location level dates
                    active_sensors += 1  # Count all sensors with matching parameters
            
            param_count = len(params_found)
            
            # Calculate score based on:
            # - Number of active sensors (most important)
            # - Parameter diversity
            # - Total sensors
            score = (active_sensors * 100) + (param_count * 10) + sensor_count
            
            # Bonus for locations with complete parameter sets
            if parameters and params_found == set(parameters):
                score += 50
            
            # Store location with score
            if active_sensors > 0:
                scored_locations.append({
                    'location': location,
                    'score': score,
                    'active_sensors': active_sensors,
                    'param_count': param_count,
                    'params': list(params_found)
                })
        
        # Sort by score and return top locations
        scored_locations.sort(key=lambda x: x['score'], reverse=True)
        
        selected = []
        selected_cities = set()
        
        # Select top locations, trying to get geographic diversity
        for item in scored_locations:
            location = item['location']
            city = location.get('locality', 'Unknown')
            
            # Prefer geographic diversity - limit locations per city
            if city in selected_cities and len([l for l in selected if l.get('locality') == city]) >= 3:
                continue
            
            selected.append(location)
            selected_cities.add(city)
            
            if len(selected) >= target_count:
                break
        
        print(f"\nSelected {len(selected)} high-quality locations:")
        for i, loc in enumerate(selected[:5]):
            item = next(x for x in scored_locations if x['location']['id'] == loc['id'])
            print(f"{i+1}. {loc.get('name')} - {loc.get('locality')}")
            print(f"   Active sensors: {item['active_sensors']}, Parameters: {', '.join(item['params'])}")
        
        if len(selected) > 5:
            print(f"... and {len(selected) - 5} more locations")
        
        return selected
    
    def estimate_data_volume(self, locations: List[Dict], start_date: datetime, 
                           end_date: datetime) -> Dict:
        """Estimate data volume and API requests needed"""
        
        total_sensors = sum(len(loc.get('sensors', [])) for loc in locations)
        days = (end_date - start_date).days
        
        # Estimate based on typical data density
        measurements_per_sensor_per_day = 24  # Hourly data
        total_measurements = total_sensors * days * measurements_per_sensor_per_day
        
        # API request estimation
        measurements_per_request = 1000
        chunk_days = 90
        
        # Batch mode (multiple locations per request)
        locations_per_request = 10
        requests_batch = len(locations) // locations_per_request + 1
        chunks_needed = (days + chunk_days - 1) // chunk_days
        total_requests_batch = requests_batch * chunks_needed
        
        # Individual mode (one sensor per request)
        requests_individual = total_sensors * chunks_needed
        
        return {
            'locations': len(locations),
            'total_sensors': total_sensors,
            'days': days,
            'estimated_measurements': total_measurements,
            'estimated_requests_batch': total_requests_batch,
            'estimated_requests_individual': requests_individual,
            'estimated_time_batch_minutes': total_requests_batch * 1.5 / 60,
            'estimated_time_individual_minutes': requests_individual * 1.5 / 60
        }