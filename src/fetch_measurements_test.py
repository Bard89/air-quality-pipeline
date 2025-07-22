import json
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from openaq_client import OpenAQClient


def test_latest_measurements(client: OpenAQClient, location_id: int, location_name: str):
    print(f"\nFetching latest measurements for {location_name} (ID: {location_id})")
    
    try:
        response = client.get_latest_measurements(location_id)
        
        # Save raw response
        identifier = f"location_{location_id}_latest"
        filepath = client.save_raw_response(response, 'latest', identifier)
        print(f"Saved raw response to: {filepath}")
        
        results = response.get('results', [])
        print(f"\nFound {len(results)} sensor latest values")
        
        for i, sensor_data in enumerate(results[:5]):
            sensor_id = sensor_data.get('sensorsId', 'Unknown')
            print(f"\n{i+1}. Sensor ID: {sensor_id}")
            print(f"   Latest value: {sensor_data.get('value', 'N/A')}")
            print(f"   Time: {sensor_data.get('datetime', {}).get('utc', 'Unknown')}")
            
        return results
        
    except Exception as e:
        print(f"Error fetching latest measurements: {e}")
        return []


def test_sensor_measurements(client: OpenAQClient, sensor_id: int, param_name: str, hours_back: int = 24, end_date: str = None):
    # Calculate date range
    if end_date:
        date_to = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    else:
        date_to = datetime.now(timezone.utc)
    date_from = date_to - timedelta(hours=hours_back)
    
    date_from_str = date_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    date_to_str = date_to.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    print(f"\nFetching measurements for sensor {sensor_id} ({param_name})")
    print(f"Date range: {date_from_str} to {date_to_str}")
    
    try:
        response = client.get_sensor_measurements(
            sensor_id=sensor_id,
            date_from=date_from_str,
            date_to=date_to_str,
            limit=100,
            page=1
        )
        
        # Save raw response
        identifier = f"sensor_{sensor_id}_last{hours_back}h"
        filepath = client.save_raw_response(response, 'measurements', identifier)
        print(f"Saved raw response to: {filepath}")
        
        measurements = response.get('results', [])
        print(f"Found {len(measurements)} measurements")
        
        if measurements:
            values = [m.get('value', 0) for m in measurements if m.get('value') is not None]
            if values:
                avg_value = sum(values) / len(values)
                print(f"Average value: {avg_value:.2f}")
                print(f"Min: {min(values):.2f}, Max: {max(values):.2f}")
        
        return measurements
        
    except Exception as e:
        print(f"Error fetching measurements: {e}")
        return []


def main():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('OPENAQ_API_KEY')
    
    if not api_key:
        print("Error: OPENAQ_API_KEY not found")
        return
    
    client = OpenAQClient(api_key=api_key)
    
    # Test with Beijing US Embassy (ID: 21)
    test_location_id = 21
    test_location_name = "Beijing US Embassy"
    
    # First get latest measurements to find active sensors
    latest_results = test_latest_measurements(client, test_location_id, test_location_name)
    
    # If we found sensors, test fetching historical data
    if latest_results and latest_results[0]:
        sensor_id = latest_results[0].get('sensorsId')
        last_datetime = latest_results[0].get('datetime', {}).get('utc')
        
        if sensor_id and last_datetime:
            print(f"\nNote: Latest data is from {last_datetime}")
            # Fetch data from around that time
            test_sensor_measurements(client, sensor_id, "PM2.5", hours_back=24*7, end_date=last_datetime)  # Get a week of data


if __name__ == "__main__":
    main()