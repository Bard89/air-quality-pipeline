import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from openaq_client import OpenAQClient


def fetch_measurements_for_location(client: OpenAQClient, location_id: int, 
                                  location_name: str, hours_back: int = 24):
    # Calculate date range
    date_to = datetime.utcnow()
    date_from = date_to - timedelta(hours=hours_back)
    
    date_from_str = date_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    date_to_str = date_to.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    print(f"\nFetching measurements for {location_name} (ID: {location_id})")
    print(f"Date range: {date_from_str} to {date_to_str}")
    
    try:
        response = client.get_measurements(
            location_ids=[location_id],
            date_from=date_from_str,
            date_to=date_to_str,
            limit=100,
            page=1
        )
        
        # Save raw response
        identifier = f"location_{location_id}_last{hours_back}h"
        filepath = client.save_raw_response(response, 'measurements', identifier)
        print(f"Saved raw response to: {filepath}")
        
        measurements = response.get('results', [])
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
    
    measurements = fetch_measurements_for_location(
        client, test_location_id, test_location_name, hours_back=24
    )
    
    print(f"\nFound {len(measurements)} measurements")
    
    if measurements:
        # Group by parameter
        by_parameter = {}
        for m in measurements:
            param = m.get('parameter', {}).get('displayName', 'Unknown')
            if param not in by_parameter:
                by_parameter[param] = []
            by_parameter[param].append(m.get('value', 0))
        
        print("\nSummary by parameter:")
        for param, values in by_parameter.items():
            if values:
                avg_value = sum(values) / len(values)
                print(f"{param}: {len(values)} measurements, avg: {avg_value:.2f}")
        
        # Show first few measurements
        print("\nFirst 3 measurements:")
        for i, m in enumerate(measurements[:3]):
            print(f"\n{i+1}. {m.get('parameter', {}).get('displayName', 'Unknown')}")
            print(f"   Value: {m.get('value', 'N/A')} {m.get('parameter', {}).get('units', '')}")
            print(f"   Time: {m.get('period', {}).get('datetimeLocal', {}).get('utc', 'Unknown')}")


if __name__ == "__main__":
    main()