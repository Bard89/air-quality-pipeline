import json
import os
from dotenv import load_dotenv
from openaq_client import OpenAQClient


def fetch_locations_for_countries(client: OpenAQClient, country_ids: list, max_pages: int = 1):
    all_locations = []
    
    for page in range(1, max_pages + 1):
        print(f"Fetching locations page {page}...")
        try:
            response = client.get_locations(country_ids=country_ids, limit=100, page=page)
            
            # Save raw response
            identifier = f"countries_{'_'.join(map(str, country_ids))}_page{page}"
            filepath = client.save_raw_response(response, 'locations', identifier)
            print(f"Saved raw response to: {filepath}")
            
            # Extract locations
            locations = response.get('results', [])
            all_locations.extend(locations)
            
            # Check if there are more pages
            meta = response.get('meta', {})
            found = meta.get('found', '0')
            # Handle 'found' being a string like '>100'
            if isinstance(found, str) and found.startswith('>'):
                found = int(found[1:]) + 1
            else:
                found = int(found) if found else 0
            
            if found <= page * 100:
                break
                
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            break
    
    return all_locations


def main():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('OPENAQ_API_KEY')
    
    if not api_key:
        print("Error: OPENAQ_API_KEY not found in environment variables")
        print("Please create a .env file with your API key")
        print("Get your API key from: https://explore.openaq.org/register")
        return
    
    client = OpenAQClient(api_key=api_key)
    
    # Load country mapping
    with open('config/country_mapping.json', 'r') as f:
        country_mapping = json.load(f)
    
    # Test with just China first
    china_id = country_mapping['CN']['id']
    test_country_ids = [china_id]
    print(f"Fetching locations for China (ID: {china_id})")
    
    locations = fetch_locations_for_countries(client, test_country_ids, max_pages=1)
    
    print(f"\nFound {len(locations)} locations")
    if locations:
        print("\nFirst 3 locations:")
        for i, loc in enumerate(locations[:3]):
            print(f"\n{i+1}. {loc.get('name', 'Unknown')} ({loc.get('id', 'No ID')})")
            print(f"   Country: {loc.get('country', {}).get('code', 'Unknown')}")
            print(f"   City: {loc.get('locality', 'Unknown')}")
            print(f"   Coordinates: {loc.get('coordinates', {})}")
            sensors = loc.get('sensors', [])
            parameters = [s.get('parameter', {}).get('displayName', '') for s in sensors]
            print(f"   Parameters: {parameters}")


if __name__ == "__main__":
    main()