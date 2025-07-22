import json
import os
from dotenv import load_dotenv
from openaq_client import OpenAQClient


def main():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('OPENAQ_API_KEY')
    
    if not api_key:
        print("Error: OPENAQ_API_KEY not found")
        return
    
    client = OpenAQClient(api_key=api_key)
    
    # Get all countries
    print("Fetching countries...")
    response = client.get_countries()
    
    # Save raw response
    filepath = client.save_raw_response(response, 'countries', 'all')
    print(f"Saved raw response to: {filepath}")
    
    # Load config to get target country codes
    with open('config/openaq_config.json', 'r') as f:
        config = json.load(f)
    
    target_codes = config['target_countries']
    countries = response.get('results', [])
    
    # Find our target countries
    country_mapping = {}
    for country in countries:
        if country.get('code') in target_codes:
            country_mapping[country['code']] = {
                'id': country['id'],
                'name': country['name'],
                'locations_count': country.get('locationsCount', 0)
            }
    
    print(f"\nFound {len(country_mapping)} target countries:")
    for code, info in country_mapping.items():
        print(f"{code}: {info['name']} (ID: {info['id']}, Locations: {info['locations_count']})")
    
    # Save country mapping
    mapping_path = 'config/country_mapping.json'
    with open(mapping_path, 'w') as f:
        json.dump(country_mapping, f, indent=2)
    print(f"\nSaved country mapping to: {mapping_path}")


if __name__ == "__main__":
    main()