import pandas as pd
from pathlib import Path


def summarize_sensor_data(csv_path: str):
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    print(f"=== Sensor Data Summary: {Path(csv_path).name} ===\n")
    print(f"Total measurements: {len(df):,}")
    print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"Unique sensors: {df['sensor_id'].nunique()}")
    print(f"Unique locations: {df['location_id'].nunique()}")
    
    print("\nSensor Details:")
    sensor_info = df.groupby(['sensor_id', 'location_name', 'latitude', 'longitude']).agg({
        'value': ['count', 'mean', 'min', 'max'],
        'datetime': ['min', 'max']
    }).round(2)
    
    for idx, row in sensor_info.iterrows():
        sensor_id, location, lat, lon = idx
        print(f"\nSensor {sensor_id}: {location}")
        print(f"  Position: {lat:.4f}, {lon:.4f}")
        print(f"  Measurements: {row['value']['count']}")
        print(f"  PM2.5 range: {row['value']['min']} - {row['value']['max']} µg/m³ (avg: {row['value']['mean']})")
        print(f"  Time range: {row['datetime']['min']} to {row['datetime']['max']}")
    
    distances = []
    coords = df[['sensor_id', 'latitude', 'longitude']].drop_duplicates()
    for i in range(len(coords)):
        for j in range(i+1, len(coords)):
            lat1, lon1 = coords.iloc[i][['latitude', 'longitude']]
            lat2, lon2 = coords.iloc[j][['latitude', 'longitude']]
            dist_km = ((lat2-lat1)**2 + (lon2-lon1)**2)**0.5 * 111
            distances.append(dist_km)
    
    if distances:
        print(f"\nSensor spacing: {min(distances):.1f} - {max(distances):.1f} km")


if __name__ == "__main__":
    summarize_sensor_data('data/openaq/processed/vietnam_sensors_june_2024.csv')