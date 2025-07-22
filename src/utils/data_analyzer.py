import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List
from datetime import datetime


class DataAnalyzer:
    def __init__(self, csv_path: str):
        self.df = pd.read_csv(csv_path)
        self.df['datetime'] = pd.to_datetime(self.df['datetime'])
        self.csv_path = Path(csv_path)
    
    def get_basic_stats(self) -> Dict:
        return {
            'file_name': self.csv_path.name,
            'total_measurements': len(self.df),
            'date_range': {
                'start': str(self.df['datetime'].min()),
                'end': str(self.df['datetime'].max()),
                'days': (self.df['datetime'].max() - self.df['datetime'].min()).days
            },
            'unique_sensors': self.df['sensor_id'].nunique(),
            'unique_locations': self.df['location_id'].nunique(),
            'unique_parameters': self.df['parameter'].nunique() if 'parameter' in self.df else 1,
            'cities': list(self.df['city'].unique()) if 'city' in self.df else []
        }
    
    def get_sensor_details(self) -> List[Dict]:
        sensors = []
        grouped = self.df.groupby(['sensor_id', 'location_name', 'latitude', 'longitude'])
        
        for (sensor_id, location, lat, lon), group in grouped:
            sensor_info = {
                'sensor_id': sensor_id,
                'location': location,
                'coordinates': (lat, lon),
                'measurements': len(group),
                'parameters': list(group['parameter'].unique()) if 'parameter' in group else ['unknown'],
                'date_range': {
                    'start': str(group['datetime'].min()),
                    'end': str(group['datetime'].max())
                }
            }
            sensors.append(sensor_info)
        
        return sensors
    
    def get_parameter_stats(self) -> Dict:
        if 'parameter' not in self.df:
            return {'pm25': self.df['value'].describe().to_dict()}
        
        stats = {}
        for param in self.df['parameter'].unique():
            param_data = self.df[self.df['parameter'] == param]['value']
            stats[param] = {
                'count': len(param_data),
                'mean': round(param_data.mean(), 2),
                'std': round(param_data.std(), 2),
                'min': round(param_data.min(), 2),
                'max': round(param_data.max(), 2),
                'percentiles': {
                    '25%': round(param_data.quantile(0.25), 2),
                    '50%': round(param_data.quantile(0.50), 2),
                    '75%': round(param_data.quantile(0.75), 2),
                    '95%': round(param_data.quantile(0.95), 2)
                }
            }
        return stats
    
    def get_coverage_analysis(self) -> Dict:
        coverage = {}
        
        for sensor_id in self.df['sensor_id'].unique():
            sensor_data = self.df[self.df['sensor_id'] == sensor_id]
            sensor_data = sensor_data.set_index('datetime').sort_index()
            
            expected_hours = int((sensor_data.index.max() - sensor_data.index.min()).total_seconds() / 3600) + 1
            actual_hours = len(sensor_data)
            
            coverage[sensor_id] = {
                'expected_hours': expected_hours,
                'actual_hours': actual_hours,
                'coverage_percent': round((actual_hours / expected_hours) * 100, 1),
                'gaps': self._find_gaps(sensor_data.index)
            }
        
        return coverage
    
    def _find_gaps(self, datetime_index) -> List[Dict]:
        gaps = []
        if len(datetime_index) < 2:
            return gaps
        
        time_diffs = pd.Series(datetime_index).diff()
        gap_mask = time_diffs > pd.Timedelta(hours=2)
        
        if gap_mask.any():
            gap_indices = gap_mask[gap_mask].index
            for idx in gap_indices:
                gaps.append({
                    'start': str(datetime_index[idx-1]),
                    'end': str(datetime_index[idx]),
                    'duration_hours': int(time_diffs.iloc[idx].total_seconds() / 3600)
                })
        
        return gaps[:5]
    
    def get_spatial_distribution(self) -> Dict:
        coords = self.df[['latitude', 'longitude']].drop_duplicates()
        
        if len(coords) < 2:
            return {'single_location': True}
        
        lat_range = coords['latitude'].max() - coords['latitude'].min()
        lon_range = coords['longitude'].max() - coords['longitude'].min()
        
        return {
            'sensor_count': len(coords),
            'bounding_box': {
                'north': float(coords['latitude'].max()),
                'south': float(coords['latitude'].min()),
                'east': float(coords['longitude'].max()),
                'west': float(coords['longitude'].min())
            },
            'area_span_km': {
                'north_south': round(lat_range * 111, 1),
                'east_west': round(lon_range * 111 * np.cos(np.radians(coords['latitude'].mean())), 1)
            }
        }
    
    def generate_report(self) -> str:
        basic = self.get_basic_stats()
        sensors = self.get_sensor_details()
        params = self.get_parameter_stats()
        spatial = self.get_spatial_distribution()
        
        report = f"""
=== Air Quality Data Analysis Report ===
File: {basic['file_name']}

OVERVIEW:
- Total measurements: {basic['total_measurements']:,}
- Date range: {basic['date_range']['days']} days ({basic['date_range']['start'][:10]} to {basic['date_range']['end'][:10]})
- Unique sensors: {basic['unique_sensors']}
- Unique locations: {basic['unique_locations']}
- Parameters measured: {basic['unique_parameters']} ({', '.join(params.keys())})

SPATIAL DISTRIBUTION:
- Sensors span: {spatial.get('area_span_km', {}).get('north_south', 0):.1f} km N-S, {spatial.get('area_span_km', {}).get('east_west', 0):.1f} km E-W
- Bounding box: ({spatial.get('bounding_box', {}).get('south', 0):.4f}, {spatial.get('bounding_box', {}).get('west', 0):.4f}) to ({spatial.get('bounding_box', {}).get('north', 0):.4f}, {spatial.get('bounding_box', {}).get('east', 0):.4f})

SENSOR DETAILS:
"""
        for i, sensor in enumerate(sensors[:10], 1):
            report += f"\n{i}. {sensor['location']} (ID: {sensor['sensor_id']})"
            report += f"\n   Location: {sensor['coordinates'][0]:.4f}, {sensor['coordinates'][1]:.4f}"
            report += f"\n   Measurements: {sensor['measurements']:,}"
            report += f"\n   Parameters: {', '.join(sensor['parameters'])}"
        
        if len(sensors) > 10:
            report += f"\n   ... and {len(sensors) - 10} more sensors"
        
        report += "\n\nPARAMETER STATISTICS:"
        for param, stats in params.items():
            report += f"\n\n{param.upper()}:"
            report += f"\n- Mean: {stats['mean']} ± {stats['std']} µg/m³"
            report += f"\n- Range: {stats['min']} - {stats['max']} µg/m³"
            report += f"\n- Percentiles: 25%={stats['percentiles']['25%']}, 50%={stats['percentiles']['50%']}, 75%={stats['percentiles']['75%']}, 95%={stats['percentiles']['95%']}"
        
        return report


def analyze_dataset(csv_path: str):
    analyzer = DataAnalyzer(csv_path)
    print(analyzer.generate_report())
    
    coverage = analyzer.get_coverage_analysis()
    avg_coverage = np.mean([c['coverage_percent'] for c in coverage.values()])
    print(f"\nDATA COMPLETENESS: {avg_coverage:.1f}% average coverage")
    
    low_coverage = [(sid, c['coverage_percent']) for sid, c in coverage.items() if c['coverage_percent'] < 50]
    if low_coverage:
        print("\nSensors with <50% coverage:")
        for sid, pct in low_coverage[:5]:
            print(f"  - Sensor {sid}: {pct}%")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        analyze_dataset(sys.argv[1])
    else:
        analyze_dataset('data/openaq/processed/vietnam_sensors_june_2024.csv')