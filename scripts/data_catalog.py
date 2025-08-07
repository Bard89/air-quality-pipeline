#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from datetime import datetime
import pandas as pd
from typing import Dict, List, Optional
import argparse
from tabulate import tabulate
from src.infrastructure.data_reference import ExternalDataManager


def get_file_info(file_path: Path) -> Dict:
    file_stats = file_path.stat()
    size_mb = file_stats.st_size / (1024 * 1024)
    
    info = {
        'file': file_path.name,
        'size_mb': round(size_mb, 2),
        'modified': datetime.fromtimestamp(file_stats.st_mtime).strftime('%Y-%m-%d %H:%M')
    }
    
    parts = file_path.stem.split('_')
    if len(parts) >= 3:
        info['country'] = parts[0].upper()
    
    if 'weather' in file_path.name or 'airquality' in file_path.name:
        try:
            if '_to_' in file_path.name:
                date_parts = file_path.stem.split('_')
                for i, part in enumerate(date_parts):
                    if part == 'to' and i > 0 and i < len(date_parts) - 1:
                        info['start_date'] = datetime.strptime(date_parts[i-1], '%Y%m%d').strftime('%Y-%m-%d')
                        info['end_date'] = datetime.strptime(date_parts[i+1], '%Y%m%d').strftime('%Y-%m-%d')
                        break
        except (ValueError, IndexError):
            pass
    
    if file_path.suffix == '.csv':
        try:
            df = pd.read_csv(file_path, nrows=5)
            info['rows'] = len(pd.read_csv(file_path))
            info['columns'] = len(df.columns)
            
            if 'datetime' in df.columns or 'timestamp' in df.columns:
                date_col = 'datetime' if 'datetime' in df.columns else 'timestamp'
                df_full = pd.read_csv(file_path, usecols=[date_col], parse_dates=[date_col])
                info['data_start'] = df_full[date_col].min()
                info['data_end'] = df_full[date_col].max()
        except Exception:
            pass
    
    return info


def analyze_data_source(manager: ExternalDataManager, source: str) -> List[Dict]:
    results = []
    
    try:
        files = manager.list_files(source)
        for file in files:
            info = get_file_info(file)
            info['source'] = source
            results.append(info)
    except ValueError:
        pass
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Catalog and analyze available environmental data'
    )
    parser.add_argument('--source', '-s', type=str,
                       choices=['openaq', 'openmeteo', 'nasapower', 'firms', 
                               'era5', 'jartic', 'terrain', 'jma', 'all'],
                       default='all',
                       help='Data source to analyze')
    parser.add_argument('--country', '-c', type=str,
                       help='Filter by country code')
    parser.add_argument('--summary', action='store_true',
                       help='Show summary statistics only')
    parser.add_argument('--export', type=str,
                       help='Export catalog to CSV file')
    
    args = parser.parse_args()
    
    manager = ExternalDataManager()
    
    sources = ['openaq', 'openmeteo', 'nasapower', 'firms', 'era5', 
              'jartic', 'terrain', 'jma'] if args.source == 'all' else [args.source]
    
    all_data = []
    for source in sources:
        data = analyze_data_source(manager, source)
        if args.country:
            data = [d for d in data if d.get('country', '').lower() == args.country.lower()]
        all_data.extend(data)
    
    if not all_data:
        print("No data files found")
        return
    
    if args.summary:
        summary = {}
        for item in all_data:
            source = item['source']
            if source not in summary:
                summary[source] = {
                    'files': 0,
                    'total_size_mb': 0,
                    'countries': set()
                }
            summary[source]['files'] += 1
            summary[source]['total_size_mb'] += item.get('size_mb', 0)
            if 'country' in item:
                summary[source]['countries'].add(item['country'])
        
        summary_table = []
        for source, stats in summary.items():
            summary_table.append([
                source,
                stats['files'],
                round(stats['total_size_mb'], 2),
                ', '.join(sorted(stats['countries']))
            ])
        
        print("\n=== Data Catalog Summary ===")
        print(tabulate(summary_table, 
                      headers=['Source', 'Files', 'Total Size (MB)', 'Countries'],
                      tablefmt='grid'))
        
        total_files = sum(s['files'] for s in summary.values())
        total_size = sum(s['total_size_mb'] for s in summary.values())
        print(f"\nTotal: {total_files} files, {round(total_size, 2)} MB")
        
    else:
        df = pd.DataFrame(all_data)
        
        display_cols = ['source', 'country', 'file', 'size_mb', 'rows']
        display_cols = [c for c in display_cols if c in df.columns]
        
        print("\n=== Available Data Files ===")
        print(tabulate(df[display_cols].head(50), 
                      headers='keys', 
                      tablefmt='grid',
                      showindex=False))
        
        if len(df) > 50:
            print(f"\n... and {len(df) - 50} more files")
    
    if args.export:
        df = pd.DataFrame(all_data)
        df.to_csv(args.export, index=False)
        print(f"\nCatalog exported to {args.export}")


if __name__ == "__main__":
    main()