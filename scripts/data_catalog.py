#!/usr/bin/env python3
from pathlib import Path

from datetime import datetime
import pandas as pd
from typing import Dict, List
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
        if '_to_' in file_path.name:
            date_parts = file_path.stem.split('_')
            for i, part in enumerate(date_parts):
                if part == 'to' and i > 0 and i < len(date_parts) - 1:
                    try:
                        prev_part = date_parts[i-1]
                        next_part = date_parts[i+1]
                        if len(prev_part) == 8 and prev_part.isdigit():
                            info['start_date'] = datetime.strptime(prev_part, '%Y%m%d').strftime('%Y-%m-%d')
                        if len(next_part) == 8 and next_part.isdigit():
                            info['end_date'] = datetime.strptime(next_part, '%Y%m%d').strftime('%Y-%m-%d')
                    except (ValueError, IndexError) as e:
                        import logging
                        logging.debug(f"Failed to parse dates from {file_path.name}: {e}")
                    break
    
    if file_path.suffix == '.csv':
        try:
            row_count = 0
            columns = None
            date_col = None
            min_date = None
            max_date = None
            
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i == 0:
                        header = line.strip().split(',')
                        columns = len(header)
                        if 'datetime' in header:
                            date_col = header.index('datetime')
                        elif 'timestamp' in header:
                            date_col = header.index('timestamp')
                    else:
                        row_count += 1
                        if date_col is not None and i < 1000:
                            cols = line.strip().split(',')
                            if date_col < len(cols):
                                try:
                                    date_val = pd.to_datetime(cols[date_col])
                                    if min_date is None or date_val < min_date:
                                        min_date = date_val
                                    if max_date is None or date_val > max_date:
                                        max_date = date_val
                                except:
                                    pass
            
            info['rows'] = row_count
            info['columns'] = columns
            if min_date:
                info['data_start'] = min_date
            if max_date:
                info['data_end'] = max_date
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
        
        page_size = 20
        total_pages = (len(df) + page_size - 1) // page_size
        
        for page in range(total_pages):
            start_idx = page * page_size
            end_idx = min((page + 1) * page_size, len(df))
            
            print(f"\nPage {page + 1}/{total_pages} (showing {start_idx + 1}-{end_idx} of {len(df)})")
            print(tabulate(df[display_cols].iloc[start_idx:end_idx], 
                          headers='keys', 
                          tablefmt='grid',
                          showindex=False))
            
            if page < total_pages - 1:
                user_input = input("\nPress Enter for next page, 'q' to quit: ")
                if user_input.lower() == 'q':
                    break
    
    if args.export:
        df = pd.DataFrame(all_data)
        df.to_csv(args.export, index=False)
        print(f"\nCatalog exported to {args.export}")


if __name__ == "__main__":
    main()