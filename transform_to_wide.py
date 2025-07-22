#!/usr/bin/env python3
import pandas as pd
import sys
from pathlib import Path


def transform_to_wide(input_csv):
    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv, parse_dates=['datetime'])
    
    print(f"Data shape: {len(df)} rows")
    print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"Locations: {df['location_id'].nunique()}")
    print(f"Parameters: {sorted(df['parameter'].unique())}")
    
    # Get units for each parameter
    param_units = df.groupby('parameter')['unit'].first().to_dict()
    print(f"Units: {param_units}")
    
    # Round to hourly
    df['hour'] = df['datetime'].dt.floor('h')
    
    # Create pivot
    print("\nCreating wide format...")
    wide = df.pivot_table(
        index=['hour', 'location_id', 'location_name', 'latitude', 'longitude'],
        columns='parameter',
        values='value',
        aggfunc='mean'
    ).reset_index()
    
    # Rename columns to include units
    wide.rename(columns={'hour': 'datetime'}, inplace=True)
    
    # Add units to parameter columns
    for param in wide.columns:
        if param in param_units:
            unit = param_units[param]
            wide.rename(columns={param: f"{param}_{unit}"}, inplace=True)
    
    # Sort
    wide.sort_values(['location_id', 'datetime'], inplace=True)
    
    # Output file
    output_csv = input_csv.replace('.csv', '_wide.csv')
    wide.to_csv(output_csv, index=False)
    
    print(f"\nOutput shape: {len(wide)} rows x {len(wide.columns)} columns")
    print(f"Saved to: {output_csv}")
    
    # Show sample
    print("\nFirst few rows:")
    print(wide.head())
    
    return output_csv


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python transform_to_wide.py <input_csv>")
        sys.exit(1)
    
    transform_to_wide(sys.argv[1])