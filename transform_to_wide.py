#!/usr/bin/env python3
from pathlib import Path
import sys

import pandas as pd
def transform_to_wide(input_csv: str) -> str:
    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv, parse_dates=['datetime'])

    print(f"Data shape: {len(df)} rows")
    print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"Locations: {df['location_id'].nunique()}")
    print(f"Parameters: {sorted(df['parameter'].unique())}")

    param_units = df.groupby('parameter')['unit'].first().to_dict()
    print(f"Units: {param_units}")

    df['hour'] = df['datetime'].dt.floor('h')

    print("\nCreating wide format...")
    wide = df.pivot_table(
        index=['hour', 'location_id', 'location_name', 'latitude', 'longitude'],
        columns='parameter',
        values='value',
        aggfunc='mean'
    ).reset_index()

    wide.rename(columns={'hour': 'datetime'}, inplace=True)

    for param in wide.columns:
        if param in param_units:
            unit = param_units[param]
            wide.rename(columns={param: f"{param}_{unit}"}, inplace=True)

    wide.sort_values(['location_id', 'datetime'], inplace=True)

    output_path = Path(input_csv)
    output_csv = str(output_path.parent / f"{output_path.stem}_wide.csv")
    wide.to_csv(output_csv, index=False)

    print(f"\nOutput shape: {len(wide)} rows x {len(wide.columns)} columns")
    print(f"Saved to: {output_csv}")

    print("\nFirst few rows:")
    print(wide.head())

    return output_csv


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python transform_to_wide.py <input_csv>")
        sys.exit(1)

    transform_to_wide(sys.argv[1])
