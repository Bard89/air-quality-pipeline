from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
def convert_to_wide_format(input_csv: str, output_csv: str = None):

    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv)

    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime_hour'] = df['datetime'].dt.round('H')

    print(f"Original shape: {df.shape}")
    print(f"Unique locations: {df['location_id'].nunique()}")
    print(f"Parameters: {sorted(df['parameter'].unique())}")

    df['param_with_unit'] = df['parameter'] + '_' + df['unit']

    print("\nConverting to wide format...")
    wide_df = df.pivot_table(
        index=['datetime_hour', 'location_id', 'location_name', 'city', 'country', 'latitude', 'longitude'],
        columns='param_with_unit',
        values='value',
        aggfunc='mean'
    ).reset_index()

    wide_df.rename(columns={'datetime_hour': 'datetime'}, inplace=True)

    wide_df.sort_values(['location_id', 'datetime'], inplace=True)

    if not output_csv:
        input_path = Path(input_csv)
        output_csv = str(input_path.parent / f"{input_path.stem}_wide.csv")

    wide_df.to_csv(output_csv, index=False)

    print(f"\nWide format shape: {wide_df.shape}")
    print(f"Saved to: {output_csv}")

    print("\nColumns in wide format:")
    for col in wide_df.columns[:15]:
        print(f"  - {col}")
    if len(wide_df.columns) > 15:
        print(f"  ... and {len(wide_df.columns) - 15} more columns")

    return output_csv


def convert_incremental(input_csv: str, output_csv: str = None):

    if not output_csv:
        input_path = Path(input_csv)
        output_csv = str(input_path.parent / f"{input_path.stem}_wide.csv")

    print(f"Processing {input_csv} incrementally...")

    params_units = set()
    for chunk in pd.read_csv(input_csv, chunksize=100000):
        chunk['param_with_unit'] = chunk['parameter'] + '_' + chunk['unit']
        params_units.update(chunk['param_with_unit'].unique())

    params_units = sorted(list(params_units))
    print(f"Found {len(params_units)} parameter-unit combinations")

    location_ids = []
    for chunk in pd.read_csv(input_csv, chunksize=100000):
        location_ids.extend(chunk['location_id'].unique())
    location_ids = sorted(list(set(location_ids)))

    print(f"Found {len(location_ids)} unique locations")

    first_location = True
    for i, loc_id in enumerate(location_ids):
        print(f"\rProcessing location {i+1}/{len(location_ids)}", end='', flush=True)

        loc_data = []
        for chunk in pd.read_csv(input_csv, chunksize=100000):
            loc_chunk = chunk[chunk['location_id'] == loc_id]
            if not loc_chunk.empty:
                loc_data.append(loc_chunk)

        if not loc_data:
            continue

        loc_df = pd.concat(loc_data, ignore_index=True)

        loc_df['datetime'] = pd.to_datetime(loc_df['datetime'])
        loc_df['datetime_hour'] = loc_df['datetime'].dt.round('H')
        loc_df['param_with_unit'] = loc_df['parameter'] + '_' + loc_df['unit']

        wide_loc = loc_df.pivot_table(
            index=['datetime_hour', 'location_id', 'location_name', 'city', 'country', 'latitude', 'longitude'],
            columns='param_with_unit',
            values='value',
            aggfunc='mean'
        ).reset_index()

        wide_loc.rename(columns={'datetime_hour': 'datetime'}, inplace=True)

        if first_location:
            wide_loc.to_csv(output_csv, index=False)
            first_location = False
        else:
            wide_loc.to_csv(output_csv, mode='a', header=False, index=False)

    print(f"\n\nSaved to: {output_csv}")
    return output_csv


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python csv_to_wide_format.py <input_csv> [output_csv]")
        print("\nExample:")
        print("  python csv_to_wide_format.py data/openaq/processed/in_airquality_20240101_20241231.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    file_size_mb = Path(input_file).stat().st_size / (1024 * 1024)

    if file_size_mb > 500:
        convert_incremental(input_file, output_file)
    else:
        convert_to_wide_format(input_file, output_file)
