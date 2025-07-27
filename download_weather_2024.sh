#!/bin/bash

# Download weather data for Japan 2024
# This script downloads data month by month to avoid timeouts

echo "Starting weather data download for Japan 2024"
echo "============================================="

# NASA POWER - Start with a few locations for the full year
echo "Downloading NASA POWER data..."

# First, download from major cities only
python download_weather_data.py \
    --source nasapower \
    --country JP \
    --max-locations 5 \
    --start 2024-01-01 \
    --end 2024-12-31 \
    --no-analyze

echo "Download complete!"
echo ""
echo "To download more locations, run:"
echo "python download_weather_data.py --source nasapower --country JP --max-locations 10 --start 2024-01-01 --end 2024-12-31"
echo ""
echo "Or download month by month for better reliability:"
echo "python download_weather_data.py --source nasapower --country JP --start 2024-01-01 --end 2024-01-31"
echo "python download_weather_data.py --source nasapower --country JP --start 2024-02-01 --end 2024-02-29"
echo "... etc"