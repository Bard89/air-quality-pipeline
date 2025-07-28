#!/bin/bash

# Fast parallel download of 2024 weather data for Japan

echo "Fast Weather Data Download for Japan 2024"
echo "========================================="
echo ""

# Download month by month to avoid timeouts
for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
    # Calculate end day
    case $month in
        01|03|05|07|08|10|12) days=31 ;;
        04|06|09|11) days=30 ;;
        02) days=29 ;; # 2024 is a leap year
    esac
    
    echo "Downloading 2024-$month..."
    python download_weather_parallel.py \
        --source nasapower \
        --country JP \
        --max-locations 10 \
        --max-concurrent 5 \
        --start "2024-$month-01" \
        --end "2024-$month-$days" \
        --no-analyze
    
    echo "Completed 2024-$month"
    echo ""
done

echo "All downloads complete!"
echo "Files saved to: data/nasapower/processed/"