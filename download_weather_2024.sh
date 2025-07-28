#\!/bin/bash

# Download weather data for each month of 2024
echo "Downloading weather data for 2024..."

# January
echo "Downloading January 2024..."
python download_weather_parallel.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31 --max-locations 30

# February
echo "Downloading February 2024..."
python download_weather_parallel.py --source openmeteo --country JP --start 2024-02-01 --end 2024-02-29 --max-locations 30

echo "Weather download complete\!"
EOF < /dev/null