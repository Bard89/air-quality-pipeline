# Weather

## Sources

| Source | Coverage | History | Resolution | Notes |
|--------|----------|---------|------------|-------|
| **Open-Meteo** | Global | 1940-present | 0.1° grid | Free, no limits |
| **NASA POWER** | Global | 1984-present | 0.5° grid | Free, slow |
| **JMA** | Japan | Last 3 days only | 1,300 stations | 10-min intervals |
| **ERA5** | Global | 1940-present | 0.25° grid | Needs API key |

## Commands

```bash
# Historical
python scripts/download_weather_incremental.py --source openmeteo --country JP --start 2024-01-01 --end 2024-01-31

# Recent only (JMA)
python scripts/download_weather_incremental.py --source jma --country JP --start $(date -I -d "2 days ago") --end $(date -I)
```

## Data Available

All sources: temperature, humidity, pressure, windspeed, winddirection, precipitation, solar_radiation, visibility, cloud_cover, dew_point

ERA5 adds: Multiple atmospheric pressure levels