# Weather

## Why This Data Source?

Weather is the primary driver of pollution dispersion. Critical factors:
- **Wind**: Horizontal transport and dilution (>5 m/s clears pollution)
- **Rain**: Wet deposition removes PM2.5 (10mm rain reduces PM by 30-50%)
- **Temperature**: Drives chemical reactions and inversions
- **Pressure**: High pressure = stagnant air = pollution accumulation
- **Combined impact**: Weather explains 40-60% of day-to-day pollution variability

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

# Recent only (JMA) - last 3 days  
# Linux/GNU:
python scripts/download_weather_incremental.py --source jma --country JP --start $(date -I -d "2 days ago") --end $(date -I)
```

## Data Available

All sources: temperature, humidity, pressure, windspeed, winddirection, precipitation, solar_radiation, visibility, cloud_cover, dew_point

ERA5 adds: Multiple atmospheric pressure levels