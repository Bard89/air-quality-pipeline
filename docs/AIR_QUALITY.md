# Air Quality

OpenAQ API v3. Downloads ALL historical data (ignores date filters).

## Why This Data Source?

Ground truth measurements from government monitoring stations. OpenAQ provides:
- **Accurate baseline**: Regulatory-grade PM2.5, NO2, O3 measurements
- **Historical context**: Multi-year data reveals seasonal patterns
- **Model validation**: Essential for training and validating prediction models
- **Limitation**: Sparse coverage (cities only), making other data sources critical for full coverage

## Coverage
- 100+ countries, 15,000+ locations
- Historical from ~2016 (varies by station)
- Real-time to hourly updates
- 60 req/min per key

## Commands

```bash
python scripts/download_air_quality.py --country JP --parameters pm25 --max-locations 10 --country-wide
python scripts/download_air_quality.py --country IN --country-wide --parallel  # Multiple API keys
```

## Parameters

- **PM**: pm25, pm10, pm1
- **Gases**: no2, o3, co, so2, no, nox
- **Weather**: temperature, humidity, pressure, windspeed, winddirection
- **Other**: bc (black carbon)

## API Keys

```env
OPENAQ_API_KEY=xxx
# For parallel:
OPENAQ_API_KEY_01=xxx
OPENAQ_API_KEY_02=xxx
```

## Notes
- Always use `--country-wide` (API bug)
- Max 16 pages (16k measurements) per sensor
- Auto-resumes from checkpoint