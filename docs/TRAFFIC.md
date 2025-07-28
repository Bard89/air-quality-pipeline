# Traffic

JARTIC archives. Japan only.

## Coverage
- ~2,600 locations across Japan
- 5-minute intervals
- Historical from 2019
- Monthly archives (~4GB compressed)

## Commands

```bash
# Download
python scripts/download_jartic_archives.py --start 2024-01 --end 2024-12
```

## Data Available
- Vehicle counts (vehicles/5min)
- Average speeds (km/h)
- Occupancy rates (%)
- Per prefecture CSVs in Shift-JIS

## Notes
- No API key needed
- Archives contain nested zips
- Processing scripts available for extraction