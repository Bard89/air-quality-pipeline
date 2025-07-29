# Traffic

JARTIC archives. Japan only.

## Why This Data Source?

Traffic is a major urban pollution source, especially during rush hours:
- **Direct emissions**: Vehicles emit NO2, PM2.5, CO directly
- **Morning peak problem**: Rush hour emissions + low morning PBL = 2-3x higher concentrations
- **Congestion correlation**: Traffic speed inversely correlates with emissions
- **Weekend effect**: 20-40% lower NO2 on weekends proves traffic impact
- **Hyperlocal**: Traffic explains street-level pollution variations

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