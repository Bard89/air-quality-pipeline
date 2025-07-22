# Air Quality Data Collection

This project collects air quality data from various sources to build ML models for predicting air quality.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Get OpenAQ API Key:
   - Sign up at https://explore.openaq.org/register
   - Find your API key in account settings
   - Create `.env` file from template:
   ```bash
   cp .env.example .env
   ```
   - Add your API key to `.env`

## Usage

Test location fetching:
```bash
python src/fetch_locations.py
```

## Project Structure

```
├── config/          # Configuration files
├── data/           # Downloaded data (gitignored)
│   └── openaq/
│       ├── raw/    # Raw API responses
│       └── processed/
├── src/            # Source code
└── requirements.txt
```