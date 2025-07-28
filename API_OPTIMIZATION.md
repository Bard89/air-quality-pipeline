# API Request Optimization Strategies

This document outlines the strategies implemented to optimize OpenAQ API usage.

## 1. Multi-Key Support

The system supports multiple API keys for increased throughput:
- Each API key provides 60 requests/minute
- Keys are rotated automatically to distribute load
- Sequential mode: Uses keys in rotation
- Parallel mode: Uses all keys concurrently

**Example**: 
- 1 key: 60 requests/minute
- 10 keys: 600 requests/minute (10x faster)

## 2. Parallel Download Mode

When `--parallel` flag is used with multiple API keys:
- Concurrent requests across all available keys
- Smart batching based on sensor count:
  - < 10 sensors per location: Batch by location
  - ≥ 10 sensors per location: Batch by sensor pages
- Automatic retry and error handling

## 3. Incremental Download & Checkpoints

All downloads are incremental and resumable:
- CSV files are written incrementally after each sensor
- Checkpoint saved after each location
- Automatic resume from last position if interrupted
- No data loss on failures

## 4. API Limit Handling

Built-in handling for OpenAQ API limitations:
- Max 16 pages (16,000 measurements) per sensor
- Page 17+ requests timeout automatically
- Parallel mode skips problematic pages
- Rate limiting enforced per key

## 5. Usage Recommendations

### For Optimal Performance:

```bash
# Single key (standard speed)
python download_air_quality.py --country JP --country-wide

# Multiple keys (faster)
# Set OPENAQ_API_KEY_01, OPENAQ_API_KEY_02, etc.
python download_air_quality.py --country JP --country-wide --parallel

# Limit scope for testing
python download_air_quality.py --country IN --max-locations 10 --country-wide
```

### Performance Estimates:

| Configuration | Speed | Best For |
|--------------|-------|----------|
| 1 key, sequential | 60 req/min | Small datasets |
| 3 keys, parallel | 180 req/min | Medium datasets |
| 10 keys, parallel | 600 req/min | Large country-wide downloads |

## 6. Memory Optimization

Memory usage scales with concurrent operations:
- Sequential mode: ~100MB baseline
- Parallel mode with 10 keys: ~300MB
- Parallel mode with 100 keys: ~1-2GB

Data is written incrementally to prevent memory accumulation.

## 7. Best Practices

1. **Use --country-wide**: OpenAQ v3 ignores date filters, so always download all available data
2. **Enable parallel mode**: When you have multiple API keys for faster downloads
3. **Monitor checkpoints**: Check `data/openaq/checkpoints/` for progress
4. **Limit locations for testing**: Use `--max-locations` to test with smaller datasets first