# Soulseek Research

## Project Description

This project monitors the Soulseek peer-to-peer file-sharing network by passively collecting search queries issued by other users for research purposes. The system consists of one or more geographically distributed clients that connect to the Soulseek network, observe incoming search requests, and log anonymized metadata (client ID, timestamp, hashed username, query text) to a central PostgreSQL database.

Precomputed statistics and materialized views support a static dashboard deployed via GitHub Pages. Data archival, view refresh, and dashboard generation run on automated daily/weekly schedules through GitHub Actions.

The current production deployment runs a single client on Hetzner Cloud (Nuremberg, Germany).

## Database Schema

```sql
-- Core search data (no indexes, optimized for write throughput)
searches: id, client_id, timestamp, username, query

-- Archive tracking
archives: id, month, file_path, record_count, file_size, archived_at, deleted

-- Cumulative statistics (archived + live data combined)
stats_cumulative: id, total_searches, total_users, total_queries,
                  total_search_pairs, first_search, last_search,
                  client_totals, last_archive_month, updated_at

-- Precomputed period statistics
period_top_queries: id, period_type, period_id, query_normalized,
                    unique_users, total_searches, rank
period_query_length_dist: id, period_type, period_id, query_length,
                          unique_query_count
query_daily_stats: query_normalized, date, search_count, unique_users

-- Materialized views (live data only)
mv_daily_stats          -- Daily aggregates by client
mv_top_queries          -- Global top queries (5+ unique users)
mv_query_length_dist    -- Query word count distribution
mv_summary_stats        -- Overall summary statistics
```

## Deployment and Production

### Requirements

- Python 3.11+
- Docker
- PostgreSQL database
- Soulseek account credentials (one per client)
- Hetzner Cloud account with an SSH key named "soulseek-research"

### Automated Schedule (UTC)

| Time | Frequency | Task |
|------|-----------|------|
| 2:00 AM | Sunday | Monthly data archival |
| 4:00 AM | Daily | Materialized view refresh |
| 4:30 AM | Daily | Period statistics precomputation |
| 5:00 AM | Daily | Dashboard generation and deployment |

### Infrastructure Deployment

```bash
# Copy and fill in API tokens and Soulseek credentials
cp terraform.tfvars.example terraform.tfvars

# Deploy infrastructure
make deploy

# Monitor data collection
make monitor

# Tear down infrastructure
make destroy
```

### Dashboard Setup

1. Add a `DATABASE_URL` secret in the repository under Settings > Secrets.
2. Enable GitHub Pages in Settings > Pages > Source: GitHub Actions.
3. Trigger the workflow manually: Actions > Update Statistics Dashboard > Run workflow.

### Local Development

```bash
pip install -e .
soulseek-research start \
  --username your_username \
  --password your_password \
  --database-url postgresql+asyncpg://user:pass@host:5432/soulseek \
  --client-id local-test \
  --encryption-key your_encryption_key
```
