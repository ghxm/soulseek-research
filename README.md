# Soulseek Research

Monitors Soulseek P2P network search requests for research purposes. Collects search patterns from distributed geographic locations and stores anonymized data for analysis.

## Architecture

The system supports multiple geographically distributed clients collecting data into a central PostgreSQL database. Currently, one client is running in production (Germany/Nuremberg, Hetzner Cloud).

**Data flow:**
1. Clients connect to the Soulseek network and monitor incoming search requests from other users
2. Search metadata (client_id, timestamp, hashed username, query) is logged to the central database
3. Materialized views are refreshed daily for fast dashboard queries
4. Period-specific statistics (weekly/monthly top queries, query length distributions) are precomputed daily
5. A GitHub Actions workflow generates a static dashboard and deploys it to GitHub Pages

**Automated schedule (UTC):**
- 2:00 AM Sunday: Monthly data archival
- 4:00 AM Daily: Materialized view refresh
- 4:30 AM Daily: Period statistics precomputation
- 5:00 AM Daily: Dashboard generation and deployment

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

## Statistics Dashboard

Auto-generated static site with interactive charts and searchable data tables. Includes:
- All-time, monthly, and weekly period pages with search volume, top queries, and query length distributions
- Individual query detail pages (~23k) with daily search activity charts (searches and unique users)
- Lazy-loaded query data tables with search and virtual scrolling

**Setup:**
1. Add `DATABASE_URL` secret in repo Settings > Secrets
2. Enable Pages in Settings > Pages > Source: GitHub Actions
3. Run workflow: Actions > Update Statistics Dashboard > Run workflow

## Deployment

**Prerequisites:**
- Hetzner Cloud account with SSH key named "soulseek-research"
- Soulseek account credentials (one per client)

```bash
# Configure credentials
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your API tokens and Soulseek credentials

# Deploy infrastructure
make deploy

# Monitor data collection
make monitor

# Destroy infrastructure
make destroy
```

## Local Development

```bash
pip install -e .
soulseek-research start \
  --username your_username \
  --password your_password \
  --database-url postgresql+asyncpg://user:pass@host:5432/soulseek \
  --client-id local-test \
  --encryption-key your_encryption_key
```

## Requirements

- Python 3.11+
- Docker
- Soulseek account credentials
- PostgreSQL database
