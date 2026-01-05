# Data Retention Policy

## Overview

This document describes how data flows through the system and is retained for long-term research while optimizing database server space.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Soulseek Network                                             │
│   ↓ (search requests from users)                            │
├─────────────────────────────────────────────────────────────┤
│ Research Clients (3 geographic regions)                      │
│   ↓ (encrypted username, query, timestamp)                  │
├─────────────────────────────────────────────────────────────┤
│ PostgreSQL Database (Live Data)                              │
│   • Retention: ~30-60 days (current + previous month)       │
│   • Write-optimized: No indexes for maximum throughput       │
│   • Size: ~5-7 GB for one month of data                     │
└─────────────────────────────────────────────────────────────┘
                         ↓ (weekly archival)
┌─────────────────────────────────────────────────────────────┐
│ Parquet Archives (/opt/archives/ on database server)        │
│   • Format: Parquet with Snappy compression                 │
│   • Retention: PERMANENT (unlimited history)                │
│   • Size: ~50-100 MB per month (90%+ compression)           │
│   • Location: Database server ONLY (secure infrastructure)  │
└─────────────────────────────────────────────────────────────┘
                         ↓ (daily, during dashboard generation)
┌─────────────────────────────────────────────────────────────┐
│ GitHub Actions (Processing Only - NO DATA STORAGE)           │
│   • Temporarily downloads archives via SSH/SCP               │
│   • Generates statistics with DuckDB (in memory)             │
│   • Publishes aggregated dashboard to GitHub Pages           │
│   • Archives deleted after workflow completes                │
└─────────────────────────────────────────────────────────────┘
```

## Retention Details

### Live Database (PostgreSQL)

**What:** Current and recent search data
**Retention:** ~30-60 days (current month + maybe previous month)
**Purge Schedule:** Weekly (Sundays, 2 AM UTC)
**Criteria:** Data older than 7 days AND in a complete month

**Example on Jan 15, 2026:**
- December 2025: Archived → Deleted from database ✓
- January 2026: Still collecting → Remains in database ✓

### Archives (Parquet Files)

**What:** Historical search data (complete months)
**Retention:** PERMANENT
**Format:** Parquet with Snappy compression (90%+ space savings)
**Location:** `/opt/archives/` on database server (46.224.187.49)

**Backup Strategy:**
1. **Primary:** Database server storage (`/opt/archives/` on 46.224.187.49)
2. **Secondary (Optional):** Private storage bucket (S3/R2/Backblaze B2) if configured
3. **Security:** Archives contain encrypted usernames but NEVER leave secure infrastructure

### Archives Table (Metadata)

Tracks which months have been archived:

```sql
CREATE TABLE archives (
    id SERIAL PRIMARY KEY,
    month VARCHAR(7),              -- 'YYYY-MM'
    file_path TEXT,                -- '/opt/archives/searches_YYYY-MM.parquet'
    record_count INTEGER,          -- Number of searches in archive
    file_size BIGINT,              -- Parquet file size in bytes
    archived_at TIMESTAMP,         -- When archive was created
    deleted BOOLEAN DEFAULT FALSE  -- Whether data deleted from searches table
);
```

**Query for current archives:**
```sql
SELECT month, record_count, pg_size_pretty(file_size) as size, deleted
FROM archives
ORDER BY month DESC;
```

## Archival Process

### Automatic (Weekly Cron)

**Schedule:** Sundays at 2 AM UTC
**Script:** `/usr/local/bin/weekly-archive.sh`
**Configuration:**
```bash
DELETE_AFTER_ARCHIVE=true  # Delete from database after archiving
ARCHIVE_PATH=/opt/archives
```

**Steps:**
1. Find complete months where `MAX(timestamp) < NOW() - INTERVAL '7 days'`
2. Export to Parquet: `searches_YYYY-MM.parquet`
3. Record metadata in `archives` table
4. Delete data from `searches` table
5. Mark archive as `deleted = TRUE`

### Manual Archive

For testing or emergency archival:

```bash
ssh root@46.224.187.49
cd /opt/soulseek-research

DATABASE_URL="postgresql://soulseek:PASSWORD@localhost:5432/soulseek" \
ARCHIVE_PATH=/opt/archives \
DELETE_AFTER_ARCHIVE=true \
python3 scripts/archive.py
```

## Dashboard Generation

**Schedule:** Daily at 3 AM UTC
**Process:**

1. **Download archives** from database server via SSH/SCP (temporary, in-memory)
2. **Load into DuckDB** (zero-copy, metadata only)
3. **Query live data** from PostgreSQL (last 30 days)
4. **Combine** archived + live data via UNION ALL
5. **Generate statistics** using DuckDB SQL (10-100× faster than pandas)
6. **Deploy** aggregated dashboard to GitHub Pages
7. **Clean up** - archives deleted from runner after workflow

**Performance:** ~6 minutes total, regardless of archive size

## Data Recovery

### Scenario: Database Server Failure

**Recovery Steps:**

1. **Restore archives from database server backup:**
   - SSH into database server: `ssh root@46.224.187.49`
   - Archives located at: `/opt/archives/*.parquet`
   - Copy to safe location before server decommission

2. **Query archives directly with DuckDB:**
   ```python
   import duckdb
   conn = duckdb.connect()
   df = conn.execute("""
       SELECT * FROM read_parquet('searches_*.parquet')
       WHERE timestamp >= '2025-01-01'
   """).df()
   ```

3. **Restore to new PostgreSQL instance:**
   ```python
   # Load Parquet
   df = pd.read_parquet('searches_2025-12.parquet')

   # Write to PostgreSQL
   from sqlalchemy import create_engine
   engine = create_engine('postgresql://...')
   df.to_sql('searches', engine, if_exists='append', index=False)
   ```

### Scenario: Archive File Corruption

- **Primary:** Re-export from database (if data not yet deleted)
- **Last Resort:** Dashboard continues working with remaining archives + live data
- **Prevention:** Consider adding automated backup to private storage bucket (S3/R2/Backblaze B2)

## Storage Estimates

### Current (Jan 2026)

- **Live Database:** ~5-6 GB (32M rows, current month only after cleanup)
- **Archives:** 54 MB (1.7M rows, December 2025)
- **Total:** ~5.1 GB on database server

### Projected (1 Year)

- **Live Database:** ~5-7 GB (consistent, only current month)
- **Archives:** ~600 MB - 1.2 GB (12 months × 50-100 MB)
- **Total:** ~6-8 GB on database server

### Long-Term (5 Years)

- **Live Database:** ~5-7 GB (consistent, only current month)
- **Archives:** ~3-6 GB (60 months × 50-100 MB)
- **Total:** ~8-13 GB on database server (highly sustainable)

## Priorities Addressed

✅ **Data Retention:** Permanent archive storage on secure database server
✅ **Space Savings:** ~95% reduction in database size (keeps only current month)
✅ **Coherence:** Clear data flow, documented processes, automated retention

## Monitoring

### Check Archive Status

```sql
-- Show all archives and their status
SELECT
    month,
    record_count,
    pg_size_pretty(file_size) as size,
    archived_at,
    deleted as deleted_from_db
FROM archives
ORDER BY month DESC;
```

### Check Database Size

```sql
-- Current database size
SELECT pg_size_pretty(pg_database_size('soulseek'));

-- Size by month (for non-archived data)
SELECT
    TO_CHAR(timestamp, 'YYYY-MM') as month,
    COUNT(*) as searches,
    pg_size_pretty(pg_total_relation_size('searches')) as table_size
FROM searches
GROUP BY TO_CHAR(timestamp, 'YYYY-MM')
ORDER BY month DESC;
```

### Check Archive Files

```bash
ssh root@46.224.187.49 'ls -lh /opt/archives/'
```

## Configuration

### Archive Script Location

- **Script:** `/opt/soulseek-research/scripts/archive.py`
- **Cron:** `/usr/local/bin/weekly-archive.sh`
- **Schedule:** `0 2 * * 0` (Sundays at 2 AM UTC)

### Environment Variables

```bash
DATABASE_URL="postgresql://soulseek:PASSWORD@localhost:5432/soulseek"
ARCHIVE_PATH="/opt/archives"
DELETE_AFTER_ARCHIVE="true"  # Set to false to keep data in DB
```

### GitHub Secrets

Required for dashboard generation:

- `DATABASE_URL`: PostgreSQL connection string
- `DB_SERVER_IP`: Database server IP (46.224.187.49)
- `DB_SERVER_SSH_KEY`: SSH private key for database server access
