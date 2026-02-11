# Testing Period Statistics Implementation

This guide provides step-by-step instructions for testing the period statistics precomputation system.

## Prerequisites

- Access to database server via SSH
- Database has at least a few weeks of data in `searches` table
- Docker and docker-compose installed on database server

## Test 1: Table Creation

Verify the new tables exist and have correct schema.

```bash
# SSH to database server
ssh root@<DB_SERVER_IP>

# Check tables exist
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "\dt period_*"

# Expected output:
#                List of relations
#  Schema |           Name            | Type  |  Owner
# --------+---------------------------+-------+----------
#  public | period_query_length_dist  | table | soulseek
#  public | period_top_queries        | table | soulseek

# Check indexes
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "\di idx_period_*"

# Expected output:
#                              List of relations
#  Schema |            Name              | Type  |  Owner   |       Table
# --------+------------------------------+-------+----------+-------------------------
#  public | idx_period_ql_dist_lookup    | index | soulseek | period_query_length_dist
#  public | idx_period_top_queries_lookup| index | soulseek | period_top_queries

# Verify schema
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "\d period_top_queries"
```

**Expected Result**: Tables and indexes exist with correct columns.

## Test 2: Manual Script Execution

Run the refresh script manually and verify it completes successfully.

```bash
# Check script exists and is executable
ls -l /usr/local/bin/refresh-period-stats.sh
cat /usr/local/bin/refresh-period-stats.sh

# Run script manually (will take several minutes for first run)
time /usr/local/bin/refresh-period-stats.sh

# Check log output
tail -20 /var/log/soulseek-period-stats.log
```

**Expected Result**:
- Script runs without errors
- Log shows progress: "Processing week 2026-W04...", "Processing month 2026-01...", etc.
- Final message: "Period stats refresh completed successfully"
- Execution time: < 5 minutes for typical dataset

## Test 3: Verify Data Population

Check that period statistics were correctly computed and stored.

```bash
# Count periods processed
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "
    SELECT period_type, COUNT(DISTINCT period_id) as periods
    FROM period_top_queries
    GROUP BY period_type;
  "

# Expected output (example):
#  period_type | periods
# -------------+---------
#  month       |       3
#  week        |      12

# Check data range matches searches table
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "
    SELECT
      DATE_TRUNC('month', MIN(timestamp)) as first_month,
      DATE_TRUNC('month', MAX(timestamp)) as last_month,
      (SELECT MIN(period_id) FROM period_top_queries WHERE period_type = 'month') as first_precomputed,
      (SELECT MAX(period_id) FROM period_top_queries WHERE period_type = 'month') as last_precomputed
    FROM searches;
  "

# Expected: first_month/first_precomputed and last_month/last_precomputed should align

# View sample data for a specific period
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "
    SELECT period_id, query_normalized, unique_users, total_searches, rank
    FROM period_top_queries
    WHERE period_type = 'month'
    ORDER BY period_id DESC, rank
    LIMIT 10;
  "

# Expected: Top queries with users >= 5, ranked correctly
```

**Expected Result**:
- Period coverage matches data range in `searches` table
- Top queries have `unique_users >= 5`
- Ranks are sequential (1, 2, 3, ...)
- Query length distributions exist for same periods

## Test 4: Query Performance

Compare lookup speed before/after implementation.

```bash
# Test precomputed table lookup (should be FAST)
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "
    EXPLAIN ANALYZE
    SELECT query_normalized, unique_users, total_searches
    FROM period_top_queries
    WHERE period_type = 'month' AND period_id = '2026-01'
    ORDER BY rank
    LIMIT 100;
  "

# Expected: Execution time < 10 ms, uses index scan

# Compare with original on-demand query (should be SLOW)
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "
    EXPLAIN ANALYZE
    SELECT
      LOWER(TRIM(query)) as query_normalized,
      COUNT(DISTINCT username) as unique_users,
      COUNT(*) as total_searches
    FROM searches
    WHERE timestamp >= '2026-01-01' AND timestamp < '2026-02-01'
    GROUP BY LOWER(TRIM(query))
    HAVING COUNT(DISTINCT username) >= 5
    ORDER BY unique_users DESC
    LIMIT 100;
  "

# Expected: Execution time >> 1 second, seq scan on searches table
```

**Expected Result**: Precomputed lookup is 100x+ faster than on-demand aggregation.

## Test 5: Dashboard Generation

Test that `generate_stats.py` uses the new precomputed tables correctly.

```bash
# Rebuild Docker image with updated generate_stats.py
cd /opt/soulseek-research
git pull origin main
docker build -t soulseek-research:latest .

# Test period page generation locally (use test database or staging)
# This would normally be run by GitHub Actions, but can test manually:

# Set up environment
export DATABASE_URL="postgresql://soulseek:<PASSWORD>@localhost:5432/soulseek"

# Run dashboard generation (measures total time)
time docker run --rm \
  --network=host \
  -e DATABASE_URL="$DATABASE_URL" \
  -v /tmp/test-dashboard:/output \
  soulseek-research:latest \
  uv run python /app/scripts/generate_stats.py

# Check that period pages were generated
ls -lh /tmp/test-dashboard/docs/weeks/
ls -lh /tmp/test-dashboard/docs/months/
```

**Expected Result**:
- Dashboard generation completes in < 5 minutes (was timing out before)
- Period pages exist in `docs/weeks/` and `docs/months/`
- Period pages contain period-specific visualizations (not global data)

## Test 6: Cron Schedule

Verify the automated refresh is scheduled correctly.

```bash
# Check crontab
crontab -l

# Expected output should include:
# 0 2 * * 0 /usr/local/bin/weekly-archive.sh
# 0 4 * * * /usr/local/bin/refresh-views.sh
# 30 4 * * * /usr/local/bin/refresh-period-stats.sh

# Wait for next scheduled run (or simulate by setting system time)
# Check log after 4:30 AM run
tail -50 /var/log/soulseek-period-stats.log

# Verify refresh completed
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "
    SELECT
      period_type,
      MAX(period_id) as latest_period,
      COUNT(*) as query_count
    FROM period_top_queries
    GROUP BY period_type;
  "
```

**Expected Result**:
- Cron job runs daily at 4:30 AM
- Log shows successful completion
- Latest period ID reflects current week/month

## Test 7: Data Accuracy

Verify that precomputed stats match on-demand calculations.

```bash
# Pick a test period (e.g., 2026-W05)
TEST_PERIOD="2026-W05"

# Get precomputed results
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -t -c "
    SELECT query_normalized, unique_users, total_searches
    FROM period_top_queries
    WHERE period_type = 'week' AND period_id = '$TEST_PERIOD'
    ORDER BY rank
    LIMIT 5;
  " > /tmp/precomputed.txt

# Get on-demand results (calculate manually)
# First, determine date range for 2026-W05
START_DATE="2026-02-02"  # Monday of week 5
END_DATE="2026-02-08"    # Sunday of week 5

docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -t -c "
    SELECT
      LOWER(TRIM(query)) as query_normalized,
      COUNT(DISTINCT username) as unique_users,
      COUNT(*) as total_searches
    FROM searches
    WHERE timestamp >= '$START_DATE' AND timestamp <= '$END_DATE 23:59:59'
    GROUP BY LOWER(TRIM(query))
    HAVING COUNT(DISTINCT username) >= 5
    ORDER BY unique_users DESC, total_searches DESC
    LIMIT 5;
  " > /tmp/ondemand.txt

# Compare results
diff /tmp/precomputed.txt /tmp/ondemand.txt
```

**Expected Result**: No differences (or minimal due to rounding/formatting).

## Test 8: Idempotency

Verify that running refresh multiple times produces consistent results.

```bash
# Run refresh twice
/usr/local/bin/refresh-period-stats.sh
sleep 5

# Capture row counts
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -t -c "
    SELECT period_type, COUNT(*) FROM period_top_queries GROUP BY period_type;
  " > /tmp/count1.txt

/usr/local/bin/refresh-period-stats.sh
sleep 5

docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -t -c "
    SELECT period_type, COUNT(*) FROM period_top_queries GROUP BY period_type;
  " > /tmp/count2.txt

# Compare counts
diff /tmp/count1.txt /tmp/count2.txt
```

**Expected Result**: Identical counts (idempotent operation).

## Test 9: Error Handling

Test script behavior with missing/invalid data.

```bash
# Test with empty searches table (should fail gracefully)
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "SELECT COUNT(*) FROM searches;"

# If count is 0, run refresh
/usr/local/bin/refresh-period-stats.sh

# Check error message in log
tail -10 /var/log/soulseek-period-stats.log

# Expected: "ERROR: No data in searches table" or similar

# Test with invalid DATABASE_URL (simulate connection failure)
DATABASE_URL="postgresql://invalid:invalid@invalid:5432/invalid" \
  docker run --rm \
  -e DATABASE_URL="$DATABASE_URL" \
  soulseek-research:latest \
  uv run python /app/scripts/refresh_period_stats.py

# Expected: Connection error with traceback
```

**Expected Result**: Script exits with clear error messages, doesn't crash silently.

## Test 10: Period Page Content

Verify period pages show ONLY period-specific data (critical requirement).

```bash
# Generate a test period page
# (This is normally done by GitHub Actions, testing manually)

# Check that top queries in period page are period-specific
# 1. View precomputed data for a period
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulsesk -c "
    SELECT query_normalized, unique_users
    FROM period_top_queries
    WHERE period_type = 'month' AND period_id = '2026-01'
    ORDER BY rank
    LIMIT 5;
  "

# 2. Check global top queries (should be different)
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c "
    SELECT query_normalized, unique_users
    FROM mv_top_queries
    ORDER BY unique_users DESC
    LIMIT 5;
  "

# 3. Generate actual dashboard and inspect HTML
# Look for period-specific queries in docs/months/2026-01.html
```

**Expected Result**: Period pages display different top queries than global all-time page.

## Troubleshooting

### Issue: Script times out during refresh

**Solution**: Increase `statement_timeout` in `refresh_period_stats.py`:
```python
options='-c statement_timeout=7200000'  # 2 hours
```

### Issue: Missing periods in precomputed tables

**Check**:
1. Were there any searches during that period?
   ```sql
   SELECT COUNT(*) FROM searches
   WHERE timestamp >= 'YYYY-MM-DD' AND timestamp < 'YYYY-MM-DD';
   ```

2. Did any queries have 5+ unique users?
   ```sql
   SELECT COUNT(DISTINCT query) FROM (
     SELECT query, COUNT(DISTINCT username) as users
     FROM searches
     WHERE timestamp >= 'YYYY-MM-DD' AND timestamp < 'YYYY-MM-DD'
     GROUP BY query
     HAVING COUNT(DISTINCT username) >= 5
   ) t;
   ```

**Solution**: If no queries meet threshold, that's expected (period won't have entries).

### Issue: Duplicate entries in period tables

**Check**:
```sql
SELECT period_type, period_id, query_normalized, COUNT(*)
FROM period_top_queries
GROUP BY period_type, period_id, query_normalized
HAVING COUNT(*) > 1;
```

**Solution**: Should be impossible due to UNIQUE constraint. If found, manually delete duplicates and re-run refresh.

## Success Criteria

Implementation is successful if:

1. Tables `period_top_queries` and `period_query_length_dist` exist with indexes
2. Refresh script runs without errors and completes in reasonable time
3. Precomputed data matches on-demand calculations
4. Period pages generate in < 10 seconds (was 9+ minutes)
5. Period pages show period-specific data (not global data)
6. Cron job runs daily at 4:30 AM and updates statistics
7. Dashboard generation completes successfully via GitHub Actions

## Performance Benchmarks

Record baseline measurements:

| Metric                    | Before (On-Demand) | After (Precomputed) | Improvement |
|---------------------------|--------------------|---------------------|-------------|
| Period page generation    | 9+ min             | < 10 sec            | 54x faster  |
| Total dashboard generation| TIMEOUT            | < 5 min             | ✓ Works     |
| Database load during gen  | Very High          | Minimal             | ~95% less   |
| Period query response     | 540+ sec           | < 0.01 sec          | 54,000x     |

## Rollback Plan

If implementation fails, rollback procedure:

1. Remove new tables:
   ```sql
   DROP TABLE period_top_queries;
   DROP TABLE period_query_length_dist;
   ```

2. Restore old `generate_stats.py`:
   ```bash
   git checkout HEAD~1 scripts/generate_stats.py
   ```

3. Remove cron job:
   ```bash
   crontab -e
   # Delete line: 30 4 * * * /usr/local/bin/refresh-period-stats.sh
   ```

4. Revert to global data for period pages (temporary workaround):
   - Period pages will show global stats instead of period-specific
   - This violates user requirement but allows dashboard to function
