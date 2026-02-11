# Period Statistics Deployment Guide

Quick reference for deploying the period statistics precomputation system.

## For New Infrastructure

Tables and cron jobs are created automatically by `setup-database.sh`.

After deployment:

```bash
# SSH to database server
ssh root@<DB_SERVER_IP>

# Wait for some data to be collected (at least a few days)
# Then run initial population
/usr/local/bin/refresh-period-stats.sh

# Verify
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c \
  "SELECT period_type, COUNT(*) FROM period_top_queries GROUP BY period_type;"
```

## For Existing Infrastructure

### Step 1: Update Code

```bash
ssh root@<DB_SERVER_IP>
cd /opt/soulseek-research
git pull origin main
```

### Step 2: Create Tables

```bash
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek << 'SQL'

CREATE TABLE IF NOT EXISTS period_top_queries (
    id SERIAL PRIMARY KEY,
    period_type VARCHAR(10) NOT NULL,
    period_id VARCHAR(20) NOT NULL,
    query_normalized TEXT NOT NULL,
    unique_users INTEGER NOT NULL,
    total_searches INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    UNIQUE(period_type, period_id, query_normalized)
);

CREATE INDEX IF NOT EXISTS idx_period_top_queries_lookup
ON period_top_queries(period_type, period_id, rank);

CREATE TABLE IF NOT EXISTS period_query_length_dist (
    id SERIAL PRIMARY KEY,
    period_type VARCHAR(10) NOT NULL,
    period_id VARCHAR(20) NOT NULL,
    query_length INTEGER NOT NULL,
    unique_query_count INTEGER NOT NULL,
    UNIQUE(period_type, period_id, query_length)
);

CREATE INDEX IF NOT EXISTS idx_period_ql_dist_lookup
ON period_query_length_dist(period_type, period_id);

SQL
```

### Step 3: Create Refresh Script

Replace `<DB_PASSWORD>` with actual password:

```bash
cat > /usr/local/bin/refresh-period-stats.sh << 'SCRIPT'
#!/bin/bash
cd /opt/soulseek-research
DB_URL="postgresql://soulseek:<DB_PASSWORD>@localhost:5432/soulseek"
docker run --rm \
  --network=host \
  -e DATABASE_URL="$DB_URL" \
  soulseek-research:latest \
  uv run python /app/scripts/refresh_period_stats.py
echo "$(date): Period stats refreshed" >> /var/log/soulseek-period-stats.log
SCRIPT

chmod +x /usr/local/bin/refresh-period-stats.sh
```

### Step 4: Update Crontab

```bash
crontab -l > /tmp/cron.txt
echo "30 4 * * * /usr/local/bin/refresh-period-stats.sh" >> /tmp/cron.txt
crontab /tmp/cron.txt
rm /tmp/cron.txt

# Verify
crontab -l
```

### Step 5: Rebuild Docker Image

```bash
cd /opt/soulseek-research
docker build -t soulseek-research:latest .
```

### Step 6: Initial Population

This may take 30+ minutes for large datasets:

```bash
time /usr/local/bin/refresh-period-stats.sh
```

### Step 7: Verify

```bash
# Check log
tail -50 /var/log/soulseek-period-stats.log

# Check data
docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) \
  psql -U soulseek -d soulseek -c \
  "SELECT period_type, COUNT(*) as periods, MIN(period_id), MAX(period_id)
   FROM period_top_queries GROUP BY period_type;"

# Should show weeks and months with data
```

## Verification Checklist

After deployment:

- [ ] Tables created: `\dt period_*` shows 2 tables
- [ ] Indexes created: `\di idx_period_*` shows 2 indexes
- [ ] Script exists: `ls -l /usr/local/bin/refresh-period-stats.sh`
- [ ] Cron scheduled: `crontab -l | grep refresh-period-stats`
- [ ] Data populated: `SELECT COUNT(*) FROM period_top_queries;` > 0
- [ ] Dashboard generates: Next 5 AM run succeeds

## Monitoring

Daily health check:

```bash
# Check last refresh
tail -1 /var/log/soulseek-period-stats.log

# Check period coverage
psql -U soulseek -d soulseek -c \
  "SELECT period_type, COUNT(*), MAX(period_id) FROM period_top_queries GROUP BY period_type;"
```

## Troubleshooting

**Script not found**: Check `/usr/local/bin/refresh-period-stats.sh` exists and is executable

**Cron not running**: Check `crontab -l` and verify time is UTC

**Tables empty**: Run manual refresh: `/usr/local/bin/refresh-period-stats.sh`

**Refresh fails**: Check logs: `tail -50 /var/log/soulseek-period-stats.log`

## Complete Documentation

- Technical details: `PERIOD_STATS_IMPLEMENTATION.md`
- Testing procedures: `TESTING_PERIOD_STATS.md`
- Overview: `IMPLEMENTATION_SUMMARY.md`
- Architecture: `CLAUDE.md`
