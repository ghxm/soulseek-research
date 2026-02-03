#!/bin/bash
# Cloud-init script to set up database server

# Install Docker and docker-compose
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
systemctl enable docker
systemctl start docker

# Install docker-compose
curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose

# Create archive directory on local storage
mkdir -p /opt/archives
chmod 755 /opt/archives

# Clone repo first to use existing files
cd /opt
git clone https://github.com/ghxm/soulseek-research.git
cd soulseek-research

# Create environment file for database.yml
cat > .env << EOF
DB_PASSWORD=${db_password}
EOF

# Start database
docker-compose -f database.yml up -d

# Wait for database to be ready
sleep 10

# Create database tables
docker exec $(docker-compose -f database.yml ps -q database) psql -U soulseek -d soulseek -c "
CREATE TABLE IF NOT EXISTS searches (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(255),
    timestamp TIMESTAMPTZ,
    username TEXT,
    query TEXT
);
CREATE TABLE IF NOT EXISTS archives (
    id SERIAL PRIMARY KEY,
    month VARCHAR(7),
    file_path TEXT,
    record_count INTEGER,
    file_size BIGINT,
    archived_at TIMESTAMPTZ,
    deleted BOOLEAN DEFAULT FALSE
);

-- Cumulative stats table: stores all-time totals including archived data
CREATE TABLE IF NOT EXISTS stats_cumulative (
    id INTEGER PRIMARY KEY DEFAULT 1,
    total_searches BIGINT DEFAULT 0,
    total_users BIGINT DEFAULT 0,
    total_queries BIGINT DEFAULT 0,
    total_search_pairs BIGINT DEFAULT 0,
    first_search TIMESTAMPTZ,
    last_search TIMESTAMPTZ,
    client_totals JSONB DEFAULT '{}',
    last_archive_month VARCHAR(7),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
);

-- Initialize cumulative stats row if not exists
INSERT INTO stats_cumulative (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"

# Create materialized views for fast stats generation
echo "Creating materialized views..."
docker exec $(docker-compose -f database.yml ps -q database) psql -U soulseek -d soulseek -c "
-- Daily stats by client (for time-series charts)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_stats AS
SELECT
    client_id,
    DATE(timestamp) as date,
    COUNT(*) as search_count,
    COUNT(DISTINCT username) as unique_users
FROM searches
GROUP BY client_id, DATE(timestamp)
ORDER BY date, client_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_stats_unique
ON mv_daily_stats (date, client_id);

-- Top queries by unique users (global, not per-period)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_queries AS
SELECT
    LOWER(TRIM(query)) as query_normalized,
    COUNT(DISTINCT username) as unique_users,
    COUNT(*) as total_searches
FROM searches
GROUP BY LOWER(TRIM(query))
ORDER BY unique_users DESC, total_searches DESC
LIMIT 100;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_top_queries_unique
ON mv_top_queries (query_normalized);

-- Query length distribution (word count)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_query_length_dist AS
SELECT
    LENGTH(query) - LENGTH(REPLACE(query, ' ', '')) + 1 as word_count,
    COUNT(DISTINCT LOWER(TRIM(query))) as unique_query_count
FROM searches
WHERE LENGTH(query) - LENGTH(REPLACE(query, ' ', '')) + 1 <= 100
GROUP BY word_count
ORDER BY word_count;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_query_length_unique
ON mv_query_length_dist (word_count);

-- Summary stats for live data (single row)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_summary_stats AS
SELECT
    (SELECT COUNT(*) FROM searches) as total_searches,
    (SELECT COUNT(DISTINCT username) FROM searches) as total_users,
    (SELECT COUNT(DISTINCT LOWER(TRIM(query))) FROM searches) as total_queries,
    (SELECT COUNT(DISTINCT (username || '|' || LOWER(TRIM(query)))) FROM searches) as total_search_pairs,
    (SELECT MIN(timestamp) FROM searches) as first_search,
    (SELECT MAX(timestamp) FROM searches) as last_search,
    (SELECT COALESCE(jsonb_object_agg(client_id, client_count), '{}'::jsonb)
     FROM (SELECT client_id, COUNT(*) as client_count FROM searches GROUP BY client_id) c
    ) as client_totals;
"

# Build image for Germany client (repo already cloned above)
docker build -t soulseek-research:latest .

# Create client environment file
cat > client.env << EOF
DATABASE_URL=postgresql+asyncpg://soulseek:${db_password}@172.17.0.1:5432/soulseek
SOULSEEK_USERNAME=${germany_username}
SOULSEEK_PASSWORD=${germany_password}
CLIENT_ID=germany
ENCRYPTION_KEY=${encryption_key}
EOF

# Start client with restart policy and port mapping
docker run -d \
  --name soulseek-client \
  --restart unless-stopped \
  -p 60000:60000 \
  -p 60001:60001 \
  --env-file /opt/soulseek-research/client.env \
  soulseek-research:latest

# Create archive script
cat > /usr/local/bin/weekly-archive.sh << 'ARCHIVE_SCRIPT'
#!/bin/bash
cd /opt/soulseek-research

# Build archive command
DB_URL="postgresql://soulseek:${db_password}@localhost:5432/soulseek"

# Run archive using Docker
docker run --rm \
  --network=host \
  -v /opt/archives:/archives \
  -e DATABASE_URL="$DB_URL" \
  -e ARCHIVE_PATH=/archives \
  -e DELETE_AFTER_ARCHIVE=true \
  soulseek-research:latest \
  uv run python /app/scripts/archive.py

# Log the result
echo "$(date): Weekly archive completed" >> /var/log/soulseek-archive.log
ARCHIVE_SCRIPT

chmod +x /usr/local/bin/weekly-archive.sh

# Create view refresh script
cat > /usr/local/bin/refresh-views.sh << 'REFRESH_SCRIPT'
#!/bin/bash
# Refresh materialized views for stats generation
# Runs daily at 4 AM UTC (before 5 AM GitHub Actions run)

cd /opt/soulseek-research

DB_CONTAINER=$(docker-compose -f database.yml ps -q database)

# Refresh views concurrently (allows reads during refresh)
docker exec $DB_CONTAINER psql -U soulseek -d soulseek -c "
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_stats;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_queries;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_query_length_dist;
REFRESH MATERIALIZED VIEW mv_summary_stats;
"

echo "$(date): Materialized views refreshed" >> /var/log/soulseek-views.log
REFRESH_SCRIPT

chmod +x /usr/local/bin/refresh-views.sh

# Set up cron jobs:
# - Weekly archive: Sunday 2 AM
# - Daily view refresh: 4 AM (before GitHub Actions at 5 AM)
(
echo "0 2 * * 0 /usr/local/bin/weekly-archive.sh"
echo "0 4 * * * /usr/local/bin/refresh-views.sh"
) | crontab -

# View refresh log rotation
cat > /etc/logrotate.d/soulseek-views << EOF
/var/log/soulseek-views.log {
    rotate 30
    daily
    compress
    missingok
    delaycompress
}
EOF

# Set up log rotation
cat > /etc/logrotate.d/docker << EOF
/var/lib/docker/containers/*/*-json.log {
    rotate 7
    daily
    compress
    size 50M
    missingok
    delaycompress
    copytruncate
}
EOF

# Archive log rotation
cat > /etc/logrotate.d/soulseek-archive << EOF
/var/log/soulseek-archive.log {
    rotate 12
    monthly
    compress
    missingok
    delaycompress
}
EOF