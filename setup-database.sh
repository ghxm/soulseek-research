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
);"

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

# Set up weekly cron job (runs every Sunday at 2 AM)
echo "0 2 * * 0 /usr/local/bin/weekly-archive.sh" | crontab -

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