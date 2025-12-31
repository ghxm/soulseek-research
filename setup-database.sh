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

# Create project directory and files
cd /opt
mkdir -p soulseek-research
cd soulseek-research

# Create database.yml file directly
cat > database.yml << 'DBCOMPOSE'
version: '3.8'

services:
  database:
    image: postgres:15
    environment:
      POSTGRES_DB: soulseek
      POSTGRES_USER: soulseek
      POSTGRES_PASSWORD: $${DB_PASSWORD:-changeme123}
    volumes:
      - db_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    restart: unless-stopped

volumes:
  db_data:
DBCOMPOSE

# Create client.yml file directly  
cat > client.yml << 'CLIENTCOMPOSE'
version: '3.8'

services:
  client:
    image: soulseek-research:latest
    environment:
      DATABASE_URL: postgresql+asyncpg://soulseek:$${DB_PASSWORD}@localhost:5432/soulseek
      SOULSEEK_USERNAME: $${SOULSEEK_USERNAME}
      SOULSEEK_PASSWORD: $${SOULSEEK_PASSWORD}
      CLIENT_ID: $${CLIENT_ID:-client}
      ENCRYPTION_KEY: research_encryption_2025
    restart: unless-stopped
CLIENTCOMPOSE

# Create environment file
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

# Clone repo and build image for Germany client
git clone https://github.com/ghxm/soulseek-research.git repo
docker build -t soulseek-research:latest repo

# Create Germany client environment
cat > germany-client.env << EOF
DATABASE_URL=postgresql+asyncpg://soulseek:${db_password}@localhost:5432/soulseek
SOULSEEK_USERNAME=${germany_username}
SOULSEEK_PASSWORD=${germany_password}
CLIENT_ID=germany
ENCRYPTION_KEY=research_encryption_2025
EOF

# Start Germany client with restart policy
docker run -d \
  --name soulseek-germany-client \
  --restart unless-stopped \
  --env-file /opt/soulseek-research/germany-client.env \
  soulseek-research:latest

# Create archive script
cat > /usr/local/bin/monthly-archive.sh << 'ARCHIVE_SCRIPT'
#!/bin/bash
cd /opt/soulseek-research

# Build archive command
DB_URL="postgresql://soulseek:${db_password}@localhost:5432/soulseek"

# Run archive using Docker
docker run --rm \
  --network="$(docker-compose -f database.yml config --services | head -1)_default" \
  -v /opt/archives:/archives \
  -e DATABASE_URL="$DB_URL" \
  soulseek-research:latest \
  soulseek-research archive --database-url "$DB_URL" --archive-path /archives

# Log the result
echo "$(date): Monthly archive completed" >> /var/log/soulseek-archive.log
ARCHIVE_SCRIPT

chmod +x /usr/local/bin/monthly-archive.sh

# Set up monthly cron job (runs on 1st of each month at 2 AM)
echo "0 2 1 * * /usr/local/bin/monthly-archive.sh" | crontab -

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