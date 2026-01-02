#!/bin/bash
# Cloud-init script to set up client server

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
systemctl enable docker
systemctl start docker

# Install docker-compose
curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose

# Clone repo and build image
cd /opt
git clone https://github.com/ghxm/soulseek-research.git
cd soulseek-research
docker build -t soulseek-research:latest .

# Create environment file with all required vars
cat > .env << EOF
DATABASE_URL=postgresql+asyncpg://soulseek:${db_password}@${db_host}:5432/soulseek
SOULSEEK_USERNAME=${soulseek_username}
SOULSEEK_PASSWORD=${soulseek_password}
CLIENT_ID=${client_id}
ENCRYPTION_KEY=${encryption_key}
EOF

# Start client with restart policy and port mapping
docker run -d \
  --name soulseek-client \
  --restart unless-stopped \
  -p 60000:60000 \
  -p 60001:60001 \
  --env-file /opt/soulseek-research/.env \
  soulseek-research:latest

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