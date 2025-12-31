#!/bin/bash
# Cloud-init script to set up client server

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
systemctl enable docker
systemctl start docker

# Clone repo and start client
cd /opt
git clone https://github.com/your-repo/soulseek-research.git
cd soulseek-research

# Create environment file
cat > .env << EOF
DB_HOST=${db_host}
DB_PASSWORD=${db_password}
SOULSEEK_USERNAME=${soulseek_username}
SOULSEEK_PASSWORD=${soulseek_password}
CLIENT_ID=${client_id}
EOF

# Start client
docker-compose -f client.yml up -d

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