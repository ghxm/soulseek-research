#!/bin/bash
# Test local setup with Docker Compose

set -e

cd "$(dirname "$0")/.."

echo "🧪 Testing local Soulseek Research setup..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Create test environment file
cat > .env.test << EOF
DB_PASSWORD=test123
SOULSEEK_USERNAME=test_user
SOULSEEK_PASSWORD=test_pass
CLIENT_ID=local-test
EOF

echo "🐳 Starting local database..."
docker-compose -f database.yml --env-file .env.test up -d

echo "⏳ Waiting for database to be ready..."
sleep 10

echo "🔍 Testing database connection..."
docker exec $(docker-compose -f database.yml ps -q database) pg_isready -U research -d research

echo "📦 Building research client image..."
docker build -t soulseek-research:latest .

echo "🔧 Testing client startup (dry run)..."
docker run --rm --network host soulseek-research:latest soulseek-research --help

echo "🧹 Cleaning up..."
docker-compose -f database.yml --env-file .env.test down -v
rm -f .env.test

echo "✅ Local test completed successfully!"