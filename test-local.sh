#!/bin/bash
# Local Docker testing script

set -e
echo "🧪 Starting local testing..."

# Prerequisites check
echo "📋 Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "❌ Docker not installed"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "❌ docker-compose not installed"; exit 1; }

# Create test environment file
echo "⚙️ Creating test environment..."
cat > .env << EOF
DB_PASSWORD=test123
SOULSEEK_USERNAME=your_test_username
SOULSEEK_PASSWORD=your_test_password  
CLIENT_ID=test-client-1
DB_HOST=database
EOF

echo "🔨 Building client Docker image..."
docker build -t soulseek-research . || { echo "❌ Docker build failed"; exit 1; }

echo "🗄️ Starting database..."
docker-compose -f database.yml up -d || { echo "❌ Database startup failed"; exit 1; }

echo "⏳ Waiting for database to be ready..."
sleep 10

echo "🚀 Starting client..."
docker-compose -f client.yml up -d || { echo "❌ Client startup failed"; exit 1; }

echo "📊 Checking container status..."
docker-compose -f database.yml ps
docker-compose -f client.yml ps

echo "📝 Showing logs (last 20 lines)..."
echo "--- Database logs ---"
docker-compose -f database.yml logs --tail=20

echo "--- Client logs ---" 
docker-compose -f client.yml logs --tail=20

echo "🧪 Running basic tests..."

# Test database connectivity
echo "🔌 Testing database connection..."
docker run --rm --network="$(docker-compose -f database.yml config --services | head -1)_default" postgres:15 \
  psql postgresql://research:test123@database:5432/research -c "SELECT version();" || \
  { echo "❌ Database connection failed"; exit 1; }

# Test CLI commands
echo "📊 Testing CLI commands..."
docker run --rm --network="$(docker-compose -f database.yml config --services | head -1)_default" \
  soulseek-research:latest \
  soulseek-research stats --database-url postgresql://research:test123@database:5432/research || \
  { echo "❌ CLI stats failed"; exit 1; }

echo "✅ Local testing completed successfully!"
echo ""
echo "🔍 To monitor:"
echo "  Database logs: docker-compose -f database.yml logs -f"
echo "  Client logs:   docker-compose -f client.yml logs -f" 
echo "  Database CLI:  docker run --rm -it --network=\"soulseek-research_default\" postgres:15 psql postgresql://research:test123@database:5432/research"
echo ""
echo "🧹 To cleanup:"
echo "  docker-compose -f client.yml down"
echo "  docker-compose -f database.yml down"
echo "  docker system prune -f"