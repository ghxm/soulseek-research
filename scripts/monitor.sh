#!/bin/bash
# Monitor production deployment

set -e

if [ ! -f "terraform.tfstate" ]; then
    echo "❌ No terraform state found. Deploy infrastructure first."
    exit 1
fi

DB_IP=$(terraform output -raw database_ip)

echo "📊 Monitoring Soulseek Research Production..."
echo "Database: $DB_IP"
echo ""

# Database status
echo "🗄️  Database Status:"
ssh -o StrictHostKeyChecking=no root@$DB_IP "docker-compose -f /opt/soulseek-research/database.yml ps"

echo ""
echo "📈 Search Count (last 24h):"
ssh -o StrictHostKeyChecking=no root@$DB_IP "docker exec \$(docker-compose -f /opt/soulseek-research/database.yml ps -q database) psql -U soulseek -d soulseek -c \"SELECT client_id, COUNT(*) as searches FROM searches WHERE timestamp > NOW() - INTERVAL '24 hours' GROUP BY client_id ORDER BY searches DESC;\""

echo ""
echo "📋 Recent Searches:"
ssh -o StrictHostKeyChecking=no root@$DB_IP "docker exec \$(docker-compose -f /opt/soulseek-research/database.yml ps -q database) psql -U soulseek -d soulseek -c \"SELECT client_id, timestamp, query FROM searches ORDER BY timestamp DESC LIMIT 10;\""

# Client servers status
echo ""
echo "🌐 Client Servers:"
terraform output -json client_ips | jq -r 'to_entries[] | "\(.key): \(.value)"' | while read line; do
    region=$(echo $line | cut -d: -f1)
    ip=$(echo $line | cut -d: -f2 | tr -d ' ')
    echo "📍 $region ($ip):"
    ssh -o StrictHostKeyChecking=no root@$ip "docker ps --format 'table {{.Names}}\t{{.Status}}'" | grep -E "(NAMES|soulseek)" || echo "  No client running"
done