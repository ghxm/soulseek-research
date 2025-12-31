#!/bin/bash
# Production monitoring script for ongoing operations

set -e
echo "📊 Production Health Check"
echo "========================="

# Get server IPs
DB_IP=$(terraform output -raw database_ip 2>/dev/null) || { 
    echo "❌ Cannot get database IP. Run from terraform directory."; exit 1; 
}

echo "🗄️ Database Server Health ($DB_IP)"
echo "-----------------------------------"

ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$DB_IP "
    echo '📊 System Status:';
    uptime;
    echo '';
    echo '🐳 Docker Containers:';
    docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}';
    echo '';
    echo '💾 Storage Usage:';
    df -h / /mnt/archives | grep -E '(Filesystem|/dev/)';
    echo '';
    echo '📈 Database Stats:';
    docker exec \$(docker ps -q -f name=database) psql -U research -d research -c \"
        SELECT 
            'Total searches: ' || COUNT(*) as searches
        FROM searches
        UNION ALL
        SELECT 
            'Archives: ' || COUNT(*) as archives  
        FROM archives
        UNION ALL
        SELECT
            'Latest search: ' || MAX(timestamp) as latest
        FROM searches;
    \" 2>/dev/null || echo 'Database not accessible';
    echo '';
    echo '📅 Archive Status:';
    ls -la /mnt/archives/ | tail -5;
    echo '';
    echo '⚙️ Cron Status:';
    systemctl status cron | grep -E '(Active|Main PID)' || echo 'Cron status unknown';
" || echo "❌ Database server unreachable"

echo ""
echo "🤖 Client Servers Health"
echo "------------------------"

terraform output -json client_ips 2>/dev/null | jq -r 'to_entries[] | "\(.key) \(.value)"' | while read region ip; do
    echo "🌍 Client $region ($ip):"
    
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$ip "
        echo '  📊 System: '\$(uptime | cut -d',' -f1);
        echo '  🐳 Docker: '\$(docker ps --format '{{.Status}}' | head -1 || echo 'Not running');
        echo '  📝 Last log: '\$(docker logs --tail=1 \$(docker ps -q) 2>/dev/null | head -1 || echo 'No logs');
        echo '  🔗 DB conn: '\$(docker exec \$(docker ps -q) timeout 5 nc -z $DB_IP 5432 && echo 'OK' || echo 'FAIL');
    " || echo "  ❌ Unreachable"
    echo ""
done

echo "💰 Cost Estimate (Current Month)"
echo "--------------------------------"
CLIENTS=$(terraform output -json client_ips 2>/dev/null | jq '. | length' || echo "0")
DB_COST=4.90
STORAGE_COST=10.00
CLIENT_COST=$(echo "$CLIENTS * 3.29" | bc -l 2>/dev/null || echo "0")
TOTAL=$(echo "$DB_COST + $STORAGE_COST + $CLIENT_COST" | bc -l 2>/dev/null || echo "0")

echo "  Database server: €$DB_COST"
echo "  Archive storage: €$STORAGE_COST"  
echo "  $CLIENTS clients: €$CLIENT_COST"
echo "  -------------------"
echo "  Monthly total: €$TOTAL"

echo ""
echo "🚨 Quick Health Summary"
echo "----------------------"

# Overall health check
ISSUES=0

# Check database reachability
if ! ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 root@$DB_IP "exit" 2>/dev/null; then
    echo "❌ Database server unreachable"
    ISSUES=$((ISSUES + 1))
fi

# Check client reachability
terraform output -json client_ips 2>/dev/null | jq -r 'values[]' | while read ip; do
    if ! ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 root@$ip "exit" 2>/dev/null; then
        echo "❌ Client $ip unreachable"
        ISSUES=$((ISSUES + 1))
    fi
done

if [ $ISSUES -eq 0 ]; then
    echo "✅ All systems operational"
else
    echo "⚠️  $ISSUES issues detected"
fi

echo ""
echo "🔧 Maintenance Commands"
echo "----------------------"
echo "  Restart database: ssh root@$DB_IP 'cd /opt/soulseek-research && docker-compose -f database.yml restart'"
echo "  View DB logs: ssh root@$DB_IP 'docker logs -f \$(docker ps -q)'"
echo "  Manual archive: ssh root@$DB_IP '/usr/local/bin/monthly-archive.sh'"
echo "  Scale clients: terraform apply (modify terraform.tfvars first)"
echo "  Emergency shutdown: terraform destroy"