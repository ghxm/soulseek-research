#!/bin/bash
# Final production deployment for 1-year operation

set -e
echo "🎯 Production Deployment for 1-Year Operation"
echo "=============================================="

# Prerequisites check
echo "📋 Checking prerequisites..."
command -v terraform >/dev/null 2>&1 || { echo "❌ Terraform not installed"; exit 1; }

if [ ! -f terraform.tfvars ]; then
    echo "❌ terraform.tfvars not found"
    echo "   Required variables:"
    echo "   - hcloud_token (Hetzner Cloud API token)"
    echo "   - db_password (secure database password)"
    echo "   - soulseek_credentials (multiple Soulseek accounts)"
    exit 1
fi

# Validate configuration
echo "🔍 Validating configuration..."
terraform validate || { echo "❌ Configuration invalid"; exit 1; }

# Show deployment plan
echo "📊 Reviewing deployment plan..."
terraform plan

echo ""
echo "🎯 This will deploy:"
echo "   • 1 Database server (cx21) - €4.90/month"
echo "   • 100GB Archive storage - €10/month"  
echo "   • N Client servers (cx11) - €3.29/month each"
echo "   • Automatic monthly archival"
echo "   • 1-year estimated cost: €300-400 depending on client count"
echo ""

read -p "🚀 Deploy production infrastructure? (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "❌ Deployment cancelled"
    exit 1
fi

echo "🚀 Deploying production infrastructure..."
terraform apply || { echo "❌ Deployment failed"; exit 1; }

# Get server IPs
DB_IP=$(terraform output -raw database_ip)
echo "✅ Database server: $DB_IP"

echo "✅ Client servers:"
terraform output -json client_ips | jq -r 'to_entries[] | "   \(.key): \(.value)"'

echo ""
echo "⏳ Waiting for initial setup (5 minutes)..."
sleep 300

echo "🔍 Verifying deployment..."

# Check database
echo "--- Checking database server ---"
ssh -o StrictHostKeyChecking=no root@$DB_IP "
    echo '✅ Database status:';
    docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -v NAMES;
    echo '✅ Archive storage:';
    df -h /mnt/archives;
    echo '✅ Cron job:';
    crontab -l;
" || echo "⚠️ Database check failed"

# Check clients
echo "--- Checking client servers ---"
terraform output -json client_ips | jq -r 'to_entries[] | "\(.key) \(.value)"' | while read region ip; do
    echo "Checking client $region ($ip)..."
    ssh -o StrictHostKeyChecking=no root@$ip "
        echo '✅ Client status:';
        docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -v NAMES;
    " || echo "⚠️ Client $region check failed"
done

echo ""
echo "🎉 PRODUCTION DEPLOYMENT COMPLETE!"
echo "================================="
echo ""
echo "📊 Infrastructure Summary:"
echo "  Database: $DB_IP"
echo "  Clients:  $(terraform output -json client_ips | jq '. | length') servers deployed"
echo ""
echo "🔍 Monitoring & Management:"
echo "  Check all services: ./monitor-production.sh"
echo "  View database stats: ssh root@$DB_IP 'docker exec -it \$(docker ps -q) psql -U research -d research -c \"SELECT COUNT(*) FROM searches;\"'"
echo "  Archive status: ssh root@$DB_IP 'ls -la /mnt/archives/'"
echo ""
echo "⚠️  Important Notes:"
echo "  • Servers are now running and incurring costs"
echo "  • Monthly archival runs automatically on 1st of each month"
echo "  • Monitor logs regularly for issues"
echo "  • Keep terraform.tfvars secure (contains credentials)"
echo ""
echo "📅 Next Steps:"
echo "  1. Set up monitoring alerts"
echo "  2. Schedule regular health checks"  
echo "  3. Plan for credential rotation (every 6 months)"
echo "  4. Monitor costs and scale as needed"