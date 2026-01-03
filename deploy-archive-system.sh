#!/bin/bash
# Quick deployment script for archival system
# Run this on the DATABASE SERVER after setting up GitHub secrets

set -e

echo "🚀 Deploying Archival System to Database Server"
echo "================================================"

# Check if we're in the right directory
if [ ! -f "setup-database.sh" ]; then
    echo "❌ Error: Must run from /opt/soulseek-research directory"
    exit 1
fi

# Get database password
read -sp "Enter database password: " DB_PASSWORD
echo ""

# Pull latest code
echo "📥 Pulling latest code..."
git pull origin main

# Rebuild Docker image
echo "🐳 Rebuilding Docker image..."
docker build -t soulseek-research:latest .

# Create archive directory
echo "📁 Creating archive directory..."
mkdir -p /opt/archives
chmod 755 /opt/archives

# Create archive script
echo "📝 Creating archive script..."
cat > /usr/local/bin/weekly-archive.sh << ARCHIVE_SCRIPT
#!/bin/bash
cd /opt/soulseek-research

# Build archive command
DB_URL="postgresql://soulseek:${DB_PASSWORD}@localhost:5432/soulseek"

# Run archive using Docker
docker run --rm \\
  --network=host \\
  -v /opt/archives:/archives \\
  -e DATABASE_URL="\$DB_URL" \\
  -e ARCHIVE_PATH=/archives \\
  -e DELETE_AFTER_ARCHIVE=true \\
  soulseek-research:latest \\
  python /app/scripts/archive.py

# Log the result
echo "\$(date): Weekly archive completed" >> /var/log/soulseek-archive.log
ARCHIVE_SCRIPT

chmod +x /usr/local/bin/weekly-archive.sh

# Backup existing cron
echo "💾 Backing up cron..."
crontab -l > /tmp/cron-backup-$(date +%Y%m%d-%H%M%S).txt 2>/dev/null || true

# Update cron
echo "⏰ Setting up cron job..."
echo "0 2 * * 0 /usr/local/bin/weekly-archive.sh" | crontab -

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📋 Next steps:"
echo "1. Test the archive script:"
echo "   docker run --rm --network=host -v /opt/archives:/archives \\"
echo "     -e DATABASE_URL='postgresql://soulseek:${DB_PASSWORD}@localhost:5432/soulseek' \\"
echo "     -e ARCHIVE_PATH=/archives -e DELETE_AFTER_ARCHIVE=false \\"
echo "     soulseek-research:latest python /app/scripts/archive.py"
echo ""
echo "2. Verify cron job:"
echo "   crontab -l"
echo ""
echo "3. Add GitHub Secrets (if not done):"
echo "   - DB_SERVER_SSH_KEY (SSH private key)"
echo "   - DB_SERVER_IP (this server's IP)"
echo ""
echo "4. Test GitHub Actions workflow manually"
echo ""
