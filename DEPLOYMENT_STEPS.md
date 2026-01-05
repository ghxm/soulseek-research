# Archival System Deployment Steps

## 1. Configure GitHub Secrets

You need to add two new secrets to your GitHub repository:

### Generate SSH Key (if you don't have one)
```bash
# Generate a new SSH key for GitHub Actions
ssh-keygen -t ed25519 -f ~/.ssh/soulseek_github_actions -N "" -C "github-actions@soulseek-research"

# Display the public key (add this to your database server)
cat ~/.ssh/soulseek_github_actions.pub

# Display the private key (add this to GitHub Secrets)
cat ~/.ssh/soulseek_github_actions
```

### Add to GitHub Repository Secrets

Go to: https://github.com/YOUR_USERNAME/soulseek-research/settings/secrets/actions

Add these secrets:
- **Name**: `DB_SERVER_SSH_KEY`
  - **Value**: Contents of `~/.ssh/soulseek_github_actions` (private key)

- **Name**: `DB_SERVER_IP`
  - **Value**: Your database server IP address (e.g., `1.2.3.4`)

---

## 2. Add SSH Public Key to Database Server

SSH into your database server and add the public key:

```bash
# SSH to database server
ssh root@<your-database-ip>

# Add the public key to authorized_keys
echo "YOUR_PUBLIC_KEY_HERE" >> ~/.ssh/authorized_keys

# Set correct permissions
chmod 600 ~/.ssh/authorized_keys
chmod 700 ~/.ssh
```

---

## 3. Update Database Server Code

On the database server:

```bash
# Navigate to project directory
cd /opt/soulseek-research

# Pull latest code
git pull origin main

# Rebuild Docker image with new archive.py
docker build -t soulseek-research:latest .

# Verify image built successfully
docker images | grep soulseek-research
```

---

## 4. Update Cron Job

The cron job configuration has changed. Re-run the relevant part of setup:

```bash
# Backup existing cron
crontab -l > /tmp/cron-backup-$(date +%Y%m%d).txt

# The setup script creates the new archive script
cat > /usr/local/bin/weekly-archive.sh << 'ARCHIVE_SCRIPT'
#!/bin/bash
cd /opt/soulseek-research

# Build archive command
DB_URL="postgresql://soulseek:YOUR_PASSWORD@localhost:5432/soulseek"

# Run archive using Docker
docker run --rm \
  --network=host \
  -v /opt/archives:/archives \
  -e DATABASE_URL="$DB_URL" \
  -e ARCHIVE_PATH=/archives \
  -e DELETE_AFTER_ARCHIVE=true \
  soulseek-research:latest \
  python /app/scripts/archive.py

# Log the result
echo "$(date): Weekly archive completed" >> /var/log/soulseek-archive.log
ARCHIVE_SCRIPT

chmod +x /usr/local/bin/weekly-archive.sh

# Update cron (runs Sundays at 2 AM)
echo "0 2 * * 0 /usr/local/bin/weekly-archive.sh" | crontab -

# Verify cron is set
crontab -l
```

**IMPORTANT**: Replace `YOUR_PASSWORD` in the script above with your actual database password.

---

## 5. Test Archive Script

Test the archival process manually (without deleting data):

```bash
# Test run without deletion
docker run --rm \
  --network=host \
  -v /opt/archives:/archives \
  -e DATABASE_URL="postgresql://soulseek:YOUR_PASSWORD@localhost:5432/soulseek" \
  -e ARCHIVE_PATH=/archives \
  -e DELETE_AFTER_ARCHIVE=false \
  soulseek-research:latest \
  python /app/scripts/archive.py
```

Expected output:
- "Processing 0 archive(s)..." (if no archives yet)
- "Found X month(s) to archive: [...]" (if old data exists)
- Creates `.csv.gz` files in `/opt/archives/`

Check the results:
```bash
# List created archives
ls -lh /opt/archives/

# Check database archives table
docker exec -it $(docker ps -q -f name=database) \
  psql -U soulseek -d soulseek -c "SELECT * FROM archives;"
```

---

## 6. Test GitHub Actions

Trigger the workflow manually to test everything end-to-end:

1. Go to: https://github.com/YOUR_USERNAME/soulseek-research/actions
2. Select "Update Statistics Dashboard"
3. Click "Run workflow" → "Run workflow"
4. Watch the logs

Expected behavior:
- Downloads archives via SSH/SCP
- Generates statistics with cumulative totals
- Deploys to GitHub Pages

Check the output:
- Visit your GitHub Pages site
- Look for "All-Time Statistics" section
- Verify range sliders work on time-series charts

---

## 7. Verify Everything Works

Final checks:

```bash
# On database server - check cron logs (after Sunday 2 AM)
tail -f /var/log/soulseek-archive.log

# Check archives were created
ls -lh /opt/archives/

# Check database
docker exec -it $(docker ps -q -f name=database) \
  psql -U soulseek -d soulseek -c "
    SELECT COUNT(*) FROM searches;
    SELECT * FROM archives ORDER BY month;
  "
```

---

## Rollback (if needed)

If something goes wrong:

```bash
# Restore previous cron
crontab /tmp/cron-backup-YYYYMMDD.txt

# Restore archives table (if data was deleted)
# You'll need to restore from database backups
```

---

## Notes

- Archives are cumulative - once created, they're used for all future dashboard generations
- The weekly cron runs on Sundays to archive the previous week's complete months
- GitHub Actions runs daily and will always include historical data
- First run may take longer as it processes all historical data
- Archive files are stored at `/opt/archives/` on database server
