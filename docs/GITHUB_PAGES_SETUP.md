# GitHub Pages Dashboard Setup

This document explains how to set up the automated statistics dashboard for the Soulseek Research project.

## Overview

The dashboard automatically updates daily at 3 AM UTC via GitHub Actions and publishes statistics to GitHub Pages, including:

- Daily search volume per client (last 7 days)
- Top searched queries (deduplicated)
- Word embedding cluster visualization
- Most active users (anonymized)
- Geographic distribution of searches

## Setup Instructions

### 1. Configure GitHub Secrets

Add the following secrets to your GitHub repository:

**Settings → Secrets and variables → Actions → New repository secret**

#### Required Secrets:

1. **DATABASE_URL**
   - Format: `postgresql://username:password@host:port/database`
   - Example: `postgresql://soulseek:mypassword@123.45.67.89:5432/soulseek`
   - This should point to your production PostgreSQL database

2. **ENCRYPTION_KEY** (optional, if you want to verify username hashing)
   - The same encryption key used by your research clients
   - Example: `research_encryption_2025`

### 2. Enable GitHub Pages

1. Go to **Settings → Pages**
2. Under "Source", select **Deploy from a branch**
3. Select branch: **gh-pages**
4. Select folder: **/ (root)**
5. Click **Save**

### 3. Run the Workflow

The workflow runs automatically daily at 3 AM UTC, but you can also trigger it manually:

1. Go to **Actions** tab
2. Select **Update Statistics Dashboard**
3. Click **Run workflow**
4. Select branch: **main**
5. Click **Run workflow**

### 4. Access the Dashboard

After the first successful run, your dashboard will be available at:

```
https://[your-username].github.io/[repo-name]/
```

For example: `https://maxhaag.github.io/soulseek-research/`

## Database Requirements

The workflow requires read access to your PostgreSQL database with the following tables:

- `searches` table with columns: `id`, `client_id`, `timestamp`, `username`, `query`

### Firewall Configuration

Ensure your database server allows connections from GitHub Actions runners. GitHub Actions uses dynamic IP addresses, so you may need to:

1. **Option A**: Allow connections from GitHub's IP ranges (see [GitHub's meta API](https://api.github.com/meta))
2. **Option B**: Use a database proxy/tunnel service
3. **Option C**: Create a read-only replica accessible from the internet

### Read-Only Database User (Recommended)

For security, create a read-only database user for the dashboard:

```sql
-- Create read-only user
CREATE USER dashboard_reader WITH PASSWORD 'secure_password';

-- Grant connect permission
GRANT CONNECT ON DATABASE soulseek TO dashboard_reader;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO dashboard_reader;

-- Grant select on searches table only
GRANT SELECT ON searches TO dashboard_reader;

-- Update DATABASE_URL secret to use this user
-- postgresql://dashboard_reader:secure_password@host:port/soulseek
```

## Troubleshooting

### Workflow Fails with "Connection Refused"

- Check that your database server's firewall allows GitHub Actions IP addresses
- Verify the DATABASE_URL format is correct
- Test the connection string locally first

### No Data Showing

- Verify the workflow ran successfully (check Actions tab)
- Check that your database has data in the `searches` table
- Look at the workflow logs for specific errors

### Page Not Found (404)

- Ensure GitHub Pages is enabled and set to deploy from `gh-pages` branch
- Wait a few minutes after the first workflow run for GitHub to build the site
- Check that the workflow completed successfully

## Manual Testing Locally

You can test the statistics generation locally before deploying:

```bash
# Install dependencies
pip install psycopg2-binary pandas plotly scikit-learn numpy

# Set environment variables
export DATABASE_URL="postgresql://user:pass@host:port/soulseek"
export ENCRYPTION_KEY="your_key"

# Run the script
python scripts/generate_stats.py

# Open the generated HTML
open docs/index.html
```

## Customization

### Change Update Frequency

Edit `.github/workflows/update-stats.yml` and modify the cron schedule:

```yaml
schedule:
  - cron: '0 3 * * *'  # Daily at 3 AM UTC
  # Examples:
  # - cron: '0 */6 * * *'  # Every 6 hours
  # - cron: '0 0 * * 0'    # Weekly on Sunday
```

### Modify Visualizations

Edit `scripts/generate_stats.py` to customize:

- Number of days shown in charts
- Number of top queries/users displayed
- Clustering parameters
- Chart styles and colors

### Add Custom Analytics

The script uses Plotly for visualizations. You can add new charts by:

1. Adding a new query function (e.g., `query_custom_stat`)
2. Creating a chart function (e.g., `create_custom_chart`)
3. Adding the chart to the `figures` dictionary in `main()`
4. Including it in the HTML template in `generate_html()`

## Privacy and Security

- **Usernames are hashed**: All usernames in the database are SHA-256 hashed and cannot be reversed
- **Read-only access**: The dashboard only needs SELECT permissions
- **Secrets management**: Database credentials are stored as GitHub Secrets and never exposed
- **No raw data export**: The dashboard shows aggregated statistics only

## License

This dashboard is part of the Soulseek Research project and follows the same license as the main project.
