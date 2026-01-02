# Soulseek Research

Monitors Soulseek P2P network search requests for research purposes. Collects search patterns from distributed geographic locations and stores anonymized data for analysis.

## Deployment

**Prerequisites:**
- Hetzner Cloud account with SSH key named "soulseek-research"
- Multiple Soulseek accounts for geographic distribution

```bash
# Configure credentials
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your API tokens and Soulseek credentials

# Deploy infrastructure
make deploy

# Monitor data collection
make monitor

# Destroy infrastructure
make destroy
```

## Architecture

**Default Infrastructure:**
- 1 database server (Germany/Nuremberg, Hetzner Cloud cx33) + Germany client
- 2 additional client servers (US/Ashburn cpx11, Singapore cpx11, Hetzner Cloud)
- Total: 3 research clients across 3 geographic regions

**Components:**
- PostgreSQL database (single server)
- Research clients (distributed across regions)
- Automatic monthly archival system

**Data flow:**
1. Clients connect to Soulseek network in different geographic regions
2. Monitor incoming search requests from other users
3. Log search metadata (client_id, timestamp, encrypted_username, query) to central database
4. Archive monthly data for long-term storage

## Database Schema

```sql
searches: id, client_id, timestamp, username, query
archives: id, month, file_path, record_count, file_size, archived_at, deleted
```

## Local Development

```bash
pip install -e .
soulseek-research start --username user --password pass --database-url sqlite:///data.db
```

## Statistics Dashboard

View live statistics and visualizations of collected data:
- Daily search volume per client
- Top searched queries (deduplicated)
- Word embedding cluster visualization
- Most active users (anonymized)
- Geographic distribution

**Setup**: See [docs/GITHUB_PAGES_SETUP.md](docs/GITHUB_PAGES_SETUP.md) for instructions.

The dashboard updates automatically daily at 3 AM UTC via GitHub Actions.

## Requirements

- Python 3.11+
- Docker
- Soulseek account credentials
- PostgreSQL database