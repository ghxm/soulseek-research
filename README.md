# Soulseek Research Package

Collect search data from the Soulseek P2P network for research into network usage patterns and user behavior.

## Quick Start

### Local Testing
```bash
# Install
git clone <repository>
cd soulseek-research  
pip install -e .

# Run
soulseek-research start \
  --username your_soulseek_username \
  --password your_soulseek_password \
  --database-url sqlite:///./data.db
```

### Production Deployment

**Prerequisites**
- Hetzner Cloud account with SSH key named "soulseek-research"
- Multiple Soulseek accounts for geographic distribution

**Automated Infrastructure (Recommended)**
```bash
# Configure credentials
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your tokens and credentials

# Deploy everything
make deploy

# Monitor deployment  
make monitor

# Destroy when done
make destroy
```

**Manual Deployment**

**1. Database Server**
```bash
git clone <repository>
cd soulseek-research
echo "DB_PASSWORD=secure_password" > .env
docker-compose -f database.yml up -d
```

**2. Client Servers** (deploy on multiple servers for geographic distribution)
```bash
git clone <repository>
cd soulseek-research
cat > .env << EOF
DB_HOST=your_database_server_ip
DB_PASSWORD=secure_password
SOULSEEK_USERNAME=your_username
SOULSEEK_PASSWORD=your_password
CLIENT_ID=client-region-1
EOF
docker-compose -f client.yml up -d
```

## What It Does

**Monitors Soulseek search requests**: When other users search the network, our client receives these search requests and logs them (username, query, timestamp) to a database.

**Geographic distribution**: Deploy clients in different regions to capture diverse search patterns, as the Soulseek network routes searches based on geographic proximity.

## Architecture

### Simple Structure
- **client.py** - Single client class with embedded database models and archival
- **cli.py** - Command line interface
- 2 Docker files for deployment

### Database Schema
```sql
searches: id, client_id, timestamp, username, query
archives: id, month, file_path, record_count, file_size, archived_at
```

### Performance
- **100+ searches/second** per client with batch processing
- **No indexes** for maximum write throughput  
- **Built-in archival** for 2+ billion searches/year capacity
- **90% storage savings** through monthly compression

## Usage

### CLI Commands
```bash
# Start client
soulseek-research start --username user --password pass --database-url postgresql://...

# Archive old data  
soulseek-research archive --database-url postgresql://...

# Check status
soulseek-research stats --database-url postgresql://...
soulseek-research archive-status --database-url postgresql://...
```

### Python API
```python
from soulseek_research import ResearchClient

client = ResearchClient(
    username="your_username",
    password="your_password", 
    database_url="postgresql://user:pass@host:5432/db"
)

await client.start()
```

## Deployment Options

### Single Server (Testing)
```bash
# Database + client on same machine
docker-compose -f database.yml up -d
docker-compose -f client.yml up -d
```

### Multi-Region Production
```bash
# Database server (Germany)
docker-compose -f database.yml up -d

# Client server 1 (US)
DB_HOST=db_ip CLIENT_ID=us-1 docker-compose -f client.yml up -d

# Client server 2 (EU) 
DB_HOST=db_ip CLIENT_ID=eu-1 docker-compose -f client.yml up -d

# Scale clients
docker-compose -f client.yml up -d --scale client=3
```

## Data Management

### Archival System
Handle large-scale data collection through automatic monthly archival:

```bash
# Archive last month (recommended monthly cron job)
soulseek-research archive --database-url postgresql://...

# Archive specific month
soulseek-research archive --year 2024 --month 3 --database-url postgresql://...
```

**Benefits:**
- Keep only recent months in active database for fast queries
- Compress old data to files (10:1 compression ratio)
- Track archive metadata with checksums
- 90% storage cost reduction

### Research Queries
```sql
-- Search activity by region
SELECT client_id, COUNT(*) as searches
FROM searches 
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY client_id;

-- Popular queries
SELECT query, COUNT(*) as frequency  
FROM searches
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY query
ORDER BY frequency DESC 
LIMIT 20;
```

## Requirements

### Local Development
- Python 3.11+
- Soulseek account

### Production Deployment  
- Docker on each server
- PostgreSQL server (can be containerized)
- Multiple Soulseek accounts (one per client)
- Servers in different geographic regions (recommended)

## Project Structure
```
soulseek-research/
├── src/soulseek_research/
│   ├── client.py           # Core client and database models
│   ├── cli.py              # Command line interface
│   └── __init__.py         # Package exports
├── Dockerfile              # Client container image
├── database.yml            # Database server deployment
├── client.yml              # Client server deployment  
└── pyproject.toml          # Dependencies (aioslsk>=1.6.1)
```

## Research Applications

### Data Collection
- **Search patterns** by geographic region and time
- **Content trends** and popularity analysis  
- **User behavior** patterns across different regions
- **Network topology** understanding through search routing

### Privacy
- Focus on aggregate patterns, not individual tracking
- Username encryption planned for storage
- Configurable data retention through archival system

## Development

```bash
# Install development version
git clone <repository>
cd soulseek-research
pip install -e .

# Run tests
pytest

# Format code  
ruff format src/
```

## License

[License information]