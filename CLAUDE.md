# Soulseek Research Package - Developer Context

## Overview

This package collects search data from the Soulseek P2P network by monitoring search requests received by clients. It stores anonymized data for research into network usage patterns and user behavior.

## Core Concept

**What it does**: Runs Soulseek clients that listen for search requests from other users on the network, then logs these searches (encrypted username, query, timestamp) to a centralized database.

**Why distributed**: Different geographic regions see different search traffic due to how Soulseek's distributed network routes searches based on geographic proximity.

## Architecture

### Infrastructure (Production)
```
Default Hetzner Cloud Setup (3 servers total):
├── Database Server (Germany/Nuremberg, cx33)
│   ├── PostgreSQL container (soulseek database)
│   └── Germany research client (client_id: germany)
├── US Client Server (Ashburn, cpx11)
│   └── US research client (client_id: us-west)
└── Singapore Client Server (Singapore, cpx11)  
    └── Asia research client (client_id: singapore)

Total: 3 research clients collecting from 3 geographic regions
```

### Code Structure
```
src/soulseek_research/
├── client.py      # Core ResearchClient class with database models
├── cli.py         # Command line interface
├── __init__.py    # Package exports

scripts/
├── deploy.sh      # Terraform deployment
├── destroy.sh     # Infrastructure teardown
├── monitor.sh     # Live monitoring
└── test-local.sh  # Local testing

Infrastructure:
├── database.yml       # PostgreSQL container
├── client.yml         # Research client container  
├── infrastructure.tf  # Hetzner Cloud deployment
├── setup-database.sh  # Database server initialization
├── setup-client.sh    # Client server initialization
└── Dockerfile         # Research client image
```

### Database Schema
```sql
-- Core search data (no indexes for max write speed)
soulseek.searches:
  id, client_id, timestamp, username, query

-- Archive tracking  
soulseek.archives:
  id, month, file_path, record_count, file_size, archived_at, deleted
```

### Key Design Decisions

**KISS Principle Applied:**
- Single ResearchClient class handles everything
- Database models embedded in client.py
- No separate adapters, managers, or complex abstractions
- Direct SQLAlchemy usage, no ORM layers

**Security & Privacy:**
- Username encryption with Fernet (research_encryption_2025 key)
- Database name changed from "research" to "soulseek" for clarity
- Focus on aggregate patterns, not individual tracking

**Write-Optimized Performance:**
- No indexes on searches table for maximum insert throughput
- Batch writes for high-volume data collection
- Simple schema optimized for append-only operations

**Network Configuration:**
- Soulseek clients expose ports 60000-60001 for incoming connections
- Hetzner Cloud firewall rules automatically configured
- Docker port mapping: `-p 60000:60000 -p 60001:60001`

## Implementation Details

### Event Flow
1. aioslsk library receives search request from Soulseek network
2. Fires `SearchRequestReceivedEvent` with username/query/result_count
3. Our `_on_search_received` handler creates `SearchRecord`
4. Username encrypted with Fernet before database storage
5. Record written to PostgreSQL database

### External Dependencies
- **aioslsk**: Handles Soulseek protocol, fires search events
- **SQLAlchemy**: Database ORM with async support (asyncpg driver)
- **cryptography**: Fernet encryption for usernames
- **Click**: CLI framework for clean command interface

### Deployment Automation
Built-in cloud-init automation via Terraform:
- Automatic Docker installation and setup
- GitHub repository cloning and image building
- Service startup with proper port mapping and restart policies
- Database table creation and initialization
- Firewall configuration for Soulseek ports

## Deployment

### Automated Production (Recommended)
```bash
# Configure credentials
cp terraform.tfvars.example terraform.tfvars
# Edit with Hetzner API token and Soulseek credentials

# Deploy everything
make deploy

# Monitor live data collection
make monitor

# Destroy infrastructure
make destroy
```

### Manual Local Development
```bash
pip install -e .
soulseek-research start \
  --username your_username \
  --password your_password \
  --database-url postgresql+asyncpg://soulseek:pass@host:5432/soulseek \
  --client-id local-test \
  --encryption-key research_encryption_2025
```

## Operational Requirements

### Network Requirements
- Soulseek clients need incoming TCP connections on ports 60000-60001
- Database server needs port 5432 accessible from client servers
- SSH access on port 22 for management

### Account Requirements
- Separate Soulseek accounts for each client region
- Hetzner Cloud account with API token
- SSH key named "soulseek-research" in Hetzner Cloud

### Geographic Distribution
Default configuration provides research coverage across:
- **Germany/Nuremberg** (Database server + germany client): European search patterns
- **US/Ashburn** (us-west client): North American search patterns  
- **Singapore** (singapore client): Asian search patterns

Each client connects to the central database server using external IP addressing.

## Data Management

### Archival System
Built-in monthly archival to handle 2B+ searches/year:
- Exports month data to compressed files using PostgreSQL COPY
- Tracks archive metadata in database
- Automatic cron job on database server (1st of month, 2AM)
- 90% storage savings through compression

### Privacy & Compliance
- Usernames encrypted at write time with consistent key
- Research focus on aggregate patterns, not individual users
- Configurable data retention through archival system
- No file sharing - only search request monitoring

## Maintenance & Updates

### Update Protocol
**IMPORTANT**: Always check if anything changed in the codebase and update this CLAUDE.md accordingly.

When making changes to the system:
1. **Infrastructure changes**: Update infrastructure.tf, setup-*.sh scripts, and this documentation
2. **Code changes**: Update client.py or cli.py and reflect changes here
3. **Database schema changes**: Update both SQL migrations and schema documentation here
4. **Deployment process changes**: Update scripts/ directory, Makefile, and deployment instructions
5. **Network/security changes**: Update firewall rules, port mappings, and security documentation
6. **Geographic/server changes**: Update infrastructure documentation and README.md
7. **Documentation**: Always update this CLAUDE.md as the final step of any change

**Version control**: This CLAUDE.md serves as the definitive source of truth for the system architecture and must be kept in sync with actual implementation.

### Monitoring Commands
```bash
# Check live data flow (all servers)
make monitor

# View specific client logs
ssh root@$(terraform output -raw database_ip) 'docker logs soulseek-germany-client --tail 20'
ssh root@$(terraform output -json client_ips | jq -r '.["us-west"]') 'docker logs soulseek-client --tail 20'
ssh root@$(terraform output -json client_ips | jq -r '.singapore') 'docker logs soulseek-client --tail 20'

# Database queries
ssh root@$(terraform output -raw database_ip) 'docker exec $(docker-compose -f /opt/soulseek-research/database.yml ps -q database) psql -U soulseek -d soulseek'

# Useful SQL queries:
SELECT client_id, COUNT(*) FROM searches GROUP BY client_id;
SELECT COUNT(*) FROM searches WHERE timestamp > NOW() - INTERVAL '1 hour';
SELECT client_id, timestamp, query FROM searches ORDER BY timestamp DESC LIMIT 10;
```

### Common Issues & Solutions
- **No search data**: Check if Soulseek ports 60000-60001 are properly exposed and clients are connected
- **Database connection errors**: Verify database password and external IP connectivity
- **Client restart loops**: Check Soulseek credentials and network connectivity
- **Permission denied**: Ensure SSH key "soulseek-research" exists in Hetzner Cloud

## Development Philosophy

**Completed Refactoring:**
- Eliminated complex distributed architectures
- Removed unnecessary abstraction layers  
- Consolidated from 15+ files to essential components
- Applied KISS principle throughout
- Implemented comprehensive automation

**Current State**: Production-ready system with automated deployment, monitoring, and data collection for long-term research operations.

**Architecture Philosophy:**
- Keep core simple, add features as separate modules
- Prioritize reliability and automation over complex features
- Maintain clear separation between data collection and analysis
- Always update documentation when making changes