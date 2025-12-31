# Soulseek Research Package - Developer Context

## Overview

This package collects search data from the Soulseek P2P network by monitoring search requests received by clients. It stores this data for research into network usage patterns and user behavior.

## Core Concept

**What it does**: Runs Soulseek clients that listen for search requests from other users on the network, then logs these searches (username, query, timestamp) to a database.

**Why distributed**: Different geographic regions see different search traffic due to how Soulseek's distributed network routes searches.

## Architecture

### Simple 3-File Structure
```
src/soulseek_research/
├── client.py      # Everything - client, database models, archival
├── cli.py         # Command line interface  
├── __init__.py    # Package exports
```

### Database Schema
```sql
-- Core search data (no indexes for max write speed)
searches:
  id, client_id, timestamp, username, query

-- Archive tracking  
archives:
  id, month, file_path, record_count, file_size, archived_at, deleted
```

### Key Design Decisions

**KISS Principle Applied:**
- Single client class handles everything
- Database models embedded in client.py
- No separate adapters, managers, or complex abstractions
- Direct SQLAlchemy usage, no ORM layers

**Write-Optimized:**
- No indexes on search table for maximum insert throughput
- Batch writes (500 records every 10 seconds)
- Simple schema optimized for append-only operations

**Scale Strategy:**
- Keep recent months in active database
- Archive older months to compressed files (90% storage savings)
- Multiple clients can write to same database concurrently

## Implementation Details

### Event Flow
1. aioslsk library receives search request from Soulseek network
2. Fires `SearchRequestReceivedEvent` with username/query/result_count
3. Our `_on_search_received` handler creates `SearchRecord`
4. Record added to in-memory queue for batch processing
5. Background worker flushes queue to database every 10 seconds

### External Dependencies
- **aioslsk**: Handles Soulseek protocol, fires search events
- **SQLAlchemy**: Database ORM with async support
- **Click**: CLI framework for clean command interface

### Archival System
Built-in monthly archival to handle 2B+ searches/year:
- Exports month data to compressed CSV using PostgreSQL COPY
- Tracks archive metadata in database
- Deletes archived data from main table to keep it fast
- 10:1 compression ratio typical

## Deployment

### Local Development
```bash
pip install -e .
soulseek-research start --username user --password pass --database-url sqlite:///data.db
```

### Production Docker
- `database.yml` - PostgreSQL server
- `client.yml` - Research client (uses Dockerfile)
- Deploy database once, deploy clients on multiple servers

### Multi-Region Setup
1. Deploy database.yml on central server (e.g., Germany)
2. Deploy client.yml on servers in different regions (US, EU, Asia)
3. Each client connects to central database with different credentials

## Research Applications

### Data Collection Capabilities
- **Search patterns**: What users search for by geographic region
- **Temporal analysis**: Search activity by time/day/season
- **Content trends**: Popular searches, emerging content
- **Network behavior**: Understanding P2P network usage patterns

### Scale Targets
- **Per client**: 100+ searches/second
- **System capacity**: 2+ billion searches/year
- **Storage efficiency**: 90% reduction through archival

### Privacy Considerations
- Username encryption planned for database storage
- Research focus on aggregate patterns, not individual users
- Archival system enables data retention policies

## Development Philosophy

**Completed Refactoring:**
- Eliminated complex distributed client architectures
- Removed unnecessary database abstraction layers  
- Consolidated 15+ files into 3 essential files
- Applied KISS principle throughout

**Result**: Simple, maintainable package that does one thing well - collect Soulseek search data efficiently at scale.

## Future Considerations

**Planned Improvements:**
- Automatic username encryption at write time
- Enhanced CLI commands for data analysis
- Optional real-time analytics capabilities

**Architecture Philosophy:**
- Keep core simple, add features as separate modules
- Prioritize write performance over query optimization
- Maintain clear separation between data collection and analysis