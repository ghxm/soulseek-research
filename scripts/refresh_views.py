#!/usr/bin/env python3
"""
Refresh materialized views for Soulseek research statistics.
Called by cron at 4 AM UTC daily, before GitHub Actions dashboard generation.
"""

import os
import sys
from datetime import datetime, timezone

import psycopg2


def get_db_connection():
    """Get database connection from environment"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    # Parse postgresql:// URL to connection params
    if database_url.startswith('postgresql://'):
        database_url = database_url.replace('postgresql://', '')
    elif database_url.startswith('postgresql+asyncpg://'):
        database_url = database_url.replace('postgresql+asyncpg://', '')

    parts = database_url.split('@')
    user_pass = parts[0].split(':')
    host_db = parts[1].split('/')
    host_port = host_db[0].split(':')

    return psycopg2.connect(
        host=host_port[0],
        port=int(host_port[1]) if len(host_port) > 1 else 5432,
        user=user_pass[0],
        password=user_pass[1] if len(user_pass) > 1 else '',
        dbname=host_db[1],
        connect_timeout=30,
        options='-c statement_timeout=1800000'  # 30 min timeout for refresh
    )


def refresh_views(conn):
    """Refresh all materialized views"""
    views = [
        ('mv_daily_stats', True),        # CONCURRENTLY supported (has unique index)
        ('mv_top_queries', True),        # CONCURRENTLY supported
        ('mv_query_length_dist', True),  # CONCURRENTLY supported
        ('mv_summary_stats', False),     # No unique index, regular refresh
    ]

    cursor = conn.cursor()

    for view_name, concurrent in views:
        start = datetime.now(timezone.utc)
        print(f"Refreshing {view_name}...")

        if concurrent:
            cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
        else:
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")

        conn.commit()
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"  Refreshed {view_name} in {elapsed:.1f}s")

    cursor.close()


def main():
    """Main execution"""
    print(f"Starting view refresh at {datetime.now(timezone.utc).isoformat()}")

    try:
        conn = get_db_connection()
        refresh_views(conn)
        conn.close()
        print("View refresh completed successfully")
        return 0
    except Exception as e:
        print(f"ERROR: View refresh failed: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
