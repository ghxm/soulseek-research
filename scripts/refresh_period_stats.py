#!/usr/bin/env python3
"""
Refresh period-specific statistics tables (weeks and months).
Called by cron at 4:30 AM UTC daily, after materialized view refresh.

Precomputes top queries and query length distribution for each period,
storing results in dedicated tables for fast dashboard generation.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

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
        options='-c statement_timeout=3600000'  # 60 min timeout for period processing
    )


def get_date_range(conn) -> Tuple[datetime, datetime]:
    """Get the min/max dates from searches table"""
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM searches")
    min_date, max_date = cursor.fetchone()
    cursor.close()

    if not min_date or not max_date:
        raise ValueError("No data in searches table")

    return min_date, max_date


def generate_periods(min_date: datetime, max_date: datetime) -> List[Tuple[str, str, datetime, datetime]]:
    """Generate all week and month periods between min and max dates.

    Returns list of (period_type, period_id, start_date, end_date) tuples.
    """
    periods = []

    # Generate weeks
    current = min_date
    while current <= max_date:
        iso_year, iso_week, _ = current.isocalendar()
        week_id = f"{iso_year}-W{iso_week:02d}"
        week_start = datetime.fromisocalendar(iso_year, iso_week, 1).replace(tzinfo=timezone.utc)
        week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        periods.append(('week', week_id, week_start, week_end))
        current = week_end + timedelta(seconds=1)

    # Generate months
    current = min_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= max_date:
        month_id = current.strftime('%Y-%m')
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        month_end = next_month - timedelta(seconds=1)
        periods.append(('month', month_id, current, month_end))
        current = next_month

    return periods


def compute_period_top_queries(conn, period_type: str, period_id: str,
                                start_date: datetime, end_date: datetime) -> int:
    """Compute top queries for a period and store in period_top_queries table.

    Returns number of queries inserted.
    """
    cursor = conn.cursor()

    # Delete existing data for this period
    cursor.execute("""
        DELETE FROM period_top_queries
        WHERE period_type = %s AND period_id = %s
    """, (period_type, period_id))

    # Compute top queries with ranking
    cursor.execute("""
        WITH ranked_queries AS (
            SELECT
                LOWER(TRIM(query)) as query_normalized,
                COUNT(DISTINCT username) as unique_users,
                COUNT(*) as total_searches,
                ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT username) DESC, COUNT(*) DESC) as rank
            FROM searches
            WHERE timestamp >= %s AND timestamp <= %s
            GROUP BY LOWER(TRIM(query))
            HAVING COUNT(DISTINCT username) >= 5
        )
        INSERT INTO period_top_queries (period_type, period_id, query_normalized, unique_users, total_searches, rank)
        SELECT %s, %s, query_normalized, unique_users, total_searches, rank
        FROM ranked_queries
    """, (start_date, end_date, period_type, period_id))

    inserted = cursor.rowcount
    conn.commit()
    cursor.close()

    return inserted


def compute_period_query_length_dist(conn, period_type: str, period_id: str,
                                      start_date: datetime, end_date: datetime) -> int:
    """Compute query length distribution for a period and store in period_query_length_dist table.

    Returns number of rows inserted.
    """
    cursor = conn.cursor()

    # Delete existing data for this period
    cursor.execute("""
        DELETE FROM period_query_length_dist
        WHERE period_type = %s AND period_id = %s
    """, (period_type, period_id))

    # Compute query length distribution (word count)
    cursor.execute("""
        INSERT INTO period_query_length_dist (period_type, period_id, query_length, unique_query_count)
        SELECT
            %s,
            %s,
            LENGTH(query) - LENGTH(REPLACE(query, ' ', '')) + 1 as query_length,
            COUNT(DISTINCT LOWER(TRIM(query))) as unique_query_count
        FROM searches
        WHERE timestamp >= %s AND timestamp <= %s
          AND LENGTH(query) - LENGTH(REPLACE(query, ' ', '')) + 1 <= 100
        GROUP BY query_length
        ORDER BY query_length
    """, (period_type, period_id, start_date, end_date))

    inserted = cursor.rowcount
    conn.commit()
    cursor.close()

    return inserted


def refresh_period_stats(conn):
    """Refresh all period statistics"""
    print("Getting date range from searches table...")
    min_date, max_date = get_date_range(conn)
    print(f"  Data range: {min_date.isoformat()} to {max_date.isoformat()}")

    print("Generating period definitions...")
    periods = generate_periods(min_date, max_date)
    print(f"  Found {len(periods)} periods to process")

    weeks = [p for p in periods if p[0] == 'week']
    months = [p for p in periods if p[0] == 'month']
    print(f"    - {len(weeks)} weeks")
    print(f"    - {len(months)} months")

    total_queries = 0
    total_dists = 0

    for i, (period_type, period_id, start_date, end_date) in enumerate(periods, 1):
        label = f"{period_type} {period_id}"
        print(f"\n[{i}/{len(periods)}] Processing {label}...")

        # Compute top queries
        start = datetime.now(timezone.utc)
        queries_inserted = compute_period_top_queries(conn, period_type, period_id, start_date, end_date)
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"  Top queries: {queries_inserted} inserted in {elapsed:.1f}s")
        total_queries += queries_inserted

        # Compute query length distribution
        start = datetime.now(timezone.utc)
        dists_inserted = compute_period_query_length_dist(conn, period_type, period_id, start_date, end_date)
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"  Query length dist: {dists_inserted} rows in {elapsed:.1f}s")
        total_dists += dists_inserted

    print(f"\n✓ Processed {len(periods)} periods successfully")
    print(f"  - {total_queries} top query entries")
    print(f"  - {total_dists} query length distribution rows")


def main():
    """Main execution"""
    print(f"Starting period stats refresh at {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    try:
        conn = get_db_connection()
        refresh_period_stats(conn)
        conn.close()

        print("=" * 60)
        print("Period stats refresh completed successfully")
        return 0

    except Exception as e:
        print(f"\nERROR: Period stats refresh failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
