#!/usr/bin/env python3
"""
Monthly archival script for Soulseek research data.
Exports old months to Parquet files and deletes from database.
"""

import json
import os
from datetime import datetime
from typing import List, Tuple

import psycopg2
import pandas as pd


def get_db_connection():
    """Get database connection from environment"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    # Parse postgresql:// URL to connection params
    # Format: postgresql://user:pass@host:port/dbname
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
        dbname=host_db[1]
    )


def ensure_user_query_pairs_table(conn):
    """Create the persistent user-query co-occurrence table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_query_pairs (
            username TEXT NOT NULL,
            query_normalized TEXT NOT NULL,
            last_seen DATE,
            PRIMARY KEY (username, query_normalized)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_uqp_query
        ON user_query_pairs (query_normalized)
    """)
    conn.commit()
    cursor.close()


def populate_user_query_pairs(conn, month: str = None):
    """Insert distinct user-query pairs from searches into user_query_pairs.

    Args:
        conn: Database connection
        month: Optional 'YYYY-MM' to scope to a single month. If None, scans all searches.
    """
    cursor = conn.cursor()
    if month:
        cursor.execute("""
            INSERT INTO user_query_pairs (username, query_normalized, last_seen)
            SELECT DISTINCT username, LOWER(TRIM(query)), CURRENT_DATE
            FROM searches
            WHERE TO_CHAR(timestamp, 'YYYY-MM') = %s
            ON CONFLICT (username, query_normalized)
            DO UPDATE SET last_seen = EXCLUDED.last_seen
        """, (month,))
    else:
        cursor.execute("""
            INSERT INTO user_query_pairs (username, query_normalized, last_seen)
            SELECT DISTINCT username, LOWER(TRIM(query)), CURRENT_DATE
            FROM searches
            ON CONFLICT (username, query_normalized)
            DO UPDATE SET last_seen = EXCLUDED.last_seen
        """)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    return inserted


def get_months_to_archive(conn) -> List[str]:
    """
    Find months that are complete and ready to archive.
    Returns list of 'YYYY-MM' strings.

    A month is ready to archive if:
    - The month has ended (it's before the current month)
    - It's not already archived
    """
    cursor = conn.cursor()
    current_month = datetime.now().strftime('%Y-%m')

    cursor.execute("""
        WITH archived_months AS (
            SELECT month FROM archives WHERE deleted = TRUE
        )
        SELECT TO_CHAR(timestamp, 'YYYY-MM') as month
        FROM searches
        WHERE TO_CHAR(timestamp, 'YYYY-MM') NOT IN (SELECT month FROM archived_months)
        GROUP BY TO_CHAR(timestamp, 'YYYY-MM')
        HAVING TO_CHAR(timestamp, 'YYYY-MM') < %s
        ORDER BY month
    """, (current_month,))

    return [row[0] for row in cursor.fetchall()]


def update_cumulative_stats(conn, month: str):
    """
    Update cumulative stats with a month's additive metrics.
    Must be called BEFORE deleting the month's data from searches table.

    Only stores additive metrics (total_searches, client_totals, timestamps).
    Distinct counts (users, queries, pairs) are computed correctly by
    refresh_period_stats.py from archived Parquet + live MV data.
    """
    cursor = conn.cursor()

    # Compute additive stats for this month
    cursor.execute("""
        SELECT COUNT(*) as searches, MIN(timestamp), MAX(timestamp)
        FROM searches
        WHERE TO_CHAR(timestamp, 'YYYY-MM') = %s
    """, (month,))
    searches, first_search, last_search = cursor.fetchone()

    if searches == 0:
        print(f"  No data found for month {month}, skipping cumulative update")
        return

    # Get per-client counts for this month
    cursor.execute("""
        SELECT client_id, COUNT(*) as count
        FROM searches
        WHERE TO_CHAR(timestamp, 'YYYY-MM') = %s
        GROUP BY client_id
    """, (month,))
    month_client_totals = {row[0]: row[1] for row in cursor.fetchall()}

    # Get current cumulative stats
    cursor.execute("""
        SELECT total_searches, first_search, last_search, client_totals, last_archive_month
        FROM stats_cumulative
        WHERE id = 1
    """)
    current = cursor.fetchone()

    if current is None:
        cursor.execute("""
            INSERT INTO stats_cumulative (id, total_searches, total_users, total_queries,
                total_search_pairs, first_search, last_search, client_totals, last_archive_month)
            VALUES (1, 0, 0, 0, 0, NULL, NULL, '{}', NULL)
        """)
        conn.commit()
        current = (0, None, None, {}, None)

    curr_searches, curr_first, curr_last, curr_clients, curr_last_month = current

    if isinstance(curr_clients, str):
        curr_clients = json.loads(curr_clients) if curr_clients else {}

    # Merge client totals (additive, correct)
    new_client_totals = curr_clients.copy() if curr_clients else {}
    for client_id, count in month_client_totals.items():
        new_client_totals[client_id] = new_client_totals.get(client_id, 0) + count

    new_searches = curr_searches + searches
    new_first = first_search if curr_first is None else min(curr_first, first_search)
    new_last = last_search if curr_last is None else max(curr_last, last_search)

    cursor.execute("""
        UPDATE stats_cumulative SET
            total_searches = %s,
            first_search = %s,
            last_search = %s,
            client_totals = %s,
            last_archive_month = %s,
            updated_at = NOW()
        WHERE id = 1
    """, (new_searches, new_first, new_last, json.dumps(new_client_totals), month))

    conn.commit()
    print(f"  Updated cumulative stats: +{searches:,} searches")


def export_daily_tuples_to_parquet(conn, month: str, archive_path: str) -> Tuple[str, int]:
    """Export daily search tuples for a month to Parquet.

    Reads from mv_daily_search_tuples via server-side cursor in chunks
    to avoid OOM on 8 GB servers.
    """
    filepath = os.path.join(archive_path, f"daily_tuples_{month}.parquet")

    cursor = conn.cursor(name="export_tuples_cursor")
    cursor.itersize = 500_000
    cursor.execute(
        "SELECT date, username, query_normalized, search_count "
        "FROM mv_daily_search_tuples WHERE TO_CHAR(date, 'YYYY-MM') = %s",
        (month,),
    )

    writer = None
    record_count = 0
    try:
        while True:
            rows = cursor.fetchmany(cursor.itersize)
            if not rows:
                break
            df = pd.DataFrame(rows, columns=['date', 'username', 'query_normalized', 'search_count'])
            if writer is None:
                import pyarrow as pa
                import pyarrow.parquet as pq
                table = pa.Table.from_pandas(df)
                writer = pq.ParquetWriter(filepath, table.schema, compression='snappy')
                writer.write_table(table)
            else:
                writer.write_table(pa.Table.from_pandas(df))
            record_count += len(rows)
            del rows, df
    finally:
        if writer is not None:
            writer.close()
        cursor.close()

    file_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
    print(f"  Exported {record_count:,} daily tuples to {filepath} ({file_size:,} bytes)")
    return filepath, record_count


def archive_daily_client_stats(conn, month: str):
    """Copy daily client stats into permanent table before deletion.

    Preserves mv_daily_stats rows so daily charts survive archival.
    """
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO daily_client_stats (client_id, date, search_count, unique_users)
        SELECT client_id, date, search_count, unique_users
        FROM mv_daily_stats
        WHERE TO_CHAR(date, 'YYYY-MM') = %s
        ON CONFLICT (client_id, date) DO NOTHING
    """, (month,))
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    print(f"  Archived {inserted} daily client stats rows")


def export_month_to_parquet(conn, month: str, archive_path: str) -> Tuple[str, int, int]:
    """Export a month's raw search data to Parquet via chunked server-side cursor.

    Streams data in 500k-row chunks to avoid OOM on 8 GB servers.
    Returns (file_path, record_count, file_size).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    filepath = os.path.join(archive_path, f"searches_{month}.parquet")

    print(f"  Streaming month data to Parquet...")

    cursor = conn.cursor(name="export_month_cursor")
    cursor.itersize = 500_000
    cursor.execute(
        "SELECT client_id, timestamp, username, query "
        "FROM searches WHERE TO_CHAR(timestamp, 'YYYY-MM') = %s ORDER BY timestamp",
        (month,),
    )

    writer = None
    record_count = 0
    try:
        while True:
            rows = cursor.fetchmany(cursor.itersize)
            if not rows:
                break
            df = pd.DataFrame(rows, columns=['client_id', 'timestamp', 'username', 'query'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            table = pa.Table.from_pandas(df)
            if writer is None:
                writer = pq.ParquetWriter(filepath, table.schema, compression='snappy')
            writer.write_table(table)
            record_count += len(rows)
            del rows, df, table
            print(f"    {record_count:,} rows exported...")
    finally:
        if writer is not None:
            writer.close()
        cursor.close()

    file_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
    print(f"  Parquet file: {file_size:,} bytes, {record_count:,} records")

    return filepath, record_count, file_size


def record_archive(conn, month: str, file_path: str, record_count: int, file_size: int):
    """Insert archive record into archives table"""
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO archives (month, file_path, record_count, file_size, archived_at, deleted)
        VALUES (%s, %s, %s, %s, NOW(), FALSE)
    """, (month, file_path, record_count, file_size))

    conn.commit()


def delete_archived_data(conn, month: str) -> int:
    """Delete data for archived month from searches table.

    Drops mv_daily_search_tuples first to free disk space for the DELETE
    WAL, then recreates it after VACUUM reclaims space.
    """
    cursor = conn.cursor()

    # Drop MV to free disk space (the DELETE + WAL needs headroom)
    print(f"  Dropping mv_daily_search_tuples to free disk space...")
    cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_daily_search_tuples")
    conn.commit()

    # Delete archived rows
    cursor.execute("""
        DELETE FROM searches
        WHERE TO_CHAR(timestamp, 'YYYY-MM') = %s
    """, (month,))
    deleted = cursor.rowcount
    conn.commit()

    # VACUUM to reclaim space before recreating MV
    print(f"  Running VACUUM FULL on searches...")
    old_isolation = conn.isolation_level
    conn.set_isolation_level(0)  # autocommit for VACUUM
    cursor.execute("VACUUM FULL searches")
    conn.set_isolation_level(old_isolation)

    # Recreate MV + index
    print(f"  Recreating mv_daily_search_tuples...")
    cursor.execute("""
        CREATE MATERIALIZED VIEW mv_daily_search_tuples AS
        SELECT date(timestamp) AS date, username,
               lower(trim(query)) AS query_normalized,
               count(*) AS search_count
        FROM searches
        GROUP BY date(timestamp), username, lower(trim(query))
    """)
    cursor.execute("""
        CREATE UNIQUE INDEX idx_mv_daily_search_tuples_unique
        ON mv_daily_search_tuples (date, username, query_normalized)
    """)
    conn.commit()
    cursor.close()
    print(f"  MV recreated successfully")

    return deleted


def mark_archive_deleted(conn, month: str):
    """Mark archive record as deleted (data removed from DB)"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE archives SET deleted = TRUE
        WHERE month = %s
    """, (month,))
    conn.commit()


def archive_month(conn, month: str, archive_path: str, delete_after: bool = False):
    """Archive a single month's data to Parquet format"""
    print(f"Archiving {month}...")

    # 1. Export raw searches to Parquet
    file_path, record_count, file_size = export_month_to_parquet(conn, month, archive_path)
    print(f"  Exported: {file_path} ({file_size:,} bytes)")

    # 2. Export daily search tuples to Parquet (for stats recomputation)
    export_daily_tuples_to_parquet(conn, month, archive_path)

    # 3. Archive daily client stats to permanent table (for charts)
    archive_daily_client_stats(conn, month)

    # 4. Record in archives table
    record_archive(conn, month, file_path, record_count, file_size)
    print(f"  Recorded in archives table")

    # 5. Optionally delete from database
    if delete_after:
        # 5a. Preserve user-query pairs BEFORE deleting
        populate_user_query_pairs(conn, month)

        # 5b. Update cumulative stats (additive metrics only)
        update_cumulative_stats(conn, month)

        # 5c. Delete from database
        deleted = delete_archived_data(conn, month)
        print(f"  Deleted {deleted:,} records from database")

        # 5d. Mark as deleted
        mark_archive_deleted(conn, month)


def main():
    """Main archival process"""
    archive_path = os.environ.get('ARCHIVE_PATH', '/opt/archives')
    delete_after = os.environ.get('DELETE_AFTER_ARCHIVE', 'false').lower() == 'true'

    os.makedirs(archive_path, exist_ok=True)

    conn = get_db_connection()

    try:
        # Ensure persistent user-query pairs table exists
        ensure_user_query_pairs_table(conn)
        # Per-month seeding happens in archive_month() before each deletion

        # Find completed months to archive
        months = get_months_to_archive(conn)

        if not months:
            print("No months ready for archival")
            return

        print(f"Found {len(months)} month(s) to archive: {months}")

        for month in months:
            archive_month(conn, month, archive_path, delete_after)

        print("✅ Archival complete!")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
