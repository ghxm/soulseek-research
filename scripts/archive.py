#!/usr/bin/env python3
"""
Monthly archival script for Soulseek research data.
Exports old months to Parquet files and deletes from database.
"""

import os
from datetime import datetime, timedelta
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


def get_months_to_archive(conn, min_age_days: int = 7) -> List[str]:
    """
    Find months that are complete and old enough to archive.
    Returns list of 'YYYY-MM' strings.

    A month is ready to archive if:
    - All its data is older than min_age_days
    - It's not already archived
    """
    cursor = conn.cursor()
    cutoff = datetime.now() - timedelta(days=min_age_days)

    cursor.execute("""
        WITH archived_months AS (
            SELECT month FROM archives WHERE deleted = TRUE
        )
        SELECT DISTINCT TO_CHAR(timestamp, 'YYYY-MM') as month
        FROM searches
        WHERE timestamp < %s
          AND TO_CHAR(timestamp, 'YYYY-MM') NOT IN (SELECT month FROM archived_months)
        GROUP BY TO_CHAR(timestamp, 'YYYY-MM')
        HAVING MAX(timestamp) < %s
        ORDER BY month
    """, (cutoff, cutoff))

    return [row[0] for row in cursor.fetchall()]


def export_month_to_parquet(conn, month: str, archive_path: str) -> Tuple[str, int, int]:
    """
    Export a month's data to Parquet format (columnar, compressed).
    Returns (file_path, record_count, file_size).

    Parquet benefits:
    - 10x smaller than CSV.gz (better compression + columnar format)
    - Much faster to read (DuckDB optimized)
    - Native timestamp support (no parsing needed)
    """
    filename = f"searches_{month}.parquet"
    filepath = os.path.join(archive_path, filename)

    print(f"  Loading month data into pandas...")

    # Load data for this month using pandas
    query = f"""
        SELECT client_id, timestamp, username, query
        FROM searches
        WHERE TO_CHAR(timestamp, 'YYYY-MM') = '{month}'
        ORDER BY timestamp
    """

    df = pd.read_sql_query(query, conn, parse_dates=['timestamp'])
    record_count = len(df)

    print(f"  Exporting {record_count:,} records to Parquet...")

    # Write to Parquet with good compression
    # Snappy compression: fast compression/decompression, good ratio
    df.to_parquet(
        filepath,
        compression='snappy',
        index=False,
        engine='pyarrow'
    )

    file_size = os.path.getsize(filepath)
    compression_ratio = 100 * (1 - file_size / (record_count * 200)) if record_count > 0 else 0
    print(f"  Parquet file: {file_size:,} bytes (~{compression_ratio:.1f}% compressed)")

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
    """Delete data for archived month from searches table"""
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM searches
        WHERE TO_CHAR(timestamp, 'YYYY-MM') = %s
    """, (month,))
    deleted = cursor.rowcount
    conn.commit()
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

    # 1. Export to Parquet (columnar, compressed)
    file_path, record_count, file_size = export_month_to_parquet(conn, month, archive_path)
    print(f"  Exported: {file_path} ({file_size:,} bytes)")

    # 2. Record in archives table
    record_archive(conn, month, file_path, record_count, file_size)
    print(f"  Recorded in archives table")

    # 3. Optionally delete from database
    if delete_after:
        deleted = delete_archived_data(conn, month)
        print(f"  Deleted {deleted:,} records from database")

        # Mark as deleted
        mark_archive_deleted(conn, month)


def main():
    """Main archival process"""
    archive_path = os.environ.get('ARCHIVE_PATH', '/opt/archives')
    delete_after = os.environ.get('DELETE_AFTER_ARCHIVE', 'false').lower() == 'true'

    os.makedirs(archive_path, exist_ok=True)

    conn = get_db_connection()

    try:
        # Find months to archive (complete and 7+ days old)
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
