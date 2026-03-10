#!/usr/bin/env python3
"""
Refresh period-specific statistics tables (weeks, months, all_time).
Called by cron at 1:00 AM UTC daily, after materialized view refresh.

Two modes:
- SQL mode (no archived Parquet files): computes everything via SQL against
  mv_daily_search_tuples. Used before any archival has happened.
- Polars mode (archived Parquet files exist): loads archived Parquet + live MV
  data into polars for unified aggregation across both sources.
"""

import glob
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import execute_values


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
        options='-c statement_timeout=3600000',  # 60 min timeout for period processing
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def get_min_live_date(conn) -> Optional[date]:
    """Get the minimum date from mv_daily_search_tuples."""
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(date) FROM mv_daily_search_tuples")
    result = cursor.fetchone()
    cursor.close()
    if result and result[0]:
        d = result[0]
        if isinstance(d, datetime):
            return d.date()
        return d
    return None


def get_existing_period_stats(conn) -> Set[Tuple[str, str]]:
    """Get all (period_type, period_id) pairs that already have summary stats."""
    cursor = conn.cursor()
    cursor.execute("SELECT period_type, period_id FROM period_summary_stats")
    result = {(row[0], row[1]) for row in cursor.fetchall()}
    cursor.close()
    return result


def generate_periods(min_date: date, max_date: date) -> List[Tuple[str, str, date, date]]:
    """Generate all week, month, and all_time periods between min and max dates."""
    periods = []

    # Generate weeks
    current = min_date
    while current <= max_date:
        iso_year, iso_week, _ = current.isocalendar()
        week_id = f"{iso_year}-W{iso_week:02d}"
        week_start = date.fromisocalendar(iso_year, iso_week, 1)
        week_end = week_start + timedelta(days=6)
        periods.append(('week', week_id, week_start, week_end))
        current = week_end + timedelta(days=1)

    # Generate months
    current = min_date.replace(day=1)
    while current <= max_date:
        month_id = current.strftime('%Y-%m')
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        month_end = next_month - timedelta(days=1)
        periods.append(('month', month_id, current, month_end))
        current = next_month

    # All-time period
    periods.append(('all_time', 'all_time', min_date, max_date))

    return periods


def ensure_period_summary_stats_table(conn):
    """Create the period_summary_stats table if it doesn't exist, and add new columns."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS period_summary_stats (
            period_type VARCHAR(10) NOT NULL,
            period_id VARCHAR(20) NOT NULL,
            unique_queries INTEGER NOT NULL,
            unique_pairs INTEGER NOT NULL,
            total_searches BIGINT,
            total_users INTEGER,
            first_date DATE,
            last_date DATE,
            PRIMARY KEY (period_type, period_id)
        )
    """)
    conn.commit()

    for col, col_type in [
        ('total_searches', 'BIGINT'),
        ('total_users', 'INTEGER'),
        ('first_date', 'DATE'),
        ('last_date', 'DATE'),
    ]:
        try:
            cursor.execute(f"ALTER TABLE period_summary_stats ADD COLUMN {col} {col_type}")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()

    cursor.close()


def has_archived_parquet(archive_path: str) -> bool:
    """Check if there are any archived daily_tuples Parquet files."""
    return bool(glob.glob(os.path.join(archive_path, "daily_tuples_*.parquet")))


# ---------------------------------------------------------------------------
# SQL mode: all computations run against mv_daily_search_tuples via SQL.
# Used when no archived Parquet files exist (before first archival).
# ---------------------------------------------------------------------------

def _sql_date_range(conn) -> Tuple[date, date]:
    """Get date range from mv_daily_search_tuples."""
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(date), MAX(date) FROM mv_daily_search_tuples")
    row = cursor.fetchone()
    cursor.close()
    if not row or row[0] is None:
        raise ValueError("No data in mv_daily_search_tuples")
    return row[0], row[1]


def _sql_date_filter(period_type: str, start_date: date, end_date: date) -> Tuple[str, list]:
    """Return a WHERE clause fragment and params for date filtering."""
    if period_type == 'all_time':
        return "", []
    return "WHERE date >= %s AND date <= %s", [start_date, end_date]


def _sql_compute_summary_single(cursor, where: str, params: list):
    """Compute summary stats with a single combined query (for smaller periods)."""
    cursor.execute(f"""
        SELECT
            SUM(search_count) AS total_searches,
            COUNT(DISTINCT username) AS total_users,
            COUNT(DISTINCT query_normalized) AS unique_queries,
            COUNT(DISTINCT (username, query_normalized)) AS unique_pairs,
            MIN(date) AS first_date,
            MAX(date) AS last_date
        FROM mv_daily_search_tuples
        {where}
    """, params)
    return cursor.fetchone()


def _sql_compute_summary_split(cursor, where: str, params: list):
    """Compute summary stats with separate queries to reduce temp disk usage.

    Used for all_time (61M+ rows) where 3 concurrent COUNT(DISTINCT ...)
    would exceed available disk space for temp files.
    """
    cursor.execute(f"""
        SELECT SUM(search_count), MIN(date), MAX(date)
        FROM mv_daily_search_tuples {where}
    """, params)
    total_searches, first_dt, last_dt = cursor.fetchone()

    cursor.execute(f"""
        SELECT COUNT(DISTINCT username) FROM mv_daily_search_tuples {where}
    """, params)
    total_users = cursor.fetchone()[0]

    cursor.execute(f"""
        SELECT COUNT(DISTINCT query_normalized) FROM mv_daily_search_tuples {where}
    """, params)
    unique_queries = cursor.fetchone()[0]

    cursor.execute(f"""
        SELECT COUNT(DISTINCT (username, query_normalized)) FROM mv_daily_search_tuples {where}
    """, params)
    unique_pairs = cursor.fetchone()[0]

    return (total_searches, total_users, unique_queries, unique_pairs, first_dt, last_dt)


def sql_compute_all_summary_stats(
    conn, periods: List[Tuple[str, str, date, date]]
) -> Dict[Tuple[str, str], Tuple[int, int, int, int, date, date]]:
    """Compute summary stats for all periods using SQL."""
    results = {}
    cursor = conn.cursor()

    for period_type, period_id, start_date, end_date in periods:
        print(f"  Computing summary for {period_type} {period_id}...")
        start = datetime.now(timezone.utc)

        where, params = _sql_date_filter(period_type, start_date, end_date)

        # Use split queries for all_time to avoid disk-full from concurrent
        # COUNT(DISTINCT) temp files over 61M+ rows
        if period_type == 'all_time':
            row = _sql_compute_summary_split(cursor, where, params)
        else:
            row = _sql_compute_summary_single(cursor, where, params)

        if not row or row[0] is None:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            print(f"    No data for this period ({elapsed:.1f}s)")
            continue

        total_searches, total_users, unique_queries, unique_pairs, first_dt, last_dt = row
        results[(period_type, period_id)] = (
            unique_queries, unique_pairs, int(total_searches), total_users, first_dt, last_dt
        )

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"    {unique_queries} queries, {unique_pairs} pairs, "
              f"{total_searches} searches, {total_users} users ({elapsed:.1f}s)")

    # Bulk upsert
    if results:
        values = [
            (pt, pid, uq, up, ts, tu, fd, ld)
            for (pt, pid), (uq, up, ts, tu, fd, ld) in results.items()
        ]
        execute_values(
            cursor,
            """INSERT INTO period_summary_stats
               (period_type, period_id, unique_queries, unique_pairs,
                total_searches, total_users, first_date, last_date)
               VALUES %s
               ON CONFLICT (period_type, period_id)
               DO UPDATE SET
                   unique_queries = EXCLUDED.unique_queries,
                   unique_pairs = EXCLUDED.unique_pairs,
                   total_searches = EXCLUDED.total_searches,
                   total_users = EXCLUDED.total_users,
                   first_date = EXCLUDED.first_date,
                   last_date = EXCLUDED.last_date""",
            values,
        )
        conn.commit()

    cursor.close()
    return results


def sql_compute_top_queries(
    conn, period_type: str, period_id: str,
    start_date: date, end_date: date
) -> int:
    """Compute top queries for a period using SQL."""
    where, params = _sql_date_filter(period_type, start_date, end_date)

    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT query_normalized,
               COUNT(DISTINCT username) AS unique_users,
               SUM(search_count) AS total_searches
        FROM mv_daily_search_tuples
        {where}
        GROUP BY query_normalized
        HAVING COUNT(DISTINCT username) >= 5
        ORDER BY unique_users DESC, total_searches DESC
    """, params)
    rows = cursor.fetchall()

    # Delete old data
    cursor.execute("""
        DELETE FROM period_top_queries
        WHERE period_type = %s AND period_id = %s
    """, (period_type, period_id))

    if not rows:
        conn.commit()
        cursor.close()
        return 0

    values = [
        (period_type, period_id, row[0], row[1], int(row[2]), rank)
        for rank, row in enumerate(rows, 1)
    ]

    execute_values(
        cursor,
        """INSERT INTO period_top_queries
           (period_type, period_id, query_normalized, unique_users, total_searches, rank)
           VALUES %s""",
        values,
        page_size=5000,
    )

    conn.commit()
    cursor.close()
    return len(values)


def sql_compute_query_length_dist(
    conn, period_type: str, period_id: str,
    start_date: date, end_date: date
) -> int:
    """Compute query length distribution for a period using SQL."""
    where, params = _sql_date_filter(period_type, start_date, end_date)

    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT query_length, COUNT(DISTINCT query_normalized) AS unique_query_count
        FROM (
            SELECT query_normalized,
                   array_length(string_to_array(query_normalized, ' '), 1) AS query_length
            FROM mv_daily_search_tuples
            {where}
            GROUP BY query_normalized
        ) sub
        WHERE query_length <= 100
        GROUP BY query_length
        ORDER BY query_length
    """, params)
    rows = cursor.fetchall()

    # Delete old data
    cursor.execute("""
        DELETE FROM period_query_length_dist
        WHERE period_type = %s AND period_id = %s
    """, (period_type, period_id))

    if not rows:
        conn.commit()
        cursor.close()
        return 0

    values = [
        (period_type, period_id, row[0], row[1])
        for row in rows
    ]

    execute_values(
        cursor,
        """INSERT INTO period_query_length_dist
           (period_type, period_id, query_length, unique_query_count)
           VALUES %s""",
        values,
    )

    conn.commit()
    cursor.close()
    return len(values)


def sql_compute_query_daily_stats(conn, min_live_date: Optional[date]):
    """Compute daily stats for queries with 35+ unique users, using SQL."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT query_normalized FROM period_top_queries
        WHERE period_type = 'all_time' AND unique_users >= 35
    """)
    eligible = [row[0] for row in cursor.fetchall()]
    cursor.close()

    if not eligible:
        print("  No queries with 35+ users found")
        return 0

    print(f"  Found {len(eligible)} eligible queries (35+ users)")

    chunk_size = 500
    total_inserted = 0

    for chunk_start in range(0, len(eligible), chunk_size):
        chunk = eligible[chunk_start:chunk_start + chunk_size]
        chunk_label = f"[{chunk_start + 1}-{min(chunk_start + chunk_size, len(eligible))}/{len(eligible)}]"

        cursor = conn.cursor()

        # Delete old data for live date range
        if min_live_date is not None:
            cursor.execute("""
                DELETE FROM query_daily_stats
                WHERE query_normalized = ANY(%s) AND date >= %s
            """, (chunk, min_live_date))
        else:
            cursor.execute("""
                DELETE FROM query_daily_stats
                WHERE query_normalized = ANY(%s)
            """, (chunk,))

        # Insert from MV
        cursor.execute("""
            INSERT INTO query_daily_stats (query_normalized, date, search_count, unique_users)
            SELECT query_normalized, date,
                   SUM(search_count) AS search_count,
                   COUNT(DISTINCT username) AS unique_users
            FROM mv_daily_search_tuples
            WHERE query_normalized = ANY(%s)
            GROUP BY query_normalized, date
            ORDER BY query_normalized, date
        """, (chunk,))

        inserted = cursor.rowcount
        conn.commit()
        cursor.close()

        total_inserted += inserted
        print(f"  {chunk_label} Inserted {inserted} daily rows")

    return total_inserted


# ---------------------------------------------------------------------------
# Polars mode: loads archived Parquet + live MV for unified aggregation.
# Used after archival when some data only exists in Parquet files.
# ---------------------------------------------------------------------------

def _load_all_tuples_polars(conn, archive_path: str):
    """Load archived Parquet + live MV data into a polars LazyFrame.

    After archival, the live MV is small (~1 month, ~20M rows) so it fits
    in memory. Archived Parquet is streamed from disk via scan_parquet.
    """
    import polars as pl

    schema = {
        "date": pl.Date,
        "username": pl.Utf8,
        "query_normalized": pl.Utf8,
        "search_count": pl.Int64,
    }
    frames = []

    # Scan archived Parquet files (streaming, memory-efficient)
    parquet_files = sorted(glob.glob(os.path.join(archive_path, "daily_tuples_*.parquet")))
    print(f"  Found {len(parquet_files)} archived Parquet files")
    if parquet_files:
        archive_lf = pl.scan_parquet(parquet_files)
        archive_lf = archive_lf.cast(schema)
        frames.append(archive_lf)

    # Load live MV data (should be small after archival)
    print("  Loading live data from mv_daily_search_tuples...")
    cursor = conn.cursor(name="load_tuples_cursor")
    cursor.itersize = 500_000
    cursor.execute("SELECT date, username, query_normalized, search_count FROM mv_daily_search_tuples")

    rows_all = []
    while True:
        rows = cursor.fetchmany(cursor.itersize)
        if not rows:
            break
        rows_all.extend(rows)
    cursor.close()
    print(f"  Loaded {len(rows_all):,} live rows")

    if rows_all:
        live_df = pl.DataFrame(
            {
                "date": [r[0] for r in rows_all],
                "username": [r[1] for r in rows_all],
                "query_normalized": [r[2] for r in rows_all],
                "search_count": [r[3] for r in rows_all],
            },
            schema=schema,
        )
        frames.append(live_df.lazy())
        del rows_all

    if not frames:
        return pl.LazyFrame(schema=schema)

    return pl.concat(frames)


def _polars_compute_summary_split(tuples_lf):
    """Compute all_time summary stats with separate queries to reduce peak memory.

    Running 3 concurrent n_unique() aggregations over ~56M rows OOMs on 8GB.
    Split them so each runs and frees memory before the next.
    """
    import polars as pl

    basics = tuples_lf.select(
        pl.col("search_count").sum().alias("total_searches"),
        pl.col("date").min().alias("first_date"),
        pl.col("date").max().alias("last_date"),
    ).collect()
    total_searches = basics["total_searches"][0]
    first_date = basics["first_date"][0]
    last_date = basics["last_date"][0]
    del basics

    if total_searches is None:
        return None

    print("    total_searches/dates done, computing distinct users...")
    total_users = tuples_lf.select(
        pl.col("username").n_unique()
    ).collect().item()

    print("    distinct users done, computing distinct queries...")
    unique_queries = tuples_lf.select(
        pl.col("query_normalized").n_unique()
    ).collect().item()

    print("    distinct queries done, computing distinct pairs...")
    unique_pairs = tuples_lf.select(
        pl.struct(["username", "query_normalized"]).n_unique()
    ).collect().item()

    return (int(unique_queries), int(unique_pairs), int(total_searches),
            int(total_users), first_date, last_date)


def polars_compute_all_summary_stats(
    conn, tuples_lf, periods: List[Tuple[str, str, date, date]]
) -> Dict[Tuple[str, str], Tuple[int, int, int, int, date, date]]:
    """Compute summary stats for all periods using polars."""
    import polars as pl

    results = {}

    for period_type, period_id, start_date, end_date in periods:
        print(f"  Computing summary for {period_type} {period_id}...")
        start = datetime.now(timezone.utc)

        if period_type == 'all_time':
            # Split into separate queries to avoid OOM on 8GB server
            row = _polars_compute_summary_split(tuples_lf)
            if row is None:
                elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                print(f"    No data for this period ({elapsed:.1f}s)")
                continue
            results[(period_type, period_id)] = row
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            print(f"    {row[0]} queries, {row[1]} pairs, "
                  f"{row[2]} searches, {row[3]} users ({elapsed:.1f}s)")
            continue

        filtered = tuples_lf.filter(
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )

        stats = filtered.select(
            pl.col("search_count").sum().alias("total_searches"),
            pl.col("username").n_unique().alias("total_users"),
            pl.col("query_normalized").n_unique().alias("unique_queries"),
            pl.struct(["username", "query_normalized"]).n_unique().alias("unique_pairs"),
            pl.col("date").min().alias("first_date"),
            pl.col("date").max().alias("last_date"),
        ).collect()

        if stats.height == 0 or stats["total_searches"][0] is None:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            print(f"    No data for this period ({elapsed:.1f}s)")
            continue

        row = stats.row(0)
        results[(period_type, period_id)] = (
            int(row[2]), int(row[3]), int(row[0]), int(row[1]), row[4], row[5]
        )

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"    {row[2]} queries, {row[3]} pairs, "
              f"{row[0]} searches, {row[1]} users ({elapsed:.1f}s)")

    # Bulk upsert
    if results:
        cursor = conn.cursor()
        values = [
            (pt, pid, uq, up, ts, tu, fd, ld)
            for (pt, pid), (uq, up, ts, tu, fd, ld) in results.items()
        ]
        execute_values(
            cursor,
            """INSERT INTO period_summary_stats
               (period_type, period_id, unique_queries, unique_pairs,
                total_searches, total_users, first_date, last_date)
               VALUES %s
               ON CONFLICT (period_type, period_id)
               DO UPDATE SET
                   unique_queries = EXCLUDED.unique_queries,
                   unique_pairs = EXCLUDED.unique_pairs,
                   total_searches = EXCLUDED.total_searches,
                   total_users = EXCLUDED.total_users,
                   first_date = EXCLUDED.first_date,
                   last_date = EXCLUDED.last_date""",
            values,
        )
        conn.commit()
        cursor.close()

    return results


def polars_compute_top_queries(
    conn, tuples_lf, period_type: str, period_id: str,
    start_date: date, end_date: date
) -> int:
    """Compute top queries for a period using polars."""
    import polars as pl

    if period_type == 'all_time':
        filtered = tuples_lf
    else:
        filtered = tuples_lf.filter(
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )

    result_df = (
        filtered
        .group_by("query_normalized")
        .agg(
            pl.col("username").n_unique().alias("unique_users"),
            pl.col("search_count").sum().alias("total_searches"),
        )
        .filter(pl.col("unique_users") >= 5)
        .sort(["unique_users", "total_searches"], descending=True)
        .with_row_index("rank", offset=1)
        .collect()
    )

    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM period_top_queries
        WHERE period_type = %s AND period_id = %s
    """, (period_type, period_id))

    if result_df.height == 0:
        conn.commit()
        cursor.close()
        return 0

    values = [
        (period_type, period_id, row[1], int(row[2]), int(row[3]), int(row[0]))
        for row in result_df.iter_rows()
    ]

    execute_values(
        cursor,
        """INSERT INTO period_top_queries
           (period_type, period_id, query_normalized, unique_users, total_searches, rank)
           VALUES %s""",
        values,
        page_size=5000,
    )

    conn.commit()
    cursor.close()
    return len(values)


def polars_compute_query_length_dist(
    conn, tuples_lf, period_type: str, period_id: str,
    start_date: date, end_date: date
) -> int:
    """Compute query length distribution for a period using polars."""
    import polars as pl

    if period_type == 'all_time':
        filtered = tuples_lf
    else:
        filtered = tuples_lf.filter(
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )

    result_df = (
        filtered
        .with_columns(
            (pl.col("query_normalized").str.count_matches(" ") + 1).alias("query_length")
        )
        .filter(pl.col("query_length") <= 100)
        .group_by("query_length")
        .agg(pl.col("query_normalized").n_unique().alias("unique_query_count"))
        .sort("query_length")
        .collect()
    )

    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM period_query_length_dist
        WHERE period_type = %s AND period_id = %s
    """, (period_type, period_id))

    if result_df.height == 0:
        conn.commit()
        cursor.close()
        return 0

    values = [
        (period_type, period_id, int(row[0]), int(row[1]))
        for row in result_df.iter_rows()
    ]

    execute_values(
        cursor,
        """INSERT INTO period_query_length_dist
           (period_type, period_id, query_length, unique_query_count)
           VALUES %s""",
        values,
    )

    conn.commit()
    cursor.close()
    return len(values)


def polars_compute_query_daily_stats(conn, tuples_lf, min_live_date: Optional[date]):
    """Compute daily stats for queries with 35+ unique users, using polars."""
    import polars as pl

    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_normalized FROM period_top_queries
        WHERE period_type = 'all_time' AND unique_users >= 35
    """)
    eligible = [row[0] for row in cursor.fetchall()]
    cursor.close()

    if not eligible:
        print("  No queries with 35+ users found")
        return 0

    print(f"  Found {len(eligible)} eligible queries (35+ users)")

    if min_live_date is not None:
        live_lf = tuples_lf.filter(pl.col("date") >= min_live_date)
    else:
        live_lf = tuples_lf

    chunk_size = 500
    total_inserted = 0

    for chunk_start in range(0, len(eligible), chunk_size):
        chunk = eligible[chunk_start:chunk_start + chunk_size]
        chunk_label = f"[{chunk_start + 1}-{min(chunk_start + chunk_size, len(eligible))}/{len(eligible)}]"

        chunk_df = (
            live_lf
            .filter(pl.col("query_normalized").is_in(chunk))
            .group_by(["query_normalized", "date"])
            .agg(
                pl.col("search_count").sum().alias("search_count"),
                pl.col("username").n_unique().alias("unique_users"),
            )
            .sort(["query_normalized", "date"])
            .collect()
        )

        if chunk_df.height == 0:
            print(f"  {chunk_label} No daily data found")
            continue

        cursor = conn.cursor()
        if min_live_date is not None:
            cursor.execute("""
                DELETE FROM query_daily_stats
                WHERE query_normalized = ANY(%s) AND date >= %s
            """, (chunk, min_live_date))
        else:
            cursor.execute("""
                DELETE FROM query_daily_stats
                WHERE query_normalized = ANY(%s)
            """, (chunk,))

        values = [
            (row[0], row[1], int(row[2]), int(row[3]))
            for row in chunk_df.iter_rows()
        ]

        execute_values(
            cursor,
            """INSERT INTO query_daily_stats
               (query_normalized, date, search_count, unique_users)
               VALUES %s""",
            values,
            page_size=5000,
        )

        conn.commit()
        cursor.close()

        total_inserted += len(values)
        print(f"  {chunk_label} Inserted {len(values)} daily rows")

    return total_inserted


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def refresh_period_stats(conn):
    """Refresh all period statistics."""
    archive_path = os.environ.get('ARCHIVE_PATH', '/archives')
    use_polars = has_archived_parquet(archive_path)

    ensure_period_summary_stats_table(conn)

    if use_polars:
        print("MODE: Polars (archived Parquet files found)")
        print("Loading all daily tuples (archived + live)...")
        tuples_lf = _load_all_tuples_polars(conn, archive_path)
    else:
        print("MODE: SQL (no archived Parquet files, using mv_daily_search_tuples directly)")
        tuples_lf = None

    # Get date range
    print("Getting date range...")
    if use_polars:
        import polars as pl
        date_range = tuples_lf.select(
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
        ).collect()
        if date_range.height == 0 or date_range["min_date"][0] is None:
            raise ValueError("No data found")
        min_date, max_date = date_range["min_date"][0], date_range["max_date"][0]
    else:
        min_date, max_date = _sql_date_range(conn)
    print(f"  Data range: {min_date} to {max_date}")

    print("Generating period definitions...")
    periods = generate_periods(min_date, max_date)
    weeks = [p for p in periods if p[0] == 'week']
    months = [p for p in periods if p[0] == 'month']
    print(f"  Found {len(periods)} periods ({len(weeks)} weeks, {len(months)} months, 1 all_time)")

    # Determine which periods can be skipped
    min_live_date = get_min_live_date(conn)
    existing_stats = get_existing_period_stats(conn)
    print(f"  Min live date: {min_live_date}")
    print(f"  Existing period stats: {len(existing_stats)} periods")

    periods_to_process = []
    for period_type, period_id, start_date, end_date in periods:
        if period_type == 'all_time':
            periods_to_process.append((period_type, period_id, start_date, end_date))
            continue
        if (
            min_live_date is not None
            and end_date < min_live_date
            and (period_type, period_id) in existing_stats
        ):
            continue
        periods_to_process.append((period_type, period_id, start_date, end_date))

    skipped = len(periods) - len(periods_to_process)
    print(f"  Skipping {skipped} fully-archived periods with existing stats")
    print(f"  Processing {len(periods_to_process)} periods")

    # Compute summary stats
    print("\nComputing summary stats for all periods...")
    t0 = datetime.now(timezone.utc)
    if use_polars:
        summary_lookup = polars_compute_all_summary_stats(conn, tuples_lf, periods_to_process)
    else:
        summary_lookup = sql_compute_all_summary_stats(conn, periods_to_process)
    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"  Done: {len(summary_lookup)} period summaries in {elapsed:.1f}s")

    # Compute top queries and query length dist per period
    total_queries = 0
    total_dists = 0

    for i, (period_type, period_id, start_date, end_date) in enumerate(periods_to_process, 1):
        label = f"{period_type} {period_id}"
        print(f"\n[{i}/{len(periods_to_process)}] Processing {label}...")

        summary = summary_lookup.get((period_type, period_id))
        if summary:
            print(f"  Summary stats: {summary[0]} unique queries, {summary[1]} pairs")

        t0 = datetime.now(timezone.utc)
        if use_polars:
            qi = polars_compute_top_queries(conn, tuples_lf, period_type, period_id, start_date, end_date)
        else:
            qi = sql_compute_top_queries(conn, period_type, period_id, start_date, end_date)
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        print(f"  Top queries: {qi} inserted in {elapsed:.1f}s")
        total_queries += qi

        t0 = datetime.now(timezone.utc)
        if use_polars:
            di = polars_compute_query_length_dist(conn, tuples_lf, period_type, period_id, start_date, end_date)
        else:
            di = sql_compute_query_length_dist(conn, period_type, period_id, start_date, end_date)
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        print(f"  Query length dist: {di} rows in {elapsed:.1f}s")
        total_dists += di

    print(f"\nProcessed {len(periods_to_process)} periods successfully")
    print(f"  - {total_queries} top query entries")
    print(f"  - {total_dists} query length distribution rows")

    return tuples_lf, min_live_date, use_polars


def main():
    """Main execution"""
    print(f"Starting period stats refresh at {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    try:
        conn = get_db_connection()
        tuples_lf, min_live_date, use_polars = refresh_period_stats(conn)

        print("\n" + "=" * 60)
        print("COMPUTING QUERY DAILY STATS")
        print("=" * 60)
        t0 = datetime.now(timezone.utc)
        if use_polars:
            total_daily = polars_compute_query_daily_stats(conn, tuples_lf, min_live_date)
        else:
            total_daily = sql_compute_query_daily_stats(conn, min_live_date)
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        print(f"  Query daily stats: {total_daily} rows in {elapsed:.1f}s")

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
