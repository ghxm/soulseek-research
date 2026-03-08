#!/usr/bin/env python3
"""
Refresh period-specific statistics tables (weeks, months, all_time).
Called by cron at 1:00 AM UTC daily, after materialized view refresh.

Loads data from archived Parquet files + live mv_daily_search_tuples,
then computes top queries, query length distribution, and summary stats
for each period using polars.
"""

import glob
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import polars as pl
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


def _dump_live_mv_to_parquet(conn, parquet_path: str) -> int:
    """Dump mv_daily_search_tuples to a temporary Parquet file on disk.

    Uses PostgreSQL COPY TO STDOUT with a server-side cursor to stream data
    in chunks, writing each chunk to Parquet via pyarrow. This avoids holding
    the full 61M+ row dataset in Python memory (which OOM-kills on 8 GB).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        ("date", pa.date32()),
        ("username", pa.string()),
        ("query_normalized", pa.string()),
        ("search_count", pa.int64()),
    ])

    cursor = conn.cursor(name="dump_live_cursor")
    cursor.itersize = 500_000
    cursor.execute("SELECT date, username, query_normalized, search_count FROM mv_daily_search_tuples")

    writer = None
    row_count = 0
    try:
        while True:
            rows = cursor.fetchmany(cursor.itersize)
            if not rows:
                break
            table = pa.table(
                {
                    "date": [r[0] for r in rows],
                    "username": [r[1] for r in rows],
                    "query_normalized": [r[2] for r in rows],
                    "search_count": [r[3] for r in rows],
                },
                schema=schema,
            )
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, schema, compression="snappy")
            writer.write_table(table)
            row_count += len(rows)
            del rows, table
            print(f"    Dumped {row_count:,} rows...")
    finally:
        if writer is not None:
            writer.close()
        cursor.close()

    return row_count


def load_all_tuples(conn, archive_path: str) -> pl.LazyFrame:
    """Load all daily tuples from archived Parquet files and live MV data.

    Dumps live MV data to a temporary Parquet file, then uses polars
    scan_parquet over all files (archived + live). This keeps memory usage
    low because polars streams from disk rather than holding everything
    in Python memory.

    Schema: {date: pl.Date, username: pl.Utf8, query_normalized: pl.Utf8, search_count: pl.Int64}
    """
    schema = {
        "date": pl.Date,
        "username": pl.Utf8,
        "query_normalized": pl.Utf8,
        "search_count": pl.Int64,
    }

    all_parquet_files = []

    # Collect archived Parquet files
    parquet_pattern = os.path.join(archive_path, "daily_tuples_*.parquet")
    archived_files = sorted(glob.glob(parquet_pattern))
    if archived_files:
        print(f"  Found {len(archived_files)} archived Parquet files")
        all_parquet_files.extend(archived_files)
    else:
        print("  No archived Parquet files found")

    # Dump live MV data to a temporary Parquet file
    live_parquet = os.path.join(archive_path, "_live_tuples.parquet")
    print("  Dumping live MV data to temporary Parquet file...")
    row_count = _dump_live_mv_to_parquet(conn, live_parquet)
    print(f"  Dumped {row_count:,} live rows to {live_parquet}")
    if row_count > 0:
        all_parquet_files.append(live_parquet)

    if not all_parquet_files:
        return pl.LazyFrame(schema=schema)

    lf = pl.scan_parquet(all_parquet_files)
    lf = lf.cast(schema)
    return lf


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
    """Generate all week, month, and all_time periods between min and max dates.

    Returns list of (period_type, period_id, start_date, end_date) tuples.
    """
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

    # Add columns if they don't exist (for existing tables)
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


def compute_all_period_summary_stats(
    conn, tuples_lf: pl.LazyFrame, periods: List[Tuple[str, str, date, date]]
) -> Dict[Tuple[str, str], Tuple[int, int, int, int, date, date]]:
    """Compute summary stats for all non-skipped periods using polars.

    Returns lookup dict of (period_type, period_id) -> (unique_queries, unique_pairs,
    total_searches, total_users, first_date, last_date).
    """
    results = {}

    for period_type, period_id, start_date, end_date in periods:
        print(f"  Computing summary for {period_type} {period_id}...")
        start = datetime.now(timezone.utc)

        if period_type == 'all_time':
            filtered = tuples_lf
        else:
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
        total_searches = int(row[0])
        total_users = int(row[1])
        unique_queries = int(row[2])
        unique_pairs = int(row[3])
        first_dt = row[4]
        last_dt = row[5]

        results[(period_type, period_id)] = (
            unique_queries, unique_pairs, total_searches, total_users, first_dt, last_dt
        )

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"    {unique_queries} queries, {unique_pairs} pairs, "
              f"{total_searches} searches, {total_users} users ({elapsed:.1f}s)")

    # Bulk upsert all results
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


def compute_period_top_queries(
    conn, tuples_lf: pl.LazyFrame, period_type: str, period_id: str,
    start_date: date, end_date: date
) -> int:
    """Compute top queries for a period using polars and store in period_top_queries table.

    Returns number of queries inserted.
    """
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

    # Delete old data
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


def compute_period_query_length_dist(
    conn, tuples_lf: pl.LazyFrame, period_type: str, period_id: str,
    start_date: date, end_date: date
) -> int:
    """Compute query length distribution for a period using polars.

    Returns number of rows inserted.
    """
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

    # Delete old data
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


def compute_query_daily_stats(conn, tuples_lf: pl.LazyFrame, min_live_date: Optional[date]):
    """Compute daily stats for queries with 35+ all-time unique users.

    Uses period_top_queries (all_time) to identify eligible queries.
    Only deletes and reinserts rows for date >= min_live_date, preserving
    archived rows.
    """
    cursor = conn.cursor()

    # Get eligible queries from the all_time top queries
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

    # Filter tuples_lf to only live date range if we have archived data
    if min_live_date is not None:
        live_lf = tuples_lf.filter(pl.col("date") >= min_live_date)
    else:
        live_lf = tuples_lf

    # Process in chunks to avoid memory issues
    chunk_size = 500
    total_inserted = 0

    for chunk_start in range(0, len(eligible), chunk_size):
        chunk = eligible[chunk_start:chunk_start + chunk_size]
        chunk_label = f"[{chunk_start + 1}-{min(chunk_start + chunk_size, len(eligible))}/{len(eligible)}]"

        # Compute daily stats from polars
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

        # Delete old data only for live date range
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


def refresh_period_stats(conn):
    """Refresh all period statistics"""
    archive_path = os.environ.get('ARCHIVE_PATH', '/archives')

    ensure_period_summary_stats_table(conn)

    print("Loading all daily tuples (archived + live)...")
    tuples_lf = load_all_tuples(conn, archive_path)

    # Get date range from the LazyFrame
    print("Getting date range...")
    date_range = tuples_lf.select(
        pl.col("date").min().alias("min_date"),
        pl.col("date").max().alias("max_date"),
    ).collect()

    if date_range.height == 0 or date_range["min_date"][0] is None:
        raise ValueError("No data found in archives or mv_daily_search_tuples")

    min_date = date_range["min_date"][0]
    max_date = date_range["max_date"][0]
    print(f"  Data range: {min_date.isoformat()} to {max_date.isoformat()}")

    print("Generating period definitions...")
    periods = generate_periods(min_date, max_date)
    weeks = [p for p in periods if p[0] == 'week']
    months = [p for p in periods if p[0] == 'month']
    print(f"  Found {len(periods)} periods to process")
    print(f"    - {len(weeks)} weeks")
    print(f"    - {len(months)} months")
    print(f"    - 1 all_time")

    # Determine which periods can be skipped
    min_live_date = get_min_live_date(conn)
    existing_stats = get_existing_period_stats(conn)
    print(f"  Min live date: {min_live_date}")
    print(f"  Existing period stats: {len(existing_stats)} periods")

    periods_to_process = []
    for period_type, period_id, start_date, end_date in periods:
        if period_type == 'all_time':
            # Always recompute all_time
            periods_to_process.append((period_type, period_id, start_date, end_date))
            continue

        # Skip if period is fully archived (end_date < min_live_date) and already computed
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

    # Compute summary stats for all non-skipped periods
    print("\nComputing summary stats for all non-skipped periods...")
    start = datetime.now(timezone.utc)
    summary_lookup = compute_all_period_summary_stats(conn, tuples_lf, periods_to_process)
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"  Done: {len(summary_lookup)} period summaries in {elapsed:.1f}s")

    total_queries = 0
    total_dists = 0

    for i, (period_type, period_id, start_date, end_date) in enumerate(periods_to_process, 1):
        label = f"{period_type} {period_id}"
        print(f"\n[{i}/{len(periods_to_process)}] Processing {label}...")

        summary = summary_lookup.get((period_type, period_id))
        if summary:
            print(f"  Summary stats: {summary[0]} unique queries, {summary[1]} pairs")

        # Compute top queries
        start = datetime.now(timezone.utc)
        queries_inserted = compute_period_top_queries(
            conn, tuples_lf, period_type, period_id, start_date, end_date
        )
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"  Top queries: {queries_inserted} inserted in {elapsed:.1f}s")
        total_queries += queries_inserted

        # Compute query length distribution
        start = datetime.now(timezone.utc)
        dists_inserted = compute_period_query_length_dist(
            conn, tuples_lf, period_type, period_id, start_date, end_date
        )
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"  Query length dist: {dists_inserted} rows in {elapsed:.1f}s")
        total_dists += dists_inserted

    print(f"\nProcessed {len(periods_to_process)} periods successfully")
    print(f"  - {total_queries} top query entries")
    print(f"  - {total_dists} query length distribution rows")

    return tuples_lf, min_live_date


def main():
    """Main execution"""
    print(f"Starting period stats refresh at {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    try:
        conn = get_db_connection()
        tuples_lf, min_live_date = refresh_period_stats(conn)

        print("\n" + "=" * 60)
        print("COMPUTING QUERY DAILY STATS")
        print("=" * 60)
        start = datetime.now(timezone.utc)
        total_daily = compute_query_daily_stats(conn, tuples_lf, min_live_date)
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"  Query daily stats: {total_daily} rows in {elapsed:.1f}s")

        conn.close()

        # Clean up temporary live Parquet file
        archive_path = os.environ.get('ARCHIVE_PATH', '/archives')
        live_parquet = os.path.join(archive_path, "_live_tuples.parquet")
        if os.path.exists(live_parquet):
            os.remove(live_parquet)
            print("Cleaned up temporary live Parquet file")

        print("=" * 60)
        print("Period stats refresh completed successfully")
        return 0

    except Exception as e:
        print(f"\nERROR: Period stats refresh failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        # Clean up temp file on error too
        archive_path = os.environ.get('ARCHIVE_PATH', '/archives')
        live_parquet = os.path.join(archive_path, "_live_tuples.parquet")
        if os.path.exists(live_parquet):
            os.remove(live_parquet)
        return 1


if __name__ == '__main__':
    sys.exit(main())
