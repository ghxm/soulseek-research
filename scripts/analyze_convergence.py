#!/usr/bin/env python3
"""
Analyze query convergence across Soulseek research clients.

Checks whether the same queries are observed across multiple geographic clients,
which helps understand how Soulseek's distributed search routing works.
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import combinations

import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection(database_url: str):
    """Create database connection from URL."""
    # Convert asyncpg URL to psycopg2 format
    url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


def analyze_convergence(conn, hours: int = 24, offset_minutes: int = 0, window_minutes: int = 5):
    """
    Analyze how search events are distributed across clients.

    A "search event" is a (username, query) pair within a time window.
    This measures whether the same user's search propagates to all clients.

    Args:
        hours: Hours of data to analyze
        offset_minutes: Minutes to offset from current time (to allow propagation)
        window_minutes: Time window to group same (username, query) as one event
    """
    cursor = conn.cursor()

    # Get time range (with offset to exclude recent unpropagated queries)
    end_time = datetime.utcnow() - timedelta(minutes=offset_minutes)
    start_time = end_time - timedelta(hours=hours)
    window_seconds = window_minutes * 60

    print(f"\n{'='*60}")
    print(f"Query Convergence Analysis")
    print(f"{'='*60}")
    print(f"Time range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')} UTC")
    if offset_minutes > 0:
        print(f"(excluding last {offset_minutes} minutes for propagation)")
    print(f"Window: {window_minutes} min (same user+query within window = 1 event)")
    print(f"{'='*60}\n")

    # Get all unique clients
    cursor.execute("""
        SELECT DISTINCT client_id FROM searches
        WHERE timestamp >= %s AND timestamp <= %s
    """, (start_time, end_time))
    clients = [row['client_id'] for row in cursor.fetchall()]
    print(f"Active clients: {', '.join(clients)}")
    print(f"Total clients: {len(clients)}\n")

    if len(clients) < 2:
        print("Need at least 2 clients to analyze convergence.")
        return

    # Get total searches per client
    cursor.execute("""
        SELECT client_id, COUNT(*) as count
        FROM searches
        WHERE timestamp >= %s AND timestamp <= %s
        GROUP BY client_id
        ORDER BY count DESC
    """, (start_time, end_time))

    print("Searches per client (raw):")
    for row in cursor.fetchall():
        print(f"  {row['client_id']}: {row['count']:,}")

    # Analyze search event distribution across clients
    # A search event = (username, query) with timestamps bucketed by window
    # We use floor(timestamp / window) to bucket events
    cursor.execute("""
        WITH search_events AS (
            SELECT
                username,
                LOWER(TRIM(query)) as norm_query,
                client_id,
                -- Bucket timestamp to window to group same user+query within window
                FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket,
                MIN(timestamp) as first_seen
            FROM searches
            WHERE timestamp >= %s AND timestamp <= %s
            GROUP BY username, LOWER(TRIM(query)), client_id,
                     FLOOR(EXTRACT(EPOCH FROM timestamp) / %s)
        ),
        event_client_counts AS (
            SELECT
                username,
                norm_query,
                time_bucket,
                COUNT(DISTINCT client_id) as client_count
            FROM search_events
            GROUP BY username, norm_query, time_bucket
        )
        SELECT
            client_count,
            COUNT(*) as event_count
        FROM event_client_counts
        GROUP BY client_count
        ORDER BY client_count
    """, (window_seconds, start_time, end_time, window_seconds))

    results = cursor.fetchall()
    total_events = sum(row['event_count'] for row in results)

    print(f"\n{'='*60}")
    print("Search Event Distribution Across Clients")
    print(f"{'='*60}")
    print(f"\nTotal search events: {total_events:,}\n")

    for row in results:
        pct = (row['event_count'] / total_events * 100) if total_events > 0 else 0
        print(f"  Seen on {row['client_count']} client(s): {row['event_count']:,} events ({pct:.1f}%)")

    # Calculate convergence rate (events seen on ALL clients)
    if clients:
        cursor.execute("""
            WITH search_events AS (
                SELECT
                    username,
                    LOWER(TRIM(query)) as norm_query,
                    client_id,
                    FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket
                FROM searches
                WHERE timestamp >= %s AND timestamp <= %s
                GROUP BY username, LOWER(TRIM(query)), client_id,
                         FLOOR(EXTRACT(EPOCH FROM timestamp) / %s)
            )
            SELECT COUNT(*) as full_convergence
            FROM (
                SELECT username, norm_query, time_bucket
                FROM search_events
                GROUP BY username, norm_query, time_bucket
                HAVING COUNT(DISTINCT client_id) = %s
            ) t
        """, (window_seconds, start_time, end_time, window_seconds, len(clients)))

        full_convergence = cursor.fetchone()['full_convergence']
        convergence_rate = (full_convergence / total_events * 100) if total_events > 0 else 0

        print(f"\n{'='*60}")
        print(f"Full Convergence Rate (seen on ALL {len(clients)} clients)")
        print(f"{'='*60}")
        print(f"  {full_convergence:,} / {total_events:,} events ({convergence_rate:.2f}%)")

    # Time-based convergence analysis
    print(f"\n{'='*60}")
    print("Time-Based Convergence (how quickly events spread)")
    print(f"{'='*60}")

    cursor.execute("""
        WITH event_times AS (
            SELECT
                username,
                LOWER(TRIM(query)) as norm_query,
                FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket,
                client_id,
                MIN(timestamp) as first_seen
            FROM searches
            WHERE timestamp >= %s AND timestamp <= %s
            GROUP BY username, LOWER(TRIM(query)),
                     FLOOR(EXTRACT(EPOCH FROM timestamp) / %s), client_id
        ),
        multi_client_events AS (
            SELECT
                username,
                norm_query,
                time_bucket,
                EXTRACT(EPOCH FROM (MAX(first_seen) - MIN(first_seen))) as spread_seconds,
                COUNT(DISTINCT client_id) as client_count
            FROM event_times
            GROUP BY username, norm_query, time_bucket
            HAVING COUNT(DISTINCT client_id) >= 2
        ),
        bucketed AS (
            SELECT
                CASE
                    WHEN spread_seconds < 60 THEN '< 1 min'
                    WHEN spread_seconds < 300 THEN '1-5 min'
                    WHEN spread_seconds < 900 THEN '5-15 min'
                    WHEN spread_seconds < 3600 THEN '15-60 min'
                    ELSE '> 1 hour'
                END as time_spread,
                CASE
                    WHEN spread_seconds < 60 THEN 1
                    WHEN spread_seconds < 300 THEN 2
                    WHEN spread_seconds < 900 THEN 3
                    WHEN spread_seconds < 3600 THEN 4
                    ELSE 5
                END as sort_order,
                client_count
            FROM multi_client_events
        )
        SELECT
            time_spread,
            COUNT(*) as event_count,
            AVG(client_count) as avg_clients
        FROM bucketed
        GROUP BY time_spread, sort_order
        ORDER BY sort_order
    """, (window_seconds, start_time, end_time, window_seconds))

    print("\nFor events seen on 2+ clients, time between first and last reception:")
    for row in cursor.fetchall():
        print(f"  {row['time_spread']}: {row['event_count']:,} events (avg {row['avg_clients']:.1f} clients)")

    # Client subset analysis - is one client redundant?
    if len(clients) >= 2:
        print(f"\n{'='*60}")
        print("Client Overlap Analysis (is one client a subset of another?)")
        print(f"{'='*60}\n")

        # Get unique events per client
        cursor.execute("""
            WITH client_events AS (
                SELECT
                    client_id,
                    username,
                    LOWER(TRIM(query)) as norm_query,
                    FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket
                FROM searches
                WHERE timestamp >= %s AND timestamp <= %s
                GROUP BY client_id, username, LOWER(TRIM(query)),
                         FLOOR(EXTRACT(EPOCH FROM timestamp) / %s)
            )
            SELECT client_id, COUNT(*) as unique_events
            FROM client_events
            GROUP BY client_id
            ORDER BY client_id
        """, (window_seconds, start_time, end_time, window_seconds))

        client_event_counts = {row['client_id']: row['unique_events'] for row in cursor.fetchall()}

        print("Unique events per client:")
        for client, count in sorted(client_event_counts.items()):
            print(f"  {client}: {count:,}")

        # Pairwise overlap analysis
        print("\nPairwise overlap:")
        print("  (A → B means '% of A's events that B also has')\n")

        for client_a, client_b in combinations(sorted(clients), 2):
            cursor.execute("""
                WITH events_a AS (
                    SELECT DISTINCT username, LOWER(TRIM(query)) as norm_query,
                           FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket
                    FROM searches
                    WHERE client_id = %s AND timestamp >= %s AND timestamp <= %s
                ),
                events_b AS (
                    SELECT DISTINCT username, LOWER(TRIM(query)) as norm_query,
                           FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket
                    FROM searches
                    WHERE client_id = %s AND timestamp >= %s AND timestamp <= %s
                )
                SELECT
                    (SELECT COUNT(*) FROM events_a) as count_a,
                    (SELECT COUNT(*) FROM events_b) as count_b,
                    (SELECT COUNT(*) FROM events_a a WHERE EXISTS (
                        SELECT 1 FROM events_b b
                        WHERE a.username = b.username AND a.norm_query = b.norm_query AND a.time_bucket = b.time_bucket
                    )) as a_in_b,
                    (SELECT COUNT(*) FROM events_b b WHERE EXISTS (
                        SELECT 1 FROM events_a a
                        WHERE b.username = a.username AND b.norm_query = a.norm_query AND b.time_bucket = a.time_bucket
                    )) as b_in_a
            """, (window_seconds, client_a, start_time, end_time,
                  window_seconds, client_b, start_time, end_time))

            row = cursor.fetchone()
            count_a, count_b = row['count_a'], row['count_b']
            a_in_b, b_in_a = row['a_in_b'], row['b_in_a']

            pct_a_in_b = (a_in_b / count_a * 100) if count_a > 0 else 0
            pct_b_in_a = (b_in_a / count_b * 100) if count_b > 0 else 0

            print(f"  {client_a} ↔ {client_b}:")
            print(f"    {client_a} → {client_b}: {a_in_b:,}/{count_a:,} ({pct_a_in_b:.1f}%) of {client_a}'s events are in {client_b}")
            print(f"    {client_b} → {client_a}: {b_in_a:,}/{count_b:,} ({pct_b_in_a:.1f}%) of {client_b}'s events are in {client_a}")

            # Highlight if one is nearly a subset
            if pct_a_in_b >= 95:
                print(f"    ⚠️  {client_a} is ~subset of {client_b} ({pct_a_in_b:.1f}% coverage)")
            if pct_b_in_a >= 95:
                print(f"    ⚠️  {client_b} is ~subset of {client_a} ({pct_b_in_a:.1f}% coverage)")
            print()

        # Unique contribution analysis - what does each client add?
        print("Unique contribution (events ONLY seen on this client):")
        cursor.execute("""
            WITH client_events AS (
                SELECT
                    client_id,
                    username,
                    LOWER(TRIM(query)) as norm_query,
                    FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket
                FROM searches
                WHERE timestamp >= %s AND timestamp <= %s
                GROUP BY client_id, username, LOWER(TRIM(query)),
                         FLOOR(EXTRACT(EPOCH FROM timestamp) / %s)
            ),
            event_client_counts AS (
                SELECT username, norm_query, time_bucket, COUNT(DISTINCT client_id) as num_clients
                FROM client_events
                GROUP BY username, norm_query, time_bucket
            ),
            unique_only AS (
                SELECT ce.client_id, COUNT(*) as unique_count
                FROM client_events ce
                JOIN event_client_counts ecc
                    ON ce.username = ecc.username
                    AND ce.norm_query = ecc.norm_query
                    AND ce.time_bucket = ecc.time_bucket
                WHERE ecc.num_clients = 1
                GROUP BY ce.client_id
            )
            SELECT client_id, unique_count
            FROM unique_only
            ORDER BY client_id
        """, (window_seconds, start_time, end_time, window_seconds))

        unique_contributions = {row['client_id']: row['unique_count'] for row in cursor.fetchall()}

        for client in sorted(clients):
            unique = unique_contributions.get(client, 0)
            total = client_event_counts.get(client, 0)
            pct = (unique / total * 100) if total > 0 else 0
            print(f"  {client}: {unique:,} unique events ({pct:.2f}% of its total)")

    # Sample of search events seen on all clients
    if len(clients) >= 2:
        print(f"\n{'='*60}")
        print(f"Sample Search Events Seen on All {len(clients)} Clients")
        print(f"{'='*60}\n")

        cursor.execute("""
            WITH event_times AS (
                SELECT
                    username,
                    LOWER(TRIM(query)) as norm_query,
                    FLOOR(EXTRACT(EPOCH FROM timestamp) / %s) as time_bucket,
                    client_id,
                    MIN(timestamp) as first_seen
                FROM searches
                WHERE timestamp >= %s AND timestamp <= %s
                GROUP BY username, LOWER(TRIM(query)),
                         FLOOR(EXTRACT(EPOCH FROM timestamp) / %s), client_id
            )
            SELECT
                username,
                norm_query,
                ARRAY_AGG(client_id ORDER BY first_seen) as client_order,
                MIN(first_seen) as earliest,
                MAX(first_seen) as latest
            FROM event_times
            GROUP BY username, norm_query, time_bucket
            HAVING COUNT(DISTINCT client_id) = %s
            ORDER BY earliest DESC
            LIMIT 10
        """, (window_seconds, start_time, end_time, window_seconds, len(clients)))

        for row in cursor.fetchall():
            spread = (row['latest'] - row['earliest']).total_seconds()
            query_preview = row['norm_query'][:50] + '...' if len(row['norm_query']) > 50 else row['norm_query']
            user_preview = row['username'][:12] + '...' if len(row['username']) > 12 else row['username']
            print(f"  \"{query_preview}\"")
            print(f"    User: {user_preview}")
            print(f"    Clients: {' -> '.join(row['client_order'])}")
            print(f"    Spread: {spread:.1f}s\n")


def main():
    parser = argparse.ArgumentParser(description='Analyze query convergence across clients')
    parser.add_argument('--database-url', '-d',
                        default=os.environ.get('DATABASE_URL'),
                        help='PostgreSQL connection URL')
    parser.add_argument('--hours', '-H', type=int, default=24,
                        help='Hours of data to analyze (default: 24)')
    parser.add_argument('--offset', '-o', type=int, default=5,
                        help='Minutes to offset from now to allow propagation (default: 5)')
    parser.add_argument('--window', '-w', type=int, default=5,
                        help='Window in minutes to group same (user, query) as one event (default: 5)')

    args = parser.parse_args()

    if not args.database_url:
        print("Error: DATABASE_URL required (via --database-url or environment)")
        sys.exit(1)

    conn = get_connection(args.database_url)
    try:
        analyze_convergence(conn, hours=args.hours, offset_minutes=args.offset,
                           window_minutes=args.window)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
