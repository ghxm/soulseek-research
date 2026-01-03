#!/usr/bin/env python3
"""
Generate statistics dashboard for Soulseek research data.
Queries the database and creates visualizations for GitHub Pages.
"""

import os
import json
import re
import gzip
import csv
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from typing import List, Dict, Any, Tuple, Optional, Iterator

import psycopg2
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import mistune
import yaml


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


def load_search_data(conn, start_date=None, end_date=None) -> pd.DataFrame:
    """
    Load search data from database into pandas DataFrame.

    Uses date filtering in SQL (leverages timestamp index) for performance.
    Returns all columns needed for analysis.
    """
    date_filter = ""
    if start_date and end_date:
        date_filter = f"WHERE timestamp >= '{start_date.isoformat()}' AND timestamp <= '{end_date.isoformat()}'"

    query = f"""
        SELECT id, client_id, timestamp, username, query
        FROM searches
        {date_filter}
        ORDER BY timestamp
    """

    print(f"  Loading data from database...")
    df = pd.read_sql_query(query, conn, parse_dates=['timestamp'])
    print(f"  Loaded {len(df):,} raw records")
    return df


def deduplicate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate searches within 5-minute windows using pandas.

    This handles the case where the same search is distributed to multiple
    clients, resulting in duplicate entries. We only keep the first occurrence
    within each 5-minute window for the same username+query combination.

    Performance: Much faster than SQL window functions on large datasets.
    Uses pandas vectorized operations and efficient hashing.
    """
    if df.empty:
        return df

    # Create 5-minute time bucket column (floor to nearest 5 minutes)
    # Use assign to avoid modifying the original DataFrame
    df_with_bucket = df.assign(time_bucket=df['timestamp'].dt.floor('5min'))

    # Sort by timestamp to ensure we keep the first occurrence
    df_sorted = df_with_bucket.sort_values('timestamp')

    # Keep first occurrence of each (username, query, time_bucket) combination
    df_dedup = df_sorted.drop_duplicates(
        subset=['username', 'query', 'time_bucket'],
        keep='first'
    ).drop(columns=['time_bucket'])

    print(f"  Deduplicated: {len(df):,} → {len(df_dedup):,} records ({100*(1-len(df_dedup)/len(df)):.1f}% reduction)")

    return df_dedup


def get_archived_months(conn) -> List[Dict[str, Any]]:
    """Get list of archived months from database"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT month, file_path, record_count
        FROM archives
        WHERE deleted = TRUE
        ORDER BY month
    """)
    return [{'month': r[0], 'file_path': r[1], 'count': r[2]}
            for r in cursor.fetchall()]


def read_archived_searches(archive_path: str) -> Iterator[Dict[str, str]]:
    """
    Stream-read a gzipped CSV archive file.
    Yields dict for each row: {client_id, timestamp, username, query}
    """
    if not os.path.exists(archive_path):
        print(f"Warning: Archive file not found: {archive_path}")
        return

    with gzip.open(archive_path, 'rt', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def compute_cumulative_stats(conn) -> Dict[str, Any]:
    """
    Compute all-time statistics by:
    1. Streaming through all archived CSV.gz files
    2. Querying current live database (non-archived months)
    3. Combining results

    Note: Uses deduplication within archives (5-min windows) but may have
    minor cross-archive boundary duplicates (~0.01% of data).
    """
    archive_base_path = os.environ.get('ARCHIVE_PATH', 'archives')

    # Track unique entities across all data
    all_users = set()
    all_queries = set()
    all_pairs = set()  # (username, normalized_query) tuples
    total_searches = 0
    first_timestamp = None
    last_timestamp = None

    # Process archived data
    archives = get_archived_months(conn)

    print(f"Processing {len(archives)} archive(s)...")

    for archive in archives:
        archive_path = archive['file_path']
        if not os.path.isabs(archive_path):
            # Relative path - resolve against archive_base_path
            archive_path = os.path.join(archive_base_path, os.path.basename(archive_path))

        print(f"  Reading {archive['month']} ({archive['count']:,} records)...")

        for row in read_archived_searches(archive_path):
            total_searches += 1

            # Normalize query for consistent counting
            normalized_query = row['query'].lower().strip()

            all_users.add(row['username'])
            all_queries.add(normalized_query)
            all_pairs.add((row['username'], normalized_query))

            # Track timestamps
            ts = row['timestamp']
            if first_timestamp is None or ts < first_timestamp:
                first_timestamp = ts
            if last_timestamp is None or ts > last_timestamp:
                last_timestamp = ts

    # Process live database (non-archived months)
    cursor = conn.cursor()

    # Get archived months list
    cursor.execute("SELECT month FROM archives WHERE deleted = TRUE")
    archived_months = [r[0] for r in cursor.fetchall()]

    if archived_months:
        archived_months_condition = "AND TO_CHAR(timestamp, 'YYYY-MM') NOT IN (%s)" % ','.join(
            ["'" + m + "'" for m in archived_months]
        )
    else:
        archived_months_condition = ""

    print("Processing live database...")

    # Load live data into pandas and deduplicate
    query = f"""
        SELECT timestamp, username, query
        FROM searches
        WHERE 1=1 {archived_months_condition}
        ORDER BY timestamp
    """

    df_live = pd.read_sql_query(query, conn, parse_dates=['timestamp'])
    cursor.close()

    print(f"  Loaded {len(df_live):,} raw live records")

    # Deduplicate using pandas
    df_live_dedup = deduplicate_dataframe(df_live)

    live_count = 0
    for _, row in df_live_dedup.iterrows():
        live_count += 1
        total_searches += 1

        normalized_query = row['query'].lower().strip()

        all_users.add(row['username'])
        all_queries.add(normalized_query)
        all_pairs.add((row['username'], normalized_query))

        ts = row['timestamp'].isoformat()
        if first_timestamp is None or ts < first_timestamp:
            first_timestamp = ts
        if last_timestamp is None or ts > last_timestamp:
            last_timestamp = ts

    print(f"  Processed {live_count:,} deduplicated live records")

    return {
        'all_time_searches': total_searches,
        'all_time_users': len(all_users),
        'all_time_queries': len(all_queries),
        'all_time_pairs': len(all_pairs),
        'first_search': first_timestamp,
        'last_search': last_timestamp,
        'has_archives': len(archives) > 0,
        'archive_count': len(archives)
    }


def load_article_content(article_path: str = 'docs/article.md') -> Optional[str]:
    """
    Load article Markdown content if it exists.

    Returns None if file doesn't exist, enabling graceful fallback to normal mode.
    """
    if os.path.exists(article_path):
        with open(article_path, 'r', encoding='utf-8') as f:
            return f.read()
    return None


def parse_article_sections(markdown_content: str) -> List[Dict[str, Any]]:
    """
    Parse Markdown into sections with chart markers identified.

    Returns list of section dicts:
    [
        {
            'type': 'prose',  # or 'chart' or 'stats-grid'
            'content': '<p>Rendered HTML...</p>',  # for prose
            'chart_id': 'daily_flow',  # for chart type
        },
        ...
    ]
    """
    sections = []

    # Pattern to match chart markers, stats-grid, and cumulative-stats
    pattern = r'(<!--\s*chart:\s*(\w+)\s*-->|<!--\s*stats-grid\s*-->|<!--\s*cumulative-stats\s*-->)'

    # Split content while keeping delimiters
    parts = re.split(pattern, markdown_content)

    # Create Markdown parser
    md_parser = mistune.create_markdown()

    i = 0
    while i < len(parts):
        part = parts[i] if i < len(parts) else ''

        if part and part.strip():
            if '<!-- chart:' in part:
                # Next item should be the chart_id from the regex group
                if i + 1 < len(parts):
                    chart_id = parts[i + 1].strip()
                    sections.append({'type': 'chart', 'chart_id': chart_id})
                    i += 2  # Skip the captured group
                else:
                    i += 1
            elif '<!-- stats-grid -->' in part:
                sections.append({'type': 'stats-grid'})
                i += 1
            elif '<!-- cumulative-stats -->' in part:
                sections.append({'type': 'cumulative-stats'})
                i += 1
            else:
                # Prose content - convert Markdown to HTML
                html = md_parser(part)
                if html.strip():
                    sections.append({'type': 'prose', 'content': html})
                i += 1
        else:
            i += 1

    return sections


def query_daily_stats(conn, days: int = 7) -> pd.DataFrame:
    """Query daily search counts per client for the last N days (RAW - not deduplicated)"""
    query = """
        SELECT
            client_id,
            DATE(timestamp) as date,
            COUNT(*) as search_count
        FROM searches
        WHERE timestamp >= NOW() - INTERVAL '%s days'
        GROUP BY client_id, DATE(timestamp)
        ORDER BY date DESC, client_id
    """
    return pd.read_sql_query(query, conn, params=(days,))


def query_daily_unique_users(conn, days: int = 7) -> pd.DataFrame:
    """Query unique users per day for the last N days (deduplicated)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            DATE(timestamp) as date,
            COUNT(DISTINCT username) as unique_users
        FROM deduplicated_searches
        WHERE timestamp >= NOW() - INTERVAL '%s days'
        GROUP BY DATE(timestamp)
        ORDER BY date DESC
    """
    return pd.read_sql_query(query, conn, params=(days,))


def query_client_convergence(conn, days: int = 7) -> pd.DataFrame:
    """
    Analyze how many searches reach different numbers of clients over time.
    Shows whether searches are global (all clients) or regional (few clients).
    """
    query = """
        WITH search_coverage AS (
            SELECT
                DATE(timestamp) as date,
                username,
                query,
                COUNT(DISTINCT client_id) as client_count
            FROM searches
            WHERE timestamp >= NOW() - INTERVAL '%s days'
            GROUP BY DATE(timestamp), username, query
        )
        SELECT
            date,
            SUM(CASE WHEN client_count = 1 THEN 1 ELSE 0 END) as single_client,
            SUM(CASE WHEN client_count = 2 THEN 1 ELSE 0 END) as two_clients,
            SUM(CASE WHEN client_count >= 3 THEN 1 ELSE 0 END) as all_clients
        FROM search_coverage
        GROUP BY date
        ORDER BY date
    """
    return pd.read_sql_query(query, conn, params=(days,))


def query_top_queries(conn, limit: int = 100) -> List[tuple]:
    """Query most searched queries (deduplicated, normalized with LOWER and TRIM)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            LOWER(TRIM(query)) as query,
            COUNT(DISTINCT username) as unique_users,
            COUNT(*) as total_searches
        FROM deduplicated_searches
        GROUP BY LOWER(TRIM(query))
        ORDER BY unique_users DESC
        LIMIT %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    cursor.close()
    return results


def query_most_active_users(conn, limit: int = 50) -> List[tuple]:
    """Query most active users by search count (deduplicated)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            username,
            COUNT(*) as search_count,
            COUNT(DISTINCT query) as unique_queries
        FROM deduplicated_searches
        GROUP BY username
        ORDER BY search_count DESC
        LIMIT %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    cursor.close()
    return results


def query_query_length_distribution(conn) -> pd.DataFrame:
    """Query distribution of search query lengths in words (unique queries, normalized)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            array_length(string_to_array(LOWER(TRIM(query)), ' '), 1) as query_length,
            COUNT(DISTINCT LOWER(TRIM(query))) as count
        FROM deduplicated_searches
        GROUP BY array_length(string_to_array(LOWER(TRIM(query)), ' '), 1)
        ORDER BY query_length
    """
    return pd.read_sql_query(query, conn)


def query_temporal_patterns(conn) -> pd.DataFrame:
    """Query searches by hour of day (deduplicated)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            EXTRACT(HOUR FROM timestamp) as hour,
            COUNT(*) as search_count
        FROM deduplicated_searches
        WHERE timestamp >= NOW() - INTERVAL '7 days'
        GROUP BY EXTRACT(HOUR FROM timestamp)
        ORDER BY hour
    """
    return pd.read_sql_query(query, conn)


def query_trending_searches(conn, days: int = 7) -> pd.DataFrame:
    """Query trending searches over the last N days (deduplicated)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            DATE(timestamp) as date,
            query,
            COUNT(DISTINCT username) as unique_users
        FROM deduplicated_searches
        WHERE timestamp >= NOW() - INTERVAL '%s days'
        GROUP BY DATE(timestamp), query
        HAVING COUNT(DISTINCT username) >= 3
        ORDER BY date DESC, unique_users DESC
    """
    return pd.read_sql_query(query, conn, params=(days,))


def query_all_queries_sample(conn, limit: int = 10000) -> List[str]:
    """Query sample of queries for pattern analysis (deduplicated)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT query
        FROM deduplicated_searches
        WHERE timestamp >= NOW() - INTERVAL '7 days'
        LIMIT %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (limit,))
    results = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return results


def analyze_query_patterns(queries: List[str]) -> Dict[str, int]:
    """Analyze query characteristics - counts unique queries with each feature"""
    patterns = {
        'Contains Numbers': 0,
        'Contains Special Characters': 0,
        'Contains Hyphens': 0,
        'Contains Parentheses': 0
    }

    for query in queries:
        # Normalize query for consistent checking
        query_normalized = query.lower().strip()

        # Contains numbers (0-9)
        if re.search(r'\d', query_normalized):
            patterns['Contains Numbers'] += 1

        # Contains special characters (excluding alphanumeric, spaces, hyphens, and parentheses)
        if re.search(r'[^\w\s\-\–\—\‐\‑\(\)\[\]\{\}]', query_normalized):
            patterns['Contains Special Characters'] += 1

        # Contains hyphens (all kinds: - – — ‐ ‑)
        if re.search(r'[\-\–\—\‐\‑]', query_normalized):
            patterns['Contains Hyphens'] += 1

        # Contains parentheses (all kinds: () [] {})
        if re.search(r'[\(\)\[\]\{\}]', query_normalized):
            patterns['Contains Parentheses'] += 1

    return patterns


def analyze_ngrams(queries: List[str], n: int = 2, limit: int = 30) -> List[Tuple[str, int]]:
    """
    Extract most common n-grams from queries.

    Note: Queries are normalized (lowercased and stripped) before analysis.
    """
    ngrams = Counter()

    for query in queries:
        # Normalize query for consistent analysis
        query_normalized = query.lower().strip()
        words = query_normalized.split()
        if len(words) >= n:
            for i in range(len(words) - n + 1):
                ngram = ' '.join(words[i:i+n])
                ngrams[ngram] += 1

    return ngrams.most_common(limit)


def analyze_term_cooccurrence(queries: List[str], min_freq: int = 5) -> List[Tuple[str, str, int]]:
    """
    Analyze which terms frequently appear together in searches.

    Note: Queries are normalized (lowercased and stripped) before analysis.
    """
    # Build word frequency
    word_freq = Counter()
    for query in queries:
        # Normalize query for consistent analysis
        query_normalized = query.lower().strip()
        words = set(query_normalized.split())
        word_freq.update(words)

    # Filter to common words
    common_words = {word for word, count in word_freq.items() if count >= min_freq and len(word) > 2}

    # Count co-occurrences
    cooccurrence = Counter()
    for query in queries:
        # Normalize query for consistent analysis
        query_normalized = query.lower().strip()
        words = [w for w in set(query_normalized.split()) if w in common_words]
        if len(words) >= 2:
            # Create pairs
            for i, w1 in enumerate(words):
                for w2 in words[i+1:]:
                    pair = tuple(sorted([w1, w2]))
                    cooccurrence[pair] += 1

    # Return top pairs as (word1, word2, count)
    return [(w1, w2, count) for (w1, w2), count in cooccurrence.most_common(50)]


def query_total_stats(conn) -> Dict[str, Any]:
    """Query overall statistics (deduplicated for main stats, raw for client collection)"""
    cursor = conn.cursor()
    dedup_cte = get_deduplication_cte()

    # Total searches (deduplicated)
    cursor.execute(f"{dedup_cte} SELECT COUNT(*) FROM deduplicated_searches")
    total_searches = cursor.fetchone()[0]

    # Total unique users
    cursor.execute(f"{dedup_cte} SELECT COUNT(DISTINCT username) FROM deduplicated_searches")
    total_users = cursor.fetchone()[0]

    # Total unique queries
    cursor.execute(f"{dedup_cte} SELECT COUNT(DISTINCT query) FROM deduplicated_searches")
    total_queries = cursor.fetchone()[0]

    # Unique (user, query) pairs - ignores repeated searches by same user for same query
    cursor.execute(f"{dedup_cte} SELECT COUNT(DISTINCT (username, query)) FROM deduplicated_searches")
    unique_search_pairs = cursor.fetchone()[0]

    # Date range
    cursor.execute(f"{dedup_cte} SELECT MIN(timestamp), MAX(timestamp) FROM deduplicated_searches")
    date_range = cursor.fetchone()

    # Per-client totals (RAW - not deduplicated, shows actual data collection)
    cursor.execute("""
        SELECT client_id, COUNT(*) as count
        FROM searches
        GROUP BY client_id
        ORDER BY count DESC
    """)
    client_totals = cursor.fetchall()

    cursor.close()

    # Calculate averages
    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = unique_search_pairs / total_users if total_users > 0 else 0

    return {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
        'unique_search_pairs': unique_search_pairs,
        'avg_searches_per_user': avg_searches_per_user,
        'avg_unique_queries_per_user': avg_unique_queries_per_user,
        'first_search': date_range[0].isoformat() if date_range[0] else None,
        'last_search': date_range[1].isoformat() if date_range[1] else None,
        'client_totals': {client: count for client, count in client_totals}
    }


def create_daily_flow_chart(df: pd.DataFrame) -> go.Figure:
    """Create line chart showing daily search flow per client"""
    fig = go.Figure()

    # Get unique clients with greyscale colors
    clients = df['client_id'].unique()
    greys = ['#000000', '#555555', '#999999', '#333333', '#777777']

    for i, client in enumerate(clients):
        client_data = df[df['client_id'] == client].sort_values('date')
        fig.add_trace(go.Scatter(
            x=client_data['date'],
            y=client_data['search_count'],
            mode='lines+markers',
            name=client,
            line=dict(width=2, color=greys[i % len(greys)]),
            marker=dict(size=8, color=greys[i % len(greys)])
        ))

    fig.update_layout(
        title='Daily Search Volume by Client - Raw Data Collection',
        xaxis_title='Date',
        yaxis_title='Number of Searches (Raw)',
        hovermode='x unified',
        height=500,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
        xaxis=dict(
            rangeslider=dict(visible=True),
            type='date'
        )
    )

    return fig


def create_daily_unique_users_chart(df: pd.DataFrame) -> go.Figure:
    """Create line chart showing daily unique users trend"""
    df_sorted = df.sort_values('date')

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_sorted['date'],
        y=df_sorted['unique_users'],
        mode='lines+markers',
        line=dict(width=3, color='#333333'),
        marker=dict(size=10, color='#333333', line=dict(color='black', width=1)),
        fill='tozeroy',
        fillcolor='rgba(51, 51, 51, 0.1)'
    ))

    fig.update_layout(
        title='Daily Active Users - Deduplicated',
        xaxis_title='Date',
        yaxis_title='Unique Users',
        hovermode='x unified',
        height=400,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
        xaxis=dict(
            rangeslider=dict(visible=True),
            type='date'
        )
    )

    return fig


def create_client_convergence_chart(df: pd.DataFrame) -> go.Figure:
    """Create stacked area chart showing search distribution across clients over time"""
    df_sorted = df.sort_values('date')

    fig = go.Figure()

    # Stacked area chart with greyscale colors
    fig.add_trace(go.Scatter(
        x=df_sorted['date'],
        y=df_sorted['all_clients'],
        mode='lines',
        name='All Clients (3+)',
        line=dict(width=0.5, color='#000000'),
        stackgroup='one',
        fillcolor='#000000'
    ))

    fig.add_trace(go.Scatter(
        x=df_sorted['date'],
        y=df_sorted['two_clients'],
        mode='lines',
        name='Two Clients',
        line=dict(width=0.5, color='#666666'),
        stackgroup='one',
        fillcolor='#666666'
    ))

    fig.add_trace(go.Scatter(
        x=df_sorted['date'],
        y=df_sorted['single_client'],
        mode='lines',
        name='Single Client Only',
        line=dict(width=0.5, color='#CCCCCC'),
        stackgroup='one',
        fillcolor='#CCCCCC'
    ))

    fig.update_layout(
        title='Search Distribution Across Clients - Client Convergence',
        xaxis_title='Date',
        yaxis_title='Number of Unique Searches',
        hovermode='x unified',
        height=500,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        xaxis=dict(
            rangeslider=dict(visible=True),
            type='date'
        )
    )

    return fig


def create_top_queries_chart(top_queries: List[tuple]) -> go.Figure:
    """Create bar chart of top queries"""
    queries = [q[0][:50] for q in top_queries[:20]]  # Truncate long queries
    unique_users = [q[1] for q in top_queries[:20]]  # q[1] is unique_users count

    fig = go.Figure(data=[
        go.Bar(
            y=queries[::-1],  # Reverse for better display
            x=unique_users[::-1],
            orientation='h',
            marker=dict(color='#333333', line=dict(color='black', width=1))
        )
    ])

    fig.update_layout(
        title='Top 20 Most Searched Queries (Unique Users)',
        xaxis_title='Number of Unique Users',
        yaxis_title='Query',
        height=600,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
    )

    return fig


def create_query_pattern_chart(patterns: Dict[str, int]) -> go.Figure:
    """Create bar chart of query pattern distribution"""
    # Sort by count
    sorted_patterns = sorted(patterns.items(), key=lambda x: x[1], reverse=True)
    pattern_names = [p[0] for p in sorted_patterns]
    counts = [p[1] for p in sorted_patterns]

    fig = go.Figure(data=[
        go.Bar(
            x=counts,
            y=pattern_names,
            orientation='h',
            marker=dict(color='#333333', line=dict(color='black', width=1))
        )
    ])

    fig.update_layout(
        title='Search Query Characteristics',
        xaxis_title='Number of Unique Queries',
        yaxis_title='Characteristic',
        height=500,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
    )

    return fig


def create_temporal_hourly_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart of searches by hour of day"""
    fig = go.Figure(data=[
        go.Bar(
            x=df['hour'],
            y=df['search_count'],
            marker=dict(color='#555555', line=dict(color='black', width=1))
        )
    ])

    fig.update_layout(
        title='Search Activity by Hour of Day (Last 7 Days, UTC)',
        xaxis_title='Hour (UTC)',
        yaxis_title='Number of Searches',
        xaxis=dict(tickmode='linear', tick0=0, dtick=2),
        height=400,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
    )

    return fig


def create_query_length_chart(df: pd.DataFrame) -> go.Figure:
    """Create histogram of query length distribution (by word count)"""
    # Filter to reasonable lengths for visualization
    df_filtered = df[df['query_length'] <= 100]

    fig = go.Figure(data=[
        go.Bar(
            x=df_filtered['query_length'],
            y=df_filtered['count'],
            marker=dict(color='#444444', line=dict(color='black', width=1))
        )
    ])

    fig.update_layout(
        title='Search Query Length Distribution (Words)',
        xaxis_title='Query Length (Number of Words)',
        yaxis_title='Number of Unique Queries',
        height=400,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
    )

    return fig


def create_ngram_chart(bigrams: List[Tuple[str, int]], trigrams: List[Tuple[str, int]]) -> go.Figure:
    """Create comparison chart of most common 2-grams and 3-grams"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=['Most Common 2-Word Phrases', 'Most Common 3-Word Phrases'],
        horizontal_spacing=0.15
    )

    # Bigrams
    if bigrams:
        top_bigrams = bigrams[:15]
        bigram_phrases = [b[0] for b in top_bigrams][::-1]
        bigram_counts = [b[1] for b in top_bigrams][::-1]

        fig.add_trace(
            go.Bar(
                y=bigram_phrases,
                x=bigram_counts,
                orientation='h',
                marker=dict(color='#333333', line=dict(color='black', width=1)),
                showlegend=False
            ),
            row=1, col=1
        )

    # Trigrams
    if trigrams:
        top_trigrams = trigrams[:15]
        trigram_phrases = [t[0] for t in top_trigrams][::-1]
        trigram_counts = [t[1] for t in top_trigrams][::-1]

        fig.add_trace(
            go.Bar(
                y=trigram_phrases,
                x=trigram_counts,
                orientation='h',
                marker=dict(color='#555555', line=dict(color='black', width=1)),
                showlegend=False
            ),
            row=1, col=2
        )

    fig.update_layout(
        title='Common Multi-Word Phrases in Searches',
        height=600,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black', size=10)
    )

    fig.update_xaxes(title_text='Frequency', row=1, col=1)
    fig.update_xaxes(title_text='Frequency', row=1, col=2)

    return fig


def create_cooccurrence_chart(cooccurrences: List[Tuple[str, str, int]]) -> go.Figure:
    """Create network visualization of term co-occurrence"""
    if not cooccurrences or len(cooccurrences) < 3:
        return None

    # Take top 30 pairs
    top_pairs = cooccurrences[:30]

    # Build edge list
    edges_x = []
    edges_y = []
    edge_weights = []

    # Get unique terms and assign positions in a circle
    terms = set()
    for w1, w2, count in top_pairs:
        terms.add(w1)
        terms.add(w2)

    terms = list(terms)[:20]  # Limit to 20 terms for readability
    n = len(terms)

    # Position terms in a circle
    term_positions = {}
    for i, term in enumerate(terms):
        angle = 2 * np.pi * i / n
        term_positions[term] = (np.cos(angle), np.sin(angle))

    # Create edges
    edge_traces = []
    for w1, w2, count in top_pairs:
        if w1 in term_positions and w2 in term_positions:
            x0, y0 = term_positions[w1]
            x1, y1 = term_positions[w2]

            # Edge thickness based on count
            width = min(count / 3, 5)

            edge_traces.append(go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode='lines',
                line=dict(width=width, color='#999999'),
                hoverinfo='none',
                showlegend=False
            ))

    # Create nodes
    node_x = [term_positions[t][0] for t in terms]
    node_y = [term_positions[t][1] for t in terms]

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers+text',
        text=terms,
        textposition='top center',
        marker=dict(
            size=15,
            color='#333333',
            line=dict(color='black', width=2)
        ),
        textfont=dict(size=10, color='black'),
        hoverinfo='text',
        showlegend=False
    )

    # Create figure
    fig = go.Figure(data=edge_traces + [node_trace])

    fig.update_layout(
        title='Term Co-occurrence Network (Words Appearing Together)',
        showlegend=False,
        hovermode='closest',
        height=700,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
    )

    return fig


def create_user_activity_chart(top_users: List[tuple]) -> go.Figure:
    """Create bar chart of most active users"""
    # Users are hashed, so show as User #1, User #2, etc.
    user_labels = [f"User #{i+1}" for i in range(min(20, len(top_users)))]
    search_counts = [u[1] for u in top_users[:20]]

    fig = go.Figure(data=[
        go.Bar(
            y=user_labels[::-1],
            x=search_counts[::-1],
            orientation='h',
            marker=dict(color='#555555', line=dict(color='black', width=1))
        )
    ])

    fig.update_layout(
        title='Top 20 Most Active Users (Anonymized)',
        xaxis_title='Total Searches',
        yaxis_title='User ID',
        height=600,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
    )

    return fig


def create_client_distribution_chart(client_totals: Dict[str, int]) -> go.Figure:
    """Create pie chart of search distribution by client"""
    # Greyscale palette for pie chart
    grey_palette = ['#000000', '#555555', '#999999', '#333333', '#777777']

    fig = go.Figure(data=[
        go.Pie(
            labels=list(client_totals.keys()),
            values=list(client_totals.values()),
            hole=0.3,
            marker=dict(colors=grey_palette, line=dict(color='white', width=2))
        )
    ])

    fig.update_layout(
        title='Search Distribution by Geographic Client - Raw Data Collection',
        height=400,
        template='plotly_white',
        paper_bgcolor='white',
        font=dict(color='black')
    )

    return fig


def generate_stats_grid_html(stats: Dict) -> str:
    """Generate the stats summary cards HTML."""
    days = (datetime.fromisoformat(stats['last_search']) -
            datetime.fromisoformat(stats['first_search'])).days

    return f'''
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Searches</h3>
                <div class="value">{stats['total_searches']:,}</div>
                <div class="label">Deduplicated searches</div>
            </div>
            <div class="stat-card">
                <h3>Unique Users</h3>
                <div class="value">{stats['total_users']:,}</div>
                <div class="label">Anonymized users</div>
            </div>
            <div class="stat-card">
                <h3>Unique Queries</h3>
                <div class="value">{stats['total_queries']:,}</div>
                <div class="label">Different search terms</div>
            </div>
            <div class="stat-card">
                <h3>Avg Searches per User</h3>
                <div class="value">{stats['avg_searches_per_user']:.1f}</div>
                <div class="label">Including repeated searches</div>
            </div>
            <div class="stat-card">
                <h3>Avg Unique Queries per User</h3>
                <div class="value">{stats['avg_unique_queries_per_user']:.1f}</div>
                <div class="label">Search diversity</div>
            </div>
            <div class="stat-card">
                <h3>Collection Period</h3>
                <div class="value">{days}</div>
                <div class="label">Days of data</div>
            </div>
        </div>
    '''


def generate_cumulative_stats_html(cumulative: Dict) -> str:
    """Generate the all-time cumulative statistics section HTML."""
    if not cumulative.get('has_archives') and cumulative.get('all_time_searches', 0) == 0:
        # No data yet
        return ''

    # Calculate collection period
    if cumulative.get('first_search') and cumulative.get('last_search'):
        first_date = cumulative['first_search'][:10] if isinstance(cumulative['first_search'], str) else cumulative['first_search'].strftime('%Y-%m-%d')
        last_date = cumulative['last_search'][:10] if isinstance(cumulative['last_search'], str) else cumulative['last_search'].strftime('%Y-%m-%d')
        period_label = f"{first_date} to {last_date}"
    else:
        period_label = "Unknown period"

    avg_queries_per_user = cumulative['all_time_pairs'] / cumulative['all_time_users'] if cumulative.get('all_time_users', 0) > 0 else 0

    archive_note = f" ({cumulative['archive_count']} archive(s))" if cumulative.get('has_archives') else ""

    return f'''
        <h2>All-Time Statistics{archive_note}</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Searches</h3>
                <div class="value">{cumulative['all_time_searches']:,}</div>
                <div class="label">Since {first_date}</div>
            </div>
            <div class="stat-card">
                <h3>Unique Users</h3>
                <div class="value">{cumulative['all_time_users']:,}</div>
                <div class="label">Across all time</div>
            </div>
            <div class="stat-card">
                <h3>Unique Queries</h3>
                <div class="value">{cumulative['all_time_queries']:,}</div>
                <div class="label">Different search terms</div>
            </div>
            <div class="stat-card">
                <h3>Unique Search Pairs</h3>
                <div class="value">{cumulative['all_time_pairs']:,}</div>
                <div class="label">Unique (user, query) combinations</div>
            </div>
            <div class="stat-card">
                <h3>Avg Queries per User</h3>
                <div class="value">{avg_queries_per_user:.1f}</div>
                <div class="label">Search diversity (all-time)</div>
            </div>
            <div class="stat-card">
                <h3>Collection Period</h3>
                <div class="value">{period_label}</div>
                <div class="label">Full date range</div>
            </div>
        </div>
    '''


def generate_article_html(stats: Dict, figures: Dict[str, go.Figure],
                          sections: List[Dict[str, Any]],
                          cumulative: Optional[Dict] = None) -> str:
    """Generate HTML dashboard in article mode with narrative content."""
    # Convert figures to HTML
    chart_html = {}
    for name, fig in figures.items():
        if fig is not None:
            chart_html[name] = fig.to_html(full_html=False, include_plotlyjs='cdn')
        else:
            chart_html[name] = '<p>Not enough data for visualization</p>'

    # Build body content from sections
    body_parts = []
    for section in sections:
        if section['type'] == 'prose':
            body_parts.append(f'<div class="prose">{section["content"]}</div>')
        elif section['type'] == 'chart':
            chart_id = section['chart_id']
            if chart_id in chart_html:
                body_parts.append(f'<div class="chart">{chart_html[chart_id]}</div>')
            else:
                print(f"Warning: Unknown chart ID '{chart_id}' in article.md")
                body_parts.append(f'<p style="color: #999; font-style: italic;">Chart not found: {chart_id}</p>')
        elif section['type'] == 'stats-grid':
            body_parts.append(generate_stats_grid_html(stats))
        elif section['type'] == 'cumulative-stats':
            if cumulative:
                body_parts.append(generate_cumulative_stats_html(cumulative))

    body_content = '\n'.join(body_parts)

    # Same HTML structure as normal mode, but with article-mode class and dynamic content
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Soulseek Research Report</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: white;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
        }}
        h1 {{
            color: black;
            border-bottom: 2px solid black;
            padding-bottom: 10px;
            font-weight: 600;
        }}
        h2 {{
            color: black;
            margin-top: 40px;
            border-bottom: 1px solid #ccc;
            padding-bottom: 8px;
            font-weight: 600;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .stat-card {{
            background: white;
            color: black;
            padding: 20px;
            border: 1px solid #333;
        }}
        .stat-card h3 {{
            margin: 0;
            font-size: 14px;
            font-weight: 400;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-card .value {{
            font-size: 32px;
            font-weight: 600;
            margin: 10px 0;
            color: black;
        }}
        .stat-card .label {{
            font-size: 12px;
            color: #999;
        }}
        .chart {{
            margin: 30px 0;
            border: 1px solid #eee;
            padding: 10px;
        }}
        .footer {{
            margin-top: 50px;
            text-align: center;
            color: #999;
            font-size: 14px;
            border-top: 1px solid #eee;
            padding-top: 20px;
        }}
        .footer a {{
            color: #666;
            text-decoration: none;
            border-bottom: 1px solid #666;
        }}
        .timestamp {{
            color: #666;
            font-size: 14px;
            margin-top: 20px;
        }}
        /* Article mode prose styling */
        .prose {{
            max-width: 800px;
            margin: 0 auto 30px auto;
            line-height: 1.7;
            color: #333;
        }}
        .prose p {{
            margin: 1em 0;
            font-size: 16px;
        }}
        .prose h2 {{
            margin-top: 50px;
        }}
        .prose h3 {{
            color: #333;
            margin-top: 30px;
            font-size: 20px;
            font-weight: 500;
        }}
        .prose ul, .prose ol {{
            margin: 1em 0;
            padding-left: 2em;
        }}
        .prose li {{
            margin: 0.5em 0;
        }}
        .prose code {{
            background: #f5f5f5;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 14px;
        }}
        .prose pre {{
            background: #f5f5f5;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
        }}
        .prose pre code {{
            background: none;
            padding: 0;
        }}
        .prose blockquote {{
            border-left: 4px solid #333;
            margin: 1.5em 0;
            padding-left: 20px;
            color: #666;
            font-style: italic;
        }}
        .prose a {{
            color: #333;
            text-decoration: underline;
        }}
        .prose strong {{
            font-weight: 600;
        }}
        .prose em {{
            font-style: italic;
        }}
        /* Center charts in article mode */
        .article-mode .chart {{
            max-width: 1200px;
            margin: 30px auto;
        }}
    </style>
</head>
<body>
    <div class="container article-mode">
        <p class="timestamp">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

        {body_content}

        <div class="footer">
            <p>Soulseek Research Project | Data collected from distributed geographic locations</p>
            <p>All usernames are cryptographically hashed for privacy | Research use only</p>
            <p><a href="https://github.com/ghxm/soulseek-research">View on GitHub</a></p>
        </div>
    </div>
</body>
</html>
"""
    return html


def generate_html(stats: Dict, figures: Dict[str, go.Figure],
                  cumulative: Optional[Dict] = None,
                  article_content: Optional[str] = None) -> str:
    """
    Generate HTML dashboard.

    If article_content is provided, generates article mode with prose.
    Otherwise, generates standard chart-only dashboard (existing behavior).
    """
    # Article mode: parse Markdown and generate article-style HTML
    if article_content:
        sections = parse_article_sections(article_content)
        return generate_article_html(stats, figures, sections, cumulative)

    # Normal mode: generate standard dashboard
    # Convert figures to HTML divs
    chart_html = {}
    for name, fig in figures.items():
        if fig is not None:
            chart_html[name] = fig.to_html(full_html=False, include_plotlyjs='cdn')
        else:
            chart_html[name] = '<p>Not enough data for visualization</p>'

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Soulseek Research Dashboard</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: white;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
        }}
        h1 {{
            color: black;
            border-bottom: 2px solid black;
            padding-bottom: 10px;
            font-weight: 600;
        }}
        h2 {{
            color: black;
            margin-top: 40px;
            border-bottom: 1px solid #ccc;
            padding-bottom: 8px;
            font-weight: 600;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .stat-card {{
            background: white;
            color: black;
            padding: 20px;
            border: 1px solid #333;
        }}
        .stat-card h3 {{
            margin: 0;
            font-size: 14px;
            font-weight: 400;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-card .value {{
            font-size: 32px;
            font-weight: 600;
            margin: 10px 0;
            color: black;
        }}
        .stat-card .label {{
            font-size: 12px;
            color: #999;
        }}
        .chart {{
            margin: 30px 0;
            border: 1px solid #eee;
            padding: 10px;
        }}
        .footer {{
            margin-top: 50px;
            text-align: center;
            color: #999;
            font-size: 14px;
            border-top: 1px solid #eee;
            padding-top: 20px;
        }}
        .footer a {{
            color: #666;
            text-decoration: none;
            border-bottom: 1px solid #666;
        }}
        .timestamp {{
            color: #666;
            font-size: 14px;
            margin-top: 20px;
        }}
        /* Article mode prose styling */
        .prose {{
            max-width: 800px;
            margin: 0 auto 30px auto;
            line-height: 1.7;
            color: #333;
        }}
        .prose p {{
            margin: 1em 0;
            font-size: 16px;
        }}
        .prose h2 {{
            margin-top: 50px;
        }}
        .prose h3 {{
            color: #333;
            margin-top: 30px;
            font-size: 20px;
            font-weight: 500;
        }}
        .prose ul, .prose ol {{
            margin: 1em 0;
            padding-left: 2em;
        }}
        .prose li {{
            margin: 0.5em 0;
        }}
        .prose code {{
            background: #f5f5f5;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 14px;
        }}
        .prose pre {{
            background: #f5f5f5;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
        }}
        .prose pre code {{
            background: none;
            padding: 0;
        }}
        .prose blockquote {{
            border-left: 4px solid #333;
            margin: 1.5em 0;
            padding-left: 20px;
            color: #666;
            font-style: italic;
        }}
        .prose a {{
            color: #333;
            text-decoration: underline;
        }}
        .prose strong {{
            font-weight: 600;
        }}
        .prose em {{
            font-style: italic;
        }}
        /* Center charts in article mode */
        .article-mode .chart {{
            max-width: 1200px;
            margin: 30px auto;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Soulseek Research Data Collection</h1>

        <p class="timestamp">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

        <div style="background: #f5f5f5; padding: 20px; margin: 20px 0; border-left: 4px solid #333;">
            <h3 style="margin-top: 0; color: #333;">Data Deduplication Note</h3>
            <p style="margin-bottom: 0; color: #666;">
                Because Soulseek distributes searches across multiple clients, the same search from one user
                can appear on 2-3 of our geographic clients simultaneously. <strong>Analysis statistics are
                deduplicated within 5-minute windows</strong> to prevent inflated counts, keeping only the first
                occurrence of each username+query combination. However, <strong>data collection metrics show
                raw counts</strong> to accurately represent what each client receives from the network.
            </p>
        </div>

        {generate_cumulative_stats_html(cumulative) if cumulative else ''}

        <h2>Current Period Statistics</h2>
        {generate_stats_grid_html(stats)}

        <h2>Data Collection Overview <span style="font-weight: normal; font-size: 14px; color: #999;">(Raw Counts)</span></h2>
        <div class="chart">
            {chart_html['daily_flow']}
        </div>

        <div class="chart">
            {chart_html['client_distribution']}
        </div>

        <div class="chart">
            {chart_html.get('client_convergence', '<p>Not enough data</p>')}
        </div>

        <h2>User Activity Trends <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
        <div class="chart">
            {chart_html.get('daily_unique_users', '<p>Not enough data</p>')}
        </div>

        <h2>Search Patterns Analysis <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
        <div class="chart">
            {chart_html['top_queries']}
        </div>

        <div class="chart">
            {chart_html.get('query_patterns', '<p>Not enough data</p>')}
        </div>

        <div class="chart">
            {chart_html.get('query_length', '<p>Not enough data</p>')}
        </div>

        <h2>Temporal Analysis <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
        <div class="chart">
            {chart_html.get('temporal_hourly', '<p>Not enough data</p>')}
        </div>

        <h2>Search Term Analysis <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
        <div class="chart">
            {chart_html.get('ngrams', '<p>Not enough data</p>')}
        </div>

        <div class="chart">
            {chart_html.get('cooccurrence', '<p>Not enough data for network visualization</p>')}
        </div>

        <h2>User Activity <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
        <div class="chart">
            {chart_html['user_activity']}
        </div>

        <div class="footer">
            <p>Soulseek Research Project | Data collected from distributed geographic locations</p>
            <p>All usernames are cryptographically hashed for privacy</p>
            <p><a href="https://github.com/ghxm/soulseek-research">View on GitHub</a></p>
        </div>
    </div>
</body>
</html>
"""
    return html


def get_available_periods(conn) -> Dict[str, List[Dict]]:
    """Get all available weeks and months from the database"""
    cursor = conn.cursor()

    # Get date range - simple query, no deduplication needed for date range
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM searches")
    min_date, max_date = cursor.fetchone()

    if not min_date or not max_date:
        return {'weeks': [], 'months': []}

    # Generate all weeks
    weeks = []
    current = min_date
    while current <= max_date:
        iso_year, iso_week, _ = current.isocalendar()
        week_id = f"{iso_year}-W{iso_week:02d}"
        week_label = f"{iso_week:02d}/{iso_year}"

        # Find week start and end
        week_start = datetime.fromisocalendar(iso_year, iso_week, 1).replace(tzinfo=timezone.utc)
        week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)

        weeks.append({
            'id': week_id,
            'label': week_label,
            'start': week_start,
            'end': week_end
        })

        current = week_end + timedelta(seconds=1)

    # Generate all months
    months = []
    current = min_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= max_date:
        month_id = current.strftime('%Y-%m')
        month_label = current.strftime('%B %Y')

        # Find month end
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        month_end = next_month - timedelta(seconds=1)

        months.append({
            'id': month_id,
            'label': month_label,
            'start': current,
            'end': month_end
        })

        current = next_month

    cursor.close()
    return {'weeks': weeks, 'months': months}


def generate_jekyll_data_files(periods: Dict[str, List[Dict]]):
    """Generate Jekyll _data files for navigation"""
    import yaml

    os.makedirs('docs/_data', exist_ok=True)

    # Generate months.yml (newest first)
    months_data = [{'id': m['id'], 'label': m['label']} for m in reversed(periods['months'])]
    with open('docs/_data/months.yml', 'w', encoding='utf-8') as f:
        yaml.dump(months_data, f, default_flow_style=False, allow_unicode=True)

    # Generate weeks.yml (newest first)
    weeks_data = [{'id': w['id'], 'label': w['label']} for w in reversed(periods['weeks'])]
    with open('docs/_data/weeks.yml', 'w', encoding='utf-8') as f:
        yaml.dump(weeks_data, f, default_flow_style=False, allow_unicode=True)


def query_period_data(conn, start_date=None, end_date=None):
    """Query all data for a specific period (or all-time if dates are None)"""
    # Get deduplication CTE with date filtering for performance
    cursor = conn.cursor()
    dedup_cte = get_deduplication_cte(start_date, end_date)

    # Total stats (date filter already in CTE)
    cursor.execute(f"{dedup_cte} SELECT COUNT(*) FROM deduplicated_searches")
    total_searches = cursor.fetchone()[0]

    cursor.execute(f"{dedup_cte} SELECT COUNT(DISTINCT username) FROM deduplicated_searches")
    total_users = cursor.fetchone()[0]

    cursor.execute(f"{dedup_cte} SELECT COUNT(DISTINCT query) FROM deduplicated_searches")
    total_queries = cursor.fetchone()[0]

    cursor.execute(f"{dedup_cte} SELECT COUNT(DISTINCT (username, query)) FROM deduplicated_searches")
    unique_search_pairs = cursor.fetchone()[0]

    cursor.execute(f"{dedup_cte} SELECT MIN(timestamp), MAX(timestamp) FROM deduplicated_searches")
    date_range = cursor.fetchone()

    # Per-client totals (raw - need explicit date filter)
    date_filter = ""
    if start_date and end_date:
        date_filter = f"WHERE timestamp >= '{start_date.isoformat()}' AND timestamp <= '{end_date.isoformat()}'"

    cursor.execute(f"""
        SELECT client_id, COUNT(*) as count
        FROM searches
        {date_filter}
        GROUP BY client_id
        ORDER BY count DESC
    """)
    client_totals = cursor.fetchall()

    cursor.close()

    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = unique_search_pairs / total_users if total_users > 0 else 0

    return {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
        'unique_search_pairs': unique_search_pairs,
        'avg_searches_per_user': avg_searches_per_user,
        'avg_unique_queries_per_user': avg_unique_queries_per_user,
        'first_search': date_range[0].isoformat() if date_range[0] else None,
        'last_search': date_range[1].isoformat() if date_range[1] else None,
        'client_totals': {client: count for client, count in client_totals},
        'date_filter': date_filter
    }


def generate_period_page_from_df(conn, df_raw: pd.DataFrame, df_dedup: pd.DataFrame,
                                  period_type: str, period_info: Optional[Dict] = None) -> str:
    """
    Generate a dashboard page for a specific period using pre-loaded dataframes.

    conn: Database connection (for computing cumulative stats if needed)
    df_raw: Raw search data (not deduplicated)
    df_dedup: Deduplicated search data
    period_type: 'all', 'month', or 'week'
    period_info: Dict with 'id', 'label', 'start', 'end' (None for all-time)
    """
    if period_type == 'all':
        period_label = "All Time"
        page_title = "Soulseek Research Dashboard - All Time"
        output_file = "docs/index.html"
    else:
        period_label = period_info['label']

        if period_type == 'week':
            page_title = f"Soulseek Research Dashboard - CW {period_label}"
            output_file = f"docs/weeks/{period_info['id']}.html"
        else:  # month
            page_title = f"Soulseek Research Dashboard - {period_label}"
            output_file = f"docs/months/{period_info['id']}.html"

    if df_dedup.empty:
        print(f"  No data for {period_label}, skipping")
        return None

    print(f"  Computing statistics for {period_label}...")

    # Calculate summary statistics from deduplicated data
    total_searches = len(df_dedup)
    total_users = df_dedup['username'].nunique()
    total_queries = df_dedup['query'].nunique()
    unique_search_pairs = df_dedup.groupby(['username', 'query']).ngroups

    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = unique_search_pairs / total_users if total_users > 0 else 0

    first_search = df_dedup['timestamp'].min().isoformat()
    last_search = df_dedup['timestamp'].max().isoformat()

    # Per-client totals (from raw data, not deduplicated)
    client_totals = df_raw.groupby('client_id').size().to_dict()

    stats = {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
        'unique_search_pairs': unique_search_pairs,
        'avg_searches_per_user': avg_searches_per_user,
        'avg_unique_queries_per_user': avg_unique_queries_per_user,
        'first_search': first_search,
        'last_search': last_search,
        'client_totals': client_totals
    }

    print(f"  Generating time-series data...")

    # Daily stats by client (raw data)
    df_raw['date'] = df_raw['timestamp'].dt.date
    daily_stats = df_raw.groupby(['client_id', 'date']).size().reset_index(name='search_count')

    # Daily unique users (deduplicated)
    df_dedup['date'] = df_dedup['timestamp'].dt.date
    daily_unique_users = df_dedup.groupby('date')['username'].nunique().reset_index(name='unique_users')

    # Client convergence analysis (how many clients see each search)
    df_raw['query_normalized'] = df_raw['query'].str.lower().str.strip()
    search_coverage = df_raw.groupby(['date', 'username', 'query_normalized'])['client_id'].nunique().reset_index(name='client_count')
    client_convergence = pd.DataFrame({
        'date': search_coverage['date'].unique()
    })
    client_convergence = client_convergence.merge(
        search_coverage[search_coverage['client_count'] == 1].groupby('date').size().reset_index(name='single_client'),
        on='date', how='left'
    ).merge(
        search_coverage[search_coverage['client_count'] == 2].groupby('date').size().reset_index(name='two_clients'),
        on='date', how='left'
    ).merge(
        search_coverage[search_coverage['client_count'] >= 3].groupby('date').size().reset_index(name='all_clients'),
        on='date', how='left'
    ).fillna(0)

    print(f"  Analyzing search patterns...")

    # Top queries (normalized, deduplicated)
    df_dedup['query_normalized'] = df_dedup['query'].str.lower().str.strip()
    top_queries_df = df_dedup.groupby('query_normalized').agg(
        unique_users=('username', 'nunique'),
        total_searches=('query', 'size')
    ).sort_values('unique_users', ascending=False).head(100)
    top_queries = [(q, row['unique_users'], row['total_searches']) for q, row in top_queries_df.iterrows()]

    # Top users
    top_users_df = df_dedup.groupby('username').agg(
        search_count=('query', 'size'),
        unique_queries=('query', 'nunique')
    ).sort_values('search_count', ascending=False).head(50)
    top_users = [(u, row['search_count'], row['unique_queries']) for u, row in top_users_df.iterrows()]

    # Sample queries for pattern analysis
    query_sample = df_dedup['query'].head(10000).tolist()
    query_patterns = analyze_query_patterns(query_sample)

    # Temporal patterns (by hour)
    df_dedup['hour'] = df_dedup['timestamp'].dt.hour
    temporal_data = df_dedup.groupby('hour').size().reset_index(name='search_count')

    # Query length distribution
    df_dedup['query_length'] = df_dedup['query_normalized'].str.split().str.len()
    query_length_data = df_dedup.groupby('query_length')['query_normalized'].nunique().reset_index(name='count')
    query_length_data = query_length_data[query_length_data['query_length'] <= 100]  # Filter outliers

    # N-grams analysis
    bigrams = analyze_ngrams(query_sample, n=2, limit=30)
    trigrams = analyze_ngrams(query_sample, n=3, limit=30)
    cooccurrences = analyze_term_cooccurrence(query_sample, min_freq=10)

    print(f"  Found {total_searches:,} searches for {period_label}")
    print(f"  Creating visualizations...")

    # Create figures
    figures = {
        'daily_flow': create_daily_flow_chart(daily_stats),
        'client_convergence': create_client_convergence_chart(client_convergence) if not client_convergence.empty else None,
        'daily_unique_users': create_daily_unique_users_chart(daily_unique_users) if not daily_unique_users.empty else None,
        'top_queries': create_top_queries_chart(top_queries) if top_queries else None,
        'user_activity': create_user_activity_chart(top_users) if top_users else None,
        'client_distribution': create_client_distribution_chart(client_totals) if client_totals else None,
        'query_patterns': create_query_pattern_chart(query_patterns) if query_patterns else None,
        'temporal_hourly': create_temporal_hourly_chart(temporal_data) if not temporal_data.empty else None,
        'query_length': create_query_length_chart(query_length_data) if not query_length_data.empty else None,
        'ngrams': create_ngram_chart(bigrams, trigrams) if bigrams and trigrams else None,
        'cooccurrence': create_cooccurrence_chart(cooccurrences) if cooccurrences else None
    }

    # Check for article mode (only for all-time page)
    article_content = None
    if period_type == 'all':
        article_content = load_article_content('docs/article.md')
        if article_content:
            print("  Using article mode for all-time page")

    # Generate HTML with Jekyll front matter
    if article_content:
        # Use article mode (with charts embedded in narrative)
        sections = parse_article_sections(article_content)
        html = generate_article_html_with_jekyll(stats, figures, sections, period_type, period_info)
    else:
        # Standard dashboard page
        html = generate_period_html(stats, figures, period_type, period_info)

    # Write output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  ✅ Generated {output_file}")
    return output_file


def generate_article_html_with_jekyll(stats: Dict, figures: Dict[str, go.Figure],
                                      sections: List[Dict[str, Any]],
                                      period_type: str, period_info: Optional[Dict] = None) -> str:
    """Generate article mode HTML with Jekyll front matter"""
    # Jekyll front matter (article mode is only for all-time)
    front_matter = "---\nlayout: dashboard\nperiod: all\ntitle: All Time Statistics\n---\n\n"

    # Convert figures to HTML
    chart_html = {}
    for name, fig in figures.items():
        if fig is not None:
            chart_html[name] = fig.to_html(full_html=False, include_plotlyjs='cdn')
        else:
            chart_html[name] = '<p>Not enough data for visualization</p>'

    # Build body content from sections
    body_parts = []
    for section in sections:
        if section['type'] == 'prose':
            body_parts.append(f'<div class="prose">{section["content"]}</div>')
        elif section['type'] == 'chart':
            chart_id = section['chart_id']
            if chart_id in chart_html:
                body_parts.append(f'<div class="chart">{chart_html[chart_id]}</div>')
            else:
                print(f"Warning: Unknown chart ID '{chart_id}' in article.md")
                body_parts.append(f'<p style="color: #999; font-style: italic;">Chart not found: {chart_id}</p>')
        elif section['type'] == 'stats-grid':
            body_parts.append(generate_stats_grid_html(stats))

    body_content = '\n'.join(body_parts)

    # Add article mode specific CSS inline
    article_css = '''
    <style>
        .prose {
            max-width: 800px;
            margin: 0 auto 30px auto;
            line-height: 1.7;
            color: #333;
        }
        .prose p {
            margin: 1em 0;
            font-size: 16px;
        }
        .prose h2 {
            margin-top: 50px;
        }
        .prose h3 {
            color: #333;
            margin-top: 30px;
            font-size: 20px;
            font-weight: 500;
        }
        .prose ul, .prose ol {
            margin: 1em 0;
            padding-left: 2em;
        }
        .prose li {
            margin: 0.5em 0;
        }
        .prose code {
            background: #f5f5f5;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 14px;
        }
        .prose pre {
            background: #f5f5f5;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
        }
        .prose pre code {
            background: none;
            padding: 0;
        }
        .prose blockquote {
            border-left: 4px solid #333;
            margin: 1.5em 0;
            padding-left: 20px;
            color: #666;
            font-style: italic;
        }
        .prose a {
            color: #333;
            text-decoration: underline;
        }
        .prose strong {
            font-weight: 600;
        }
        .prose em {
            font-style: italic;
        }
        .chart {
            max-width: 1200px;
            margin: 30px auto;
        }
    </style>
    '''

    content = f'''{article_css}
<p class="timestamp">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

{body_content}
'''

    return front_matter + content


def generate_period_html(stats: Dict, figures: Dict[str, go.Figure],
                         period_type: str, period_info: Optional[Dict] = None) -> str:
    """Generate HTML for a period page with Jekyll front matter"""
    # Jekyll front matter
    if period_type == 'all':
        front_matter = "---\nlayout: dashboard\nperiod: all\ntitle: All Time Statistics\n---\n\n"
        period_title = "All Time Statistics"
    elif period_type == 'week':
        front_matter = f"---\nlayout: dashboard\nperiod: week\nperiod_id: {period_info['id']}\ntitle: CW {period_info['label']}\n---\n\n"
        period_title = f"Calendar Week {period_info['label']}"
    else:  # month
        front_matter = f"---\nlayout: dashboard\nperiod: month\nperiod_id: {period_info['id']}\ntitle: {period_info['label']}\n---\n\n"
        period_title = period_info['label']

    # Convert figures to HTML
    chart_html = {}
    for name, fig in figures.items():
        if fig is not None:
            chart_html[name] = fig.to_html(full_html=False, include_plotlyjs='cdn')
        else:
            chart_html[name] = '<p style="color: #999">Not enough data for visualization</p>'

    # Generate stats grid
    days = (datetime.fromisoformat(stats['last_search']) -
            datetime.fromisoformat(stats['first_search'])).days if stats['first_search'] and stats['last_search'] else 0

    stats_grid = f'''
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Searches</h3>
                <div class="value">{stats['total_searches']:,}</div>
                <div class="label">Deduplicated searches</div>
            </div>
            <div class="stat-card">
                <h3>Unique Users</h3>
                <div class="value">{stats['total_users']:,}</div>
                <div class="label">Anonymized users</div>
            </div>
            <div class="stat-card">
                <h3>Unique Queries</h3>
                <div class="value">{stats['total_queries']:,}</div>
                <div class="label">Different search terms</div>
            </div>
            <div class="stat-card">
                <h3>Avg Searches per User</h3>
                <div class="value">{stats['avg_searches_per_user']:.1f}</div>
                <div class="label">Including repeated searches</div>
            </div>
            <div class="stat-card">
                <h3>Avg Unique Queries per User</h3>
                <div class="value">{stats['avg_unique_queries_per_user']:.1f}</div>
                <div class="label">Search diversity</div>
            </div>
            <div class="stat-card">
                <h3>Period</h3>
                <div class="value">{days}</div>
                <div class="label">Days of data</div>
            </div>
        </div>
    '''

    # Page content (no Jekyll layout, just the content)
    content = f'''<h1>{period_title}</h1>

<p class="timestamp">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

<div style="background: #f5f5f5; padding: 20px; margin: 20px 0; border-left: 4px solid #333;">
    <h3 style="margin-top: 0; color: #333;">Data Deduplication Note</h3>
    <p style="margin-bottom: 0; color: #666;">
        Because Soulseek distributes searches across multiple clients, the same search from one user
        can appear on 2-3 of our geographic clients simultaneously. <strong>Analysis statistics are
        deduplicated within 5-minute windows</strong> to prevent inflated counts, keeping only the first
        occurrence of each username+query combination. However, <strong>data collection metrics show
        raw counts</strong> to accurately represent what each client receives from the network.
    </p>
</div>

<h2>Summary Statistics</h2>
{stats_grid}

<h2>Data Collection Overview <span style="font-weight: normal; font-size: 14px; color: #999;">(Raw Counts)</span></h2>
<div class="chart">
    {chart_html['daily_flow']}
</div>

<div class="chart">
    {chart_html.get('client_distribution', '<p>Not enough data</p>')}
</div>

<div class="chart">
    {chart_html.get('client_convergence', '<p>Not enough data</p>')}
</div>

<h2>User Activity Trends <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
<div class="chart">
    {chart_html.get('daily_unique_users', '<p>Not enough data</p>')}
</div>

<h2>Search Patterns Analysis <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
<div class="chart">
    {chart_html.get('top_queries', '<p>Not enough data</p>')}
</div>

<div class="chart">
    {chart_html.get('query_patterns', '<p>Not enough data</p>')}
</div>

<div class="chart">
    {chart_html.get('query_length', '<p>Not enough data</p>')}
</div>

<h2>Temporal Analysis <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
<div class="chart">
    {chart_html.get('temporal_hourly', '<p>Not enough data</p>')}
</div>

<h2>Search Term Analysis <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
<div class="chart">
    {chart_html.get('ngrams', '<p>Not enough data</p>')}
</div>

<div class="chart">
    {chart_html.get('cooccurrence', '<p>Not enough data for network visualization</p>')}
</div>

<h2>User Activity <span style="font-weight: normal; font-size: 14px; color: #999;">(Deduplicated)</span></h2>
<div class="chart">
    {chart_html.get('user_activity', '<p>Not enough data</p>')}
</div>
'''

    return front_matter + content


def main():
    """Main execution"""
    print("Connecting to database...")
    conn = get_db_connection()

    try:
        # Get available periods from database
        periods = get_available_periods(conn)

        print(f"Found {len(periods['months'])} months and {len(periods['weeks'])} weeks")

        # Generate Jekyll data files for navigation
        generate_jekyll_data_files(periods)
        print("✅ Generated Jekyll navigation data files")

        # TEMPORARILY SKIP all-time and monthly pages to avoid loading all 20M rows
        # TODO: Re-enable once we have more data or implement better pagination

        # Generate weekly pages (load data per week to avoid network timeout)
        for week in periods['weeks']:
            print("="*80)
            print(f"GENERATING WEEKLY PAGE: CW {week['label']}")
            print("="*80)
            print(f"  Loading data for period: {week['start']} to {week['end']}")
            week_raw = load_search_data(conn, start_date=week['start'], end_date=week['end'])
            week_dedup = deduplicate_dataframe(week_raw)
            generate_period_page_from_df(conn, week_raw, week_dedup, 'week', week)

        # Generate index page that redirects to most recent week
        if periods['weeks']:
            most_recent_week = periods['weeks'][0]  # weeks are sorted newest first
            index_content = f"""---
layout: default
title: Soulseek Research Dashboard
---

<script>
window.location.href = "week-{most_recent_week['year']}-{most_recent_week['week']:02d}.html";
</script>

<p>Redirecting to most recent week...</p>
<p>If not redirected, <a href="week-{most_recent_week['year']}-{most_recent_week['week']:02d}.html">click here</a>.</p>
"""
            with open(os.path.join(DOCS_DIR, 'index.html'), 'w') as f:
                f.write(index_content)
            print("✅ Generated index.html redirect to most recent week")

        print("\n" + "="*80)
        print(f"✅ Generated {len(periods['weeks'])} dashboard pages")
        print(f"   - {len(periods['weeks'])} weekly pages")
        print("="*80)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
