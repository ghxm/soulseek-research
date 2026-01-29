#!/usr/bin/env python3
"""
Generate statistics dashboard for Soulseek research data.
Queries the database and creates visualizations for GitHub Pages.
"""

import os
import re
import gc
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import psycopg2
import duckdb
import pandas as pd
import plotly.graph_objects as go
import mistune
import yaml


def format_days(first_search_str, last_search_str):
    """Calculate days of data from first/last search timestamps.

    Uses hours/24 for precision. Displays as integer when the
    1-decimal rounding is a whole number, otherwise as float.
    """
    if not first_search_str or not last_search_str:
        return "0"
    delta = datetime.fromisoformat(last_search_str) - datetime.fromisoformat(first_search_str)
    days_float = delta.total_seconds() / 86400
    days_rounded = round(days_float, 1)
    if days_rounded == int(days_rounded):
        return str(int(days_rounded))
    return f"{days_rounded:.1f}"


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
        dbname=host_db[1],
        connect_timeout=30,
        options='-c statement_timeout=600000'
    )


def load_data_into_duckdb(pg_conn, duckdb_conn, start_date=None, end_date=None, cutoff_date=None):
    """
    Load search data from PostgreSQL into DuckDB for fast analytics.

    Uses chunked loading to avoid memory issues, but DuckDB's columnar
    storage is much more memory-efficient than pandas.

    Args:
        cutoff_date: Hard upper bound to exclude incomplete days (e.g., current day).
    """
    # Apply cutoff as effective end date
    effective_end = cutoff_date or end_date
    if cutoff_date and end_date:
        effective_end = min(cutoff_date, end_date)

    date_filter = ""
    if start_date and effective_end:
        date_filter = f"WHERE timestamp >= '{start_date.isoformat()}' AND timestamp <= '{effective_end.isoformat()}'"
    elif effective_end:
        date_filter = f"WHERE timestamp <= '{effective_end.isoformat()}'"
    elif start_date:
        date_filter = f"WHERE timestamp >= '{start_date.isoformat()}'"
    else:
        # When loading all data, still filter to last 30 days for performance
        date_filter = "WHERE timestamp >= NOW() - INTERVAL '30 days'"

    query = f"""
        SELECT id, client_id, timestamp, username, query
        FROM searches
        {date_filter}
        ORDER BY timestamp
    """

    print(f"  Loading data from PostgreSQL into DuckDB...")
    print(f"    Query: SELECT id, client_id, timestamp, username, query FROM searches {date_filter}")

    # Create empty table in DuckDB
    duckdb_conn.execute("""
        CREATE TABLE searches_raw (
            id BIGINT,
            client_id VARCHAR,
            timestamp TIMESTAMP,
            username VARCHAR,
            query VARCHAR
        )
    """)

    # Load data in chunks using pandas as efficient transfer format
    # DuckDB has optimized pandas integration for fast bulk loading
    chunksize = 2_000_000  # 2M rows per chunk
    total_rows = 0
    chunk_num = 0

    for chunk_df in pd.read_sql_query(query, pg_conn, parse_dates=['timestamp'], chunksize=chunksize):
        chunk_num += 1
        total_rows += len(chunk_df)

        # Convert Pandas 3.0 string dtype to object for DuckDB compatibility
        # Pandas 3.0 uses pd.StringDtype() by default which DuckDB doesn't recognize
        for col in chunk_df.select_dtypes(include=['string']).columns:
            chunk_df[col] = chunk_df[col].astype(object)

        # Insert pandas DataFrame into DuckDB (VERY FAST - optimized C++ path)
        # DuckDB can read directly from pandas without Python overhead
        duckdb_conn.execute("INSERT INTO searches_raw SELECT * FROM chunk_df")

        print(f"    Loaded chunk {chunk_num}: {len(chunk_df):,} rows (total: {total_rows:,})")

        # Free pandas memory immediately after transfer to DuckDB
        del chunk_df
        gc.collect()

    print(f"  Loaded {total_rows:,} raw records into DuckDB")

    return total_rows


def deduplicate_in_duckdb(duckdb_conn):
    """
    Deduplicate searches within 5-minute windows using DuckDB SQL.

    This is 10-50x faster than pandas and uses minimal memory.
    Creates a new deduplicated table.
    """
    print(f"  Deduplicating data in DuckDB (5-minute windows)...")

    # Get count before deduplication
    raw_count = duckdb_conn.execute("SELECT COUNT(*) FROM searches_raw").fetchone()[0]

    # Deduplicate using SQL - much faster than pandas
    duckdb_conn.execute("""
        CREATE TABLE searches_dedup AS
        SELECT DISTINCT ON (username, query, FLOOR(EPOCH(timestamp) / 300))
            id, client_id, timestamp, username, query
        FROM searches_raw
        ORDER BY username, query, FLOOR(EPOCH(timestamp) / 300), timestamp
    """)

    # Get count after deduplication
    dedup_count = duckdb_conn.execute("SELECT COUNT(*) FROM searches_dedup").fetchone()[0]

    reduction_pct = 100 * (1 - dedup_count / raw_count) if raw_count > 0 else 0
    print(f"  Deduplicated: {raw_count:,} → {dedup_count:,} records ({reduction_pct:.1f}% reduction)")

    return raw_count, dedup_count


def get_archived_months(conn) -> List[Dict[str, Any]]:
    """Get list of archived months from database"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT month, file_path, record_count
        FROM archives
        WHERE deleted = FALSE
        ORDER BY month
    """)
    return [{'month': r[0], 'file_path': r[1], 'count': r[2]}
            for r in cursor.fetchall()]


def download_parquet_archives(pg_conn, local_archive_path: str) -> List[str]:
    """
    Download Parquet archive files from database server to local path.
    Returns list of local file paths.

    If DB_SERVER_SSH_KEY and DB_SERVER_IP are set, downloads via SCP.
    Otherwise assumes archives are already locally available.
    """
    os.makedirs(local_archive_path, exist_ok=True)

    archives = get_archived_months(pg_conn)
    if not archives:
        print("  No archives found in database")
        return []

    print(f"  Found {len(archives)} archived months")

    # Check if we need to download via SSH
    ssh_key_path = os.environ.get('DB_SERVER_SSH_KEY')
    db_server_ip = os.environ.get('DB_SERVER_IP')

    local_files = []

    if ssh_key_path and db_server_ip:
        print(f"  Downloading archives from {db_server_ip} via SCP...")

        for archive in archives:
            remote_path = archive['file_path']
            filename = os.path.basename(remote_path)
            local_path = os.path.join(local_archive_path, filename)

            # Skip if already downloaded
            if os.path.exists(local_path):
                print(f"    {filename} (cached)")
                local_files.append(local_path)
                continue

            # Download via SCP
            import subprocess
            scp_cmd = [
                'scp',
                '-i', ssh_key_path,
                '-o', 'StrictHostKeyChecking=no',
                f'root@{db_server_ip}:{remote_path}',
                local_path
            ]

            try:
                subprocess.run(scp_cmd, check=True, capture_output=True)
                print(f"    Downloaded {filename} ({archive['count']:,} records)")
                local_files.append(local_path)
            except subprocess.CalledProcessError as e:
                print(f"    Warning: Failed to download {filename}: {e}")

    else:
        print(f"  Using local archives from {local_archive_path}")
        for archive in archives:
            local_path = os.path.join(local_archive_path, os.path.basename(archive['file_path']))
            if os.path.exists(local_path):
                local_files.append(local_path)
            else:
                print(f"    Warning: Archive not found: {local_path}")

    return local_files


def load_parquet_archives_into_duckdb(duckdb_conn, parquet_files: List[str]):
    """
    Load Parquet archive files into DuckDB as a single unioned table.

    DuckDB can read Parquet files extremely efficiently - no need to
    load all data into memory. Just register the files and query them.
    """
    if not parquet_files:
        print("  No Parquet archives to load")
        return 0

    print(f"  Loading {len(parquet_files)} Parquet archives into DuckDB...")

    # DuckDB can read multiple Parquet files as a single table
    # This is VERY fast - it just registers the files, doesn't load everything
    parquet_pattern = "', '".join(parquet_files)

    # Create a view that unions all Parquet files
    # Note: Add NULL id column to match schema of live data table
    duckdb_conn.execute(f"""
        CREATE VIEW searches_archived AS
        SELECT NULL::BIGINT as id, client_id, timestamp, username, query
        FROM read_parquet(['{parquet_pattern}'])
    """)

    # Count total archived records
    archive_count = duckdb_conn.execute("SELECT COUNT(*) FROM searches_archived").fetchone()[0]
    print(f"  Loaded {archive_count:,} archived records (metadata only, not in memory)")

    return archive_count


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


def create_top_queries_chart(top_queries: List[tuple]) -> go.Figure:
    """Create bar chart of top queries"""
    queries = [q[0][:50] for q in top_queries[:100]]  # Truncate long queries
    unique_users = [q[1] for q in top_queries[:100]]  # q[1] is unique_users count

    fig = go.Figure(data=[
        go.Bar(
            y=queries[::-1],  # Reverse for better display
            x=unique_users[::-1],
            orientation='h',
            marker=dict(color='#333333', line=dict(color='black', width=1))
        )
    ])

    fig.update_layout(
        title='Top 100 Most Searched Queries (Unique Users)',
        xaxis_title='Number of Unique Users',
        yaxis_title='Query',
        height=2500,
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
    days = format_days(stats['first_search'], stats['last_search'])

    return f'''
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Search Events</h3>
                <div class="value">{stats['total_searches']:,}</div>
                <div class="label">Raw search requests received</div>
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


def get_available_periods(conn, max_date=None) -> Dict[str, List[Dict]]:
    """Get all available weeks and months from the database.

    Args:
        max_date: Cap for period generation (excludes incomplete day).
    """
    cursor = conn.cursor()

    # Get date range - simple query, no deduplication needed for date range
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM searches")
    min_date, db_max_date = cursor.fetchone()

    if not min_date or not db_max_date:
        return {'weeks': [], 'months': []}

    # Apply cap if provided (to exclude incomplete current day)
    effective_max = min(db_max_date, max_date) if max_date else db_max_date

    # Generate all weeks
    weeks = []
    current = min_date
    while current <= effective_max:
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
    while current <= effective_max:
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


def generate_period_page_with_duckdb(pg_conn, duckdb_conn, period_type: str, period_info: Optional[Dict] = None) -> str:
    """
    Generate a dashboard page for a specific period using DuckDB for analytics.

    pg_conn: PostgreSQL connection (for metadata if needed)
    duckdb_conn: DuckDB connection with 'searches' table loaded
    period_type: 'all', 'month', or 'week'
    period_info: Dict with 'id', 'label', 'start', 'end' (None for all-time)

    Note: Deduplication is done inline per-query using COUNT(DISTINCT ...) rather
    than creating a separate deduplicated table upfront.
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

    # Check if we have data
    record_count = duckdb_conn.execute("SELECT COUNT(*) FROM searches").fetchone()[0]
    if record_count == 0:
        print(f"  No data for {period_label}, skipping")
        return None

    print(f"  Computing statistics for {period_label}...")

    # Calculate summary statistics using DuckDB SQL (FAST!)
    # Uses inline deduplication via COUNT(DISTINCT ...) - no separate dedup table needed
    stats_query = duckdb_conn.execute("""
        SELECT
            COUNT(*) as total_searches,
            COUNT(DISTINCT username) as total_users,
            COUNT(DISTINCT LOWER(TRIM(query))) as total_queries,
            COUNT(DISTINCT (username || '|' || LOWER(TRIM(query)))) as unique_search_pairs,
            MIN(timestamp) as first_search,
            MAX(timestamp) as last_search
        FROM searches
    """).fetchone()

    total_searches, total_users, total_queries, unique_search_pairs, first_search, last_search = stats_query

    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = unique_search_pairs / total_users if total_users > 0 else 0

    # Per-client totals
    client_totals_df = duckdb_conn.execute("""
        SELECT client_id, COUNT(*) as count
        FROM searches
        GROUP BY client_id
    """).df()
    client_totals = dict(zip(client_totals_df['client_id'], client_totals_df['count']))

    stats = {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
        'unique_search_pairs': unique_search_pairs,
        'avg_searches_per_user': avg_searches_per_user,
        'avg_unique_queries_per_user': avg_unique_queries_per_user,
        'first_search': first_search.isoformat(),
        'last_search': last_search.isoformat(),
        'client_totals': client_totals
    }

    print(f"  Generating time-series data...")

    # Daily stats by client - DuckDB query returns small DataFrame
    daily_stats = duckdb_conn.execute("""
        SELECT
            client_id,
            CAST(timestamp AS DATE) as date,
            COUNT(*) as search_count
        FROM searches
        GROUP BY client_id, CAST(timestamp AS DATE)
        ORDER BY date, client_id
    """).df()

    # Daily unique users - DuckDB query returns small DataFrame
    daily_unique_users = duckdb_conn.execute("""
        SELECT
            CAST(timestamp AS DATE) as date,
            COUNT(DISTINCT username) as unique_users
        FROM searches
        GROUP BY CAST(timestamp AS DATE)
        ORDER BY date
    """).df()

    print(f"  Analyzing search patterns...")

    # Top queries (count once per user) - DuckDB query, only 100 rows returned
    print(f"  Computing top queries using DuckDB...")
    top_queries_df = duckdb_conn.execute("""
        SELECT
            LOWER(TRIM(query)) as query_normalized,
            COUNT(DISTINCT username) as unique_users,
            COUNT(*) as total_searches
        FROM searches
        GROUP BY LOWER(TRIM(query))
        ORDER BY unique_users DESC, total_searches DESC
        LIMIT 100
    """).df()

    top_queries = list(top_queries_df.itertuples(index=False, name=None))

    # Query length distribution - count unique queries per length bucket
    print(f"  Computing query length distribution using DuckDB...")
    query_length_data = duckdb_conn.execute("""
        SELECT
            LENGTH(query) - LENGTH(REPLACE(query, ' ', '')) + 1 as query_length,
            COUNT(DISTINCT LOWER(TRIM(query))) as count
        FROM searches
        WHERE LENGTH(query) - LENGTH(REPLACE(query, ' ', '')) + 1 <= 100
        GROUP BY query_length
        ORDER BY query_length
    """).df()

    print(f"  Found {total_searches:,} searches for {period_label}")
    print(f"  Creating visualizations...")

    # Create figures (lightweight charts only) - these work with small DataFrames
    figures = {
        'daily_flow': create_daily_flow_chart(daily_stats),
        'daily_unique_users': create_daily_unique_users_chart(daily_unique_users) if not daily_unique_users.empty else None,
        'client_distribution': create_client_distribution_chart(client_totals) if client_totals else None,
        'top_queries': create_top_queries_chart(top_queries) if top_queries else None,
        'query_length': create_query_length_chart(query_length_data) if not query_length_data.empty else None
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
    with open(output_file, 'w') as f:
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

    # Format date range for display
    if stats['first_search'] and stats['last_search']:
        first_dt = datetime.fromisoformat(stats['first_search'])
        last_dt = datetime.fromisoformat(stats['last_search'])
        date_range_str = f"{first_dt.strftime('%Y-%m-%d %H:%M')} to {last_dt.strftime('%Y-%m-%d %H:%M')} UTC"
    else:
        date_range_str = "No data"

    content = f'''{article_css}
<h1>All Time Statistics</h1>
<p class="period-range">{date_range_str}</p>

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
    days = format_days(stats['first_search'], stats['last_search'])

    stats_grid = f'''
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Search Events</h3>
                <div class="value">{stats['total_searches']:,}</div>
                <div class="label">Raw search requests received</div>
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

    # Format date range for display
    if stats['first_search'] and stats['last_search']:
        first_dt = datetime.fromisoformat(stats['first_search'])
        last_dt = datetime.fromisoformat(stats['last_search'])
        date_range_str = f"{first_dt.strftime('%Y-%m-%d %H:%M')} to {last_dt.strftime('%Y-%m-%d %H:%M')} UTC"
    else:
        date_range_str = "No data"

    # Page content (no Jekyll layout, just the content)
    content = f'''<h1>{period_title}</h1>
<p class="period-range">{date_range_str}</p>

<p class="timestamp">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

<div style="background: #f5f5f5; padding: 20px; margin: 20px 0; border-left: 4px solid #333;">
    <h3 style="margin-top: 0; color: #333;">Data Collection Note</h3>
    <p style="margin-bottom: 0; color: #666;">
        <strong>Total Search Events</strong> shows the raw count of search requests received by
        the research client. <strong>Unique Users</strong> and <strong>Unique Queries</strong>
        are counted distinctly to show actual network diversity. Top queries are ranked by
        <strong>unique users searching for that term</strong>, not raw event count.
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

<h2>User Activity Trends <span style="font-weight: normal; font-size: 14px; color: #999;">(Unique Users per Day)</span></h2>
<div class="chart">
    {chart_html.get('daily_unique_users', '<p>Not enough data</p>')}
</div>

<h2>Top Search Terms <span style="font-weight: normal; font-size: 14px; color: #999;">(Count once per user)</span></h2>
<div class="chart">
    {chart_html.get('top_queries', '<p>Not enough data</p>')}
</div>

<h2>Query Length Distribution <span style="font-weight: normal; font-size: 14px; color: #999;">(Unique Queries)</span></h2>
<div class="chart">
    {chart_html.get('query_length', '<p>Not enough data</p>')}
</div>

'''

    return front_matter + content


def main():
    """Main execution using DuckDB for fast analytics"""
    print("Connecting to PostgreSQL database...")
    try:
        pg_conn = get_db_connection()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        raise

    # Create DuckDB connection (in-memory for fast analytics)
    print("Creating DuckDB connection...")
    duck_conn = duckdb.connect(':memory:')

    # Calculate cutoff: end of yesterday UTC (exclude incomplete current day)
    # Pipeline runs at 3am, so current day only has partial data
    cutoff_date = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                   - timedelta(seconds=1))
    print(f"Data cutoff: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')} UTC (excluding incomplete current day)")

    try:
        # Get available periods from database
        periods = get_available_periods(pg_conn, max_date=cutoff_date)

        print(f"Found {len(periods['months'])} months and {len(periods['weeks'])} weeks")

        # Generate Jekyll data files for navigation
        generate_jekyll_data_files(periods)
        print("✅ Generated Jekyll navigation data files")

        # CHECK FOR AND LOAD PARQUET ARCHIVES
        print("="*80)
        print("CHECKING FOR ARCHIVED DATA")
        print("="*80)
        local_archive_path = os.environ.get('ARCHIVE_PATH', './archives')
        parquet_files = download_parquet_archives(pg_conn, local_archive_path)

        # Load archives into DuckDB (if any exist)
        archive_count = 0
        if parquet_files:
            archive_count = load_parquet_archives_into_duckdb(duck_conn, parquet_files)

        # LOAD LIVE DATA INTO DUCKDB
        print("="*80)
        print("LOADING LIVE DATA INTO DUCKDB")
        print("="*80)
        raw_count = load_data_into_duckdb(pg_conn, duck_conn, cutoff_date=cutoff_date)

        # If we have archives, union them with live data
        if archive_count > 0:
            print("  Combining archived and live data...")
            # Rename live tables temporarily
            duck_conn.execute("ALTER TABLE searches_raw RENAME TO searches_live_raw")

            # Create unified raw table (union of archived + live)
            duck_conn.execute("""
                CREATE TABLE searches_raw AS
                SELECT * FROM searches_archived
                UNION ALL
                SELECT * FROM searches_live_raw
            """)

            # Clean up
            duck_conn.execute("DROP TABLE searches_live_raw")
            duck_conn.execute("DROP VIEW searches_archived")

            total_raw = duck_conn.execute("SELECT COUNT(*) FROM searches_raw").fetchone()[0]
            print(f"  Combined: {archive_count:,} archived + {raw_count:,} live = {total_raw:,} total raw records")
            raw_count = total_raw

        # Rename table for simpler usage (no separate dedup table needed)
        # Deduplication is done inline per-query using COUNT(DISTINCT ...)
        duck_conn.execute("ALTER TABLE searches_raw RENAME TO searches")

        # Generate all-time page (use full dataset)
        print("="*80)
        print("GENERATING ALL-TIME PAGE")
        print("="*80)
        print(f"  Using full dataset: {raw_count:,} records")
        generate_period_page_with_duckdb(pg_conn, duck_conn, 'all', None)

        # Rename base table to avoid view name conflicts
        duck_conn.execute("ALTER TABLE searches RENAME TO searches_base")

        # Generate monthly pages (filter using SQL WHERE clauses)
        for month in periods['months']:
            print("="*80)
            print(f"GENERATING MONTHLY PAGE: {month['label']}")
            print("="*80)
            print(f"  Filtering data for period: {month['start']} to {month['end']}")

            # Create filtered view in DuckDB (fast SQL filtering)
            duck_conn.execute("DROP VIEW IF EXISTS searches")
            duck_conn.execute(f"""
                CREATE VIEW searches AS
                SELECT * FROM searches_base
                WHERE timestamp >= '{month['start'].isoformat()}'
                  AND timestamp <= '{month['end'].isoformat()}'
            """)

            month_count = duck_conn.execute("SELECT COUNT(*) FROM searches").fetchone()[0]
            print(f"  Filtered to {month_count:,} records")

            if month_count > 0:
                generate_period_page_with_duckdb(pg_conn, duck_conn, 'month', month)

        # Generate weekly pages (filter using SQL WHERE clauses)
        for week in periods['weeks']:
            print("="*80)
            print(f"GENERATING WEEKLY PAGE: CW {week['label']}")
            print("="*80)
            print(f"  Filtering data for period: {week['start']} to {week['end']}")

            # Create filtered view in DuckDB (fast SQL filtering)
            duck_conn.execute("DROP VIEW IF EXISTS searches")
            duck_conn.execute(f"""
                CREATE VIEW searches AS
                SELECT * FROM searches_base
                WHERE timestamp >= '{week['start'].isoformat()}'
                  AND timestamp <= '{week['end'].isoformat()}'
            """)

            week_count = duck_conn.execute("SELECT COUNT(*) FROM searches").fetchone()[0]
            print(f"  Filtered to {week_count:,} records")

            if week_count > 0:
                generate_period_page_with_duckdb(pg_conn, duck_conn, 'week', week)

        print("\n" + "="*80)
        print(f"✅ Generated {1 + len(periods['months']) + len(periods['weeks'])} dashboard pages")
        print(f"   - 1 all-time page")
        print(f"   - {len(periods['months'])} monthly pages")
        print(f"   - {len(periods['weeks'])} weekly pages")
        print("="*80)

    finally:
        pg_conn.close()
        duck_conn.close()


if __name__ == '__main__':
    main()
