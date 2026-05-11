#!/usr/bin/env python3
"""
Generate statistics dashboard for Soulseek research data.
Queries pre-computed materialized views and cumulative stats table.
"""

import fnmatch
import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import numpy as np
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import mistune
import yaml
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity


def format_days(first_search_str, last_search_str):
    """Calculate days of data from first/last search timestamps.

    Uses hours/24 for precision. Displays as integer when the
    1-decimal rounding is a whole number, otherwise as float.
    """
    if not first_search_str or not last_search_str:
        return "0"
    first = datetime.fromisoformat(str(first_search_str).replace('Z', '+00:00'))
    last = datetime.fromisoformat(str(last_search_str).replace('Z', '+00:00'))
    delta = last - first
    days_float = delta.total_seconds() / 86400
    days_rounded = round(days_float, 1)
    if days_rounded == int(days_rounded):
        return str(int(days_rounded))
    return f"{days_rounded:.1f}"


def parse_blacklist(raw: str) -> List[str]:
    """Parse blacklist patterns from environment variable (comma or newline separated).

    Returns a list of lowercase patterns suitable for fnmatch matching.
    Empty/whitespace-only entries are ignored.
    """
    if not raw or not raw.strip():
        return []
    # Support both comma and newline as separators
    entries = raw.replace('\n', ',').split(',')
    return [p.strip().lower() for p in entries if p.strip()]


def is_blacklisted(query: str, patterns: List[str]) -> bool:
    """Check if a query matches any blacklist pattern (case-insensitive).

    Uses fnmatch for wildcard matching: * matches anything, ? matches one char.
    """
    if not patterns:
        return False
    query_lower = query.lower()
    return any(fnmatch.fnmatch(query_lower, p) for p in patterns)


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
        options='-c statement_timeout=600000'
    )


def get_cumulative_stats(conn) -> Dict[str, Any]:
    """Get all-time stats from precomputed period_summary_stats."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT total_searches, total_users, unique_queries, unique_pairs, first_date, last_date
        FROM period_summary_stats
        WHERE period_type = 'all_time' AND period_id = 'all_time'
    """)
    row = cursor.fetchone()

    if row is None:
        cursor.close()
        return {
            'total_searches': 0,
            'total_users': 0,
            'total_queries': 0,
            'total_search_pairs': 0,
            'avg_searches_per_user': 0,
            'avg_unique_queries_per_user': 0,
            'first_search': None,
            'last_search': None,
            'client_totals': {}
        }

    total_searches, total_users, total_queries, total_pairs, first_date, last_date = row

    # Client totals: union archived + live daily client stats
    cursor.execute("""
        SELECT client_id, SUM(search_count)::bigint
        FROM (
            SELECT client_id, search_count FROM daily_client_stats
            UNION ALL
            SELECT client_id, search_count FROM mv_daily_stats
        ) combined
        GROUP BY client_id
    """)
    client_totals = {r[0]: int(r[1]) for r in cursor.fetchall()}
    cursor.close()

    total_searches = int(total_searches) if total_searches else 0
    total_users = int(total_users) if total_users else 0
    total_queries = int(total_queries) if total_queries else 0
    total_pairs = int(total_pairs) if total_pairs else 0

    avg_searches = total_searches / total_users if total_users > 0 else 0
    avg_queries = total_pairs / total_users if total_users > 0 else 0

    return {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
        'total_search_pairs': total_pairs,
        'avg_searches_per_user': avg_searches,
        'avg_unique_queries_per_user': avg_queries,
        'first_search': datetime.combine(first_date, datetime.min.time()).replace(tzinfo=timezone.utc).isoformat() if first_date else None,
        'last_search': datetime.combine(last_date, datetime.max.time()).replace(tzinfo=timezone.utc).isoformat() if last_date else None,
        'client_totals': client_totals,
    }



def get_daily_stats(conn, start_date=None, end_date=None) -> pd.DataFrame:
    """Get daily stats from archived + live data, optionally filtered by date range"""
    cursor = conn.cursor()

    base_query = """
        SELECT client_id, date, search_count, unique_users FROM daily_client_stats
        UNION ALL
        SELECT client_id, date, search_count, unique_users FROM mv_daily_stats
    """

    if start_date and end_date:
        cursor.execute(f"""
            SELECT client_id, date, search_count, unique_users
            FROM ({base_query}) combined
            WHERE date >= %s AND date <= %s
            ORDER BY date, client_id
        """, (start_date.date(), end_date.date()))
    elif end_date:
        cursor.execute(f"""
            SELECT client_id, date, search_count, unique_users
            FROM ({base_query}) combined
            WHERE date <= %s
            ORDER BY date, client_id
        """, (end_date.date(),))
    else:
        cursor.execute(f"""
            SELECT client_id, date, search_count, unique_users
            FROM ({base_query}) combined
            ORDER BY date, client_id
        """)

    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return pd.DataFrame(columns=['client_id', 'date', 'search_count', 'unique_users'])

    return pd.DataFrame(rows, columns=['client_id', 'date', 'search_count', 'unique_users'])


def get_daily_unique_users(conn, start_date=None, end_date=None) -> pd.DataFrame:
    """Get daily unique users from archived + live data (approximation - max across clients per day)"""
    cursor = conn.cursor()

    base_query = """
        SELECT client_id, date, search_count, unique_users FROM daily_client_stats
        UNION ALL
        SELECT client_id, date, search_count, unique_users FROM mv_daily_stats
    """

    if start_date and end_date:
        cursor.execute(f"""
            SELECT date, MAX(unique_users) as unique_users
            FROM ({base_query}) combined
            WHERE date >= %s AND date <= %s
            GROUP BY date
            ORDER BY date
        """, (start_date.date(), end_date.date()))
    elif end_date:
        cursor.execute(f"""
            SELECT date, MAX(unique_users) as unique_users
            FROM ({base_query}) combined
            WHERE date <= %s
            GROUP BY date
            ORDER BY date
        """, (end_date.date(),))
    else:
        cursor.execute(f"""
            SELECT date, MAX(unique_users) as unique_users
            FROM ({base_query}) combined
            GROUP BY date
            ORDER BY date
        """)

    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return pd.DataFrame(columns=['date', 'unique_users'])

    return pd.DataFrame(rows, columns=['date', 'unique_users'])


def get_top_queries(conn) -> List[tuple]:
    """Get all-time top queries from precomputed period_top_queries."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_normalized, unique_users, total_searches
        FROM period_top_queries
        WHERE period_type = 'all_time' AND period_id = 'all_time'
        ORDER BY rank
    """)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def get_query_length_distribution(conn) -> pd.DataFrame:
    """Get all-time query length distribution from precomputed period table."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_length, unique_query_count
        FROM period_query_length_dist
        WHERE period_type = 'all_time' AND period_id = 'all_time'
        ORDER BY query_length
    """)
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return pd.DataFrame(columns=['query_length', 'count'])

    return pd.DataFrame(rows, columns=['query_length', 'count'])


def get_period_stats(conn, start_date, end_date, period_type: str = None, period_id: str = None) -> Dict[str, Any]:
    """Get stats for a specific period using archived + live daily stats"""
    cursor = conn.cursor()

    base_query = """
        SELECT client_id, date, search_count, unique_users FROM daily_client_stats
        UNION ALL
        SELECT client_id, date, search_count, unique_users FROM mv_daily_stats
    """

    # Use archived + live daily stats for fast aggregation
    cursor.execute(f"""
        SELECT
            COALESCE(SUM(search_count), 0) as total_searches,
            COALESCE(SUM(unique_users), 0) as total_users,
            MIN(date) as first_date,
            MAX(date) as last_date
        FROM ({base_query}) combined
        WHERE date >= %s AND date <= %s
    """, (start_date.date(), end_date.date()))

    result = cursor.fetchone()

    if result is None or result[0] == 0:
        cursor.close()
        return None

    total_searches, total_users, first_date, last_date = result
    total_searches = int(total_searches) if total_searches else 0
    total_users = int(total_users) if total_users else 0

    # Get per-client totals from archived + live daily stats
    cursor.execute(f"""
        SELECT client_id, SUM(search_count) as count
        FROM ({base_query}) combined
        WHERE date >= %s AND date <= %s
        GROUP BY client_id
    """, (start_date.date(), end_date.date()))
    client_totals = {row[0]: int(row[1]) for row in cursor.fetchall()}

    cursor.close()

    # Get precomputed stats from period_summary_stats (correct distinct counts)
    summary_row = None
    if period_type and period_id:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT unique_queries, unique_pairs, total_searches, total_users
            FROM period_summary_stats
            WHERE period_type = %s AND period_id = %s
        """, (period_type, period_id))
        summary_row = cursor.fetchone()
        cursor.close()

    if summary_row:
        total_queries = int(summary_row[0])
        total_pairs = int(summary_row[1])
        # Override total_searches and total_users from precomputed stats
        # (the UNION-based SUM(unique_users) is wrong: it sums per-client-per-day
        # counts, massively overcounting users who appear across days/clients)
        if summary_row[2] is not None:
            total_searches = int(summary_row[2])
        if summary_row[3] is not None:
            total_users = int(summary_row[3])
    else:
        total_queries = int(total_users * 0.6) if total_users > 0 else 0
        total_pairs = total_users

    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = total_pairs / total_users if total_users > 0 else 0

    # Convert dates to timestamps for display
    first_search = datetime.combine(first_date, datetime.min.time()).replace(tzinfo=timezone.utc) if first_date else None
    last_search = datetime.combine(last_date, datetime.max.time()).replace(tzinfo=timezone.utc) if last_date else None

    return {
        'total_searches': int(total_searches),
        'total_users': int(total_users),
        'total_queries': total_queries,
        'total_search_pairs': total_pairs,
        'avg_searches_per_user': avg_searches_per_user,
        'avg_unique_queries_per_user': avg_unique_queries_per_user,
        'first_search': first_search.isoformat() if first_search else None,
        'last_search': last_search.isoformat() if last_search else None,
        'client_totals': client_totals
    }


def get_period_top_queries(conn, period_type: str, period_id: str) -> List[tuple]:
    """Get top queries for a specific period from precomputed table.

    Args:
        conn: Database connection
        period_type: 'week' or 'month'
        period_id: Period identifier like '2026-01' or '2026-W04'

    Returns:
        List of (query_normalized, unique_users, total_searches) tuples
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_normalized, unique_users, total_searches
        FROM period_top_queries
        WHERE period_type = %s AND period_id = %s
        ORDER BY rank
    """, (period_type, period_id))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def get_period_query_length_dist(conn, period_type: str, period_id: str) -> pd.DataFrame:
    """Get query length distribution for a specific period from precomputed table.

    Args:
        conn: Database connection
        period_type: 'week' or 'month'
        period_id: Period identifier like '2026-01' or '2026-W04'

    Returns:
        DataFrame with query_length and count columns
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_length, unique_query_count as count
        FROM period_query_length_dist
        WHERE period_type = %s AND period_id = %s
        ORDER BY query_length
    """, (period_type, period_id))
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return pd.DataFrame(columns=['query_length', 'count'])

    return pd.DataFrame(rows, columns=['query_length', 'count'])


def load_article_content(article_path: str = 'docs/article.md') -> Optional[str]:
    """Load article Markdown content if it exists."""
    if os.path.exists(article_path):
        with open(article_path, 'r', encoding='utf-8') as f:
            return f.read()
    return None


def parse_article_sections(markdown_content: str) -> List[Dict[str, Any]]:
    """Parse Markdown into sections with chart markers identified."""
    sections = []
    pattern = r'(<!--\s*chart:\s*(\w+)\s*-->|<!--\s*stats-grid\s*-->|<!--\s*cumulative-stats\s*-->)'
    parts = re.split(pattern, markdown_content)
    md_parser = mistune.create_markdown()

    i = 0
    while i < len(parts):
        part = parts[i] if i < len(parts) else ''
        if part and part.strip():
            if '<!-- chart:' in part:
                if i + 1 < len(parts):
                    chart_id = parts[i + 1].strip()
                    sections.append({'type': 'chart', 'chart_id': chart_id})
                    i += 2
                else:
                    i += 1
            elif '<!-- stats-grid -->' in part:
                sections.append({'type': 'stats-grid'})
                i += 1
            elif '<!-- cumulative-stats -->' in part:
                sections.append({'type': 'cumulative-stats'})
                i += 1
            else:
                html = md_parser(part)
                if html.strip():
                    sections.append({'type': 'prose', 'content': html})
                i += 1
        else:
            i += 1
    return sections


def slugify_query(query: str) -> str:
    """Convert a query string to a URL-safe slug with hash for uniqueness.

    Example: "Pink Floyd" -> "pink-floyd-7a8c2e1d"
    """
    # Lowercase, replace non-alphanumeric with hyphens, collapse multiple hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', query.lower()).strip('-')
    # Truncate long slugs
    if len(slug) > 80:
        slug = slug[:80].rstrip('-')
    # Append 8-char hash for uniqueness
    hash_suffix = hashlib.sha256(query.encode('utf-8')).hexdigest()[:8]
    return f"{slug}-{hash_suffix}" if slug else hash_suffix


def create_svg_line_chart(daily_data: List[tuple]) -> str:
    """Create an inline SVG line chart with dual Y-axes for searches and unique users.

    Args:
        daily_data: List of (date, search_count, unique_users) tuples, sorted by date

    Returns:
        SVG string
    """
    if not daily_data or len(daily_data) < 2:
        return '<p style="color: #999;">Not enough data for chart</p>'

    # Chart dimensions - extra right padding for second Y-axis
    width, height = 800, 320
    pad_left, pad_right, pad_top, pad_bottom = 60, 60, 40, 50

    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom

    dates = [d[0] for d in daily_data]
    counts = [d[1] for d in daily_data]
    users = [d[2] for d in daily_data]
    max_count = max(counts) if counts else 1
    max_users = max(users) if users else 1
    n = len(dates)

    search_color = '#333'
    user_color = '#b04040'

    # Scale functions
    def x_pos(i):
        return pad_left + (i / max(n - 1, 1)) * plot_w

    def y_count(val):
        return pad_top + plot_h - (val / max(max_count, 1)) * plot_h

    def y_user(val):
        return pad_top + plot_h - (val / max(max_users, 1)) * plot_h

    # Build search line path
    search_pts = [(x_pos(i), y_count(counts[i])) for i in range(n)]
    search_line = 'M ' + ' L '.join(f'{x:.1f},{y:.1f}' for x, y in search_pts)
    search_area = search_line + f' L {search_pts[-1][0]:.1f},{pad_top + plot_h:.1f} L {search_pts[0][0]:.1f},{pad_top + plot_h:.1f} Z'

    # Build users line path
    user_pts = [(x_pos(i), y_user(users[i])) for i in range(n)]
    user_line = 'M ' + ' L '.join(f'{x:.1f},{y:.1f}' for x, y in user_pts)

    # X-axis labels (show ~8 labels evenly spaced)
    x_labels = []
    label_count = min(8, n)
    for i in range(label_count):
        idx = int(i * (n - 1) / max(label_count - 1, 1))
        x = x_pos(idx)
        label = dates[idx].strftime('%b %d') if hasattr(dates[idx], 'strftime') else str(dates[idx])
        x_labels.append(f'<text x="{x:.1f}" y="{height - 5}" text-anchor="middle" font-size="11" fill="#999">{label}</text>')

    # Left Y-axis labels (searches, 5 ticks)
    y_labels = []
    for i in range(5):
        val = int(max_count * i / 4)
        y = y_count(val)
        y_labels.append(f'<text x="{pad_left - 8}" y="{y:.1f}" text-anchor="end" font-size="11" fill="{search_color}" dominant-baseline="middle">{val:,}</text>')
        y_labels.append(f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width - pad_right}" y2="{y:.1f}" stroke="#eee" stroke-width="1"/>')

    # Right Y-axis labels (users, 5 ticks)
    for i in range(5):
        val = int(max_users * i / 4)
        y = y_user(val)
        y_labels.append(f'<text x="{width - pad_right + 8}" y="{y:.1f}" text-anchor="start" font-size="11" fill="{user_color}" dominant-baseline="middle">{val:,}</text>')

    # Legend (top of chart)
    legend = (
        f'<line x1="{pad_left}" y1="12" x2="{pad_left + 20}" y2="12" stroke="{search_color}" stroke-width="2"/>'
        f'<text x="{pad_left + 25}" y="12" font-size="11" fill="{search_color}" dominant-baseline="middle">Searches</text>'
        f'<line x1="{pad_left + 100}" y1="12" x2="{pad_left + 120}" y2="12" stroke="{user_color}" stroke-width="2" stroke-dasharray="4,3"/>'
        f'<text x="{pad_left + 125}" y="12" font-size="11" fill="{user_color}" dominant-baseline="middle">Unique users</text>'
    )

    # Hover-sensitive overlay circles (larger hit area, both values in tooltip)
    hover_circles = []
    for i in range(n):
        x = x_pos(i)
        # Place circle on search line
        _, y_s = search_pts[i]
        date_str = dates[i].strftime('%Y-%m-%d') if hasattr(dates[i], 'strftime') else str(dates[i])
        hover_circles.append(
            f'<circle cx="{x:.1f}" cy="{y_s:.1f}" r="8" fill="transparent" style="cursor:pointer">'
            f'<title>{date_str}: {counts[i]:,} searches, {users[i]:,} unique users</title></circle>'
        )

    svg = f'''<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:auto;max-width:{width}px;">
  {legend}
  {''.join(y_labels)}
  <path d="{search_area}" fill="rgba(51,51,51,0.06)" stroke="none"/>
  <path d="{search_line}" fill="none" stroke="{search_color}" stroke-width="2"/>
  <path d="{user_line}" fill="none" stroke="{user_color}" stroke-width="1.5" stroke-dasharray="4,3"/>
  {''.join(x_labels)}
  {''.join(hover_circles)}
</svg>'''

    return svg


def create_daily_flow_chart(df: pd.DataFrame) -> go.Figure:
    """Create line chart showing daily search flow per client"""
    fig = go.Figure()
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
        xaxis=dict(rangeslider=dict(visible=True), type='date')
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
        xaxis=dict(rangeslider=dict(visible=True), type='date')
    )
    return fig


def create_top_queries_chart(top_queries: List[tuple], limit: int = 25) -> go.Figure:
    """Create interactive bar chart of top N queries for performance.

    Args:
        top_queries: List of (query, unique_users, total_searches) tuples
        limit: Maximum number of queries to show in chart (default 25)

    Returns:
        Plotly figure with top N queries
    """
    total_count = len(top_queries)
    queries = [q[0] for q in top_queries[:limit]]
    unique_users = [q[1] for q in top_queries[:limit]]

    chart_height = max(400, limit * 25)

    fig = go.Figure(data=[
        go.Bar(
            y=queries[::-1],  # Reverse for top-to-bottom display
            x=unique_users[::-1],
            orientation='h',
            marker=dict(color='#333333', line=dict(color='black', width=1)),
            customdata=queries[::-1],  # Store full query for search
            hovertemplate='<b>%{customdata}</b><br>Unique Users: %{x}<extra></extra>'
        )
    ])

    title_text = f'Top {limit:,} Queries (of {total_count:,} total with 5+ users)'

    fig.update_layout(
        title=title_text,
        xaxis_title='Number of Unique Users',
        yaxis_title='Query',
        height=chart_height,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
        yaxis=dict(
            tickmode='linear',
            tickfont=dict(size=10)
        )
    )
    return fig


def write_queries_db_file(top_queries: List[tuple], db_dir: str,
                          data_file_id: str,
                          query_slug_map: Dict[str, str] = None):
    """Write query data to a chunked SQLite database for sql.js-httpvfs.

    Produces chunk files (queries_{id}.db.000, .001, ...) and a config JSON
    that sql.js-httpvfs uses to serve the database via HTTP Range requests.

    Args:
        top_queries: List of (query, unique_users, total_searches) tuples
        db_dir: Directory to write files into (e.g. 'docs/data')
        data_file_id: Period identifier (e.g. 'all', '2026-01', '2026-W03')
        query_slug_map: Optional dict mapping query_normalized -> slug
    """
    import sqlite3 as sqlite3_mod
    import tempfile
    import glob as glob_mod

    os.makedirs(db_dir, exist_ok=True)
    prefix = f'queries_{data_file_id}'

    # Remove old chunk files for this period
    for old_file in glob_mod.glob(os.path.join(db_dir, f'{prefix}.db.*')):
        os.remove(old_file)

    # Build SQLite in a temp file
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        tmp_path = tmp.name

    try:
        conn = sqlite3_mod.connect(tmp_path)
        conn.execute('PRAGMA page_size = 4096')
        conn.execute('PRAGMA journal_mode = DELETE')

        conn.execute('''CREATE TABLE queries (
            rank INTEGER PRIMARY KEY,
            query TEXT NOT NULL,
            unique_users INTEGER NOT NULL,
            total_searches INTEGER NOT NULL,
            slug TEXT
        )''')

        rows = []
        for i, (query, users, searches) in enumerate(top_queries, 1):
            slug = query_slug_map.get(query) if query_slug_map else None
            rows.append((i, query, users, searches, slug))
        conn.executemany('INSERT INTO queries VALUES (?, ?, ?, ?, ?)', rows)

        # Word index for prefix search via B-tree (works with any SQLite build,
        # unlike FTS5 which fails with sql.js-httpvfs's HTTP VFS)
        conn.execute('CREATE TABLE word_index (word TEXT NOT NULL, rank INTEGER NOT NULL)')
        word_rows = []
        for i, (query, _, _) in enumerate(top_queries, 1):
            for w in set(query.lower().split()):
                if w:
                    word_rows.append((w, i))
        conn.executemany('INSERT INTO word_index VALUES (?, ?)', word_rows)
        conn.execute('CREATE INDEX idx_word ON word_index(word)')

        conn.commit()
        conn.execute('VACUUM')
        conn.close()

        # Split into chunks for GitHub Pages compatibility
        config = _split_db_file(tmp_path, db_dir, prefix)

        # Write config JSON for sql.js-httpvfs
        config_path = os.path.join(db_dir, f'{prefix}_config.json')
        with open(config_path, 'w') as f:
            json.dump(config, f, separators=(',', ':'))

        db_size = os.path.getsize(tmp_path)
        n_chunks = len(glob_mod.glob(os.path.join(db_dir, f'{prefix}.db.*')))
        print(f"  SQLite: {db_size / 1024 / 1024:.1f} MB, {n_chunks} chunks, {len(top_queries):,} rows")
    finally:
        os.unlink(tmp_path)


def _split_db_file(db_path: str, output_dir: str, prefix: str,
                   chunk_size: int = 10 * 1024 * 1024) -> dict:
    """Split a SQLite file into chunks and return sql.js-httpvfs config."""
    file_size = os.path.getsize(db_path)
    suffix_length = 3
    chunk_num = 0

    with open(db_path, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            chunk_filename = f'{prefix}.db.{str(chunk_num).zfill(suffix_length)}'
            with open(os.path.join(output_dir, chunk_filename), 'wb') as cf:
                cf.write(data)
            chunk_num += 1

    return {
        'serverMode': 'chunked',
        'requestChunkSize': 4096,
        'databaseLengthBytes': file_size,
        'serverChunkSize': chunk_size,
        'urlPrefix': f'{prefix}.db.',
        'suffixLength': suffix_length,
    }


def create_queries_data_table(top_queries: List[tuple], db_config_url: str) -> str:
    """Create a searchable HTML table backed by sql.js-httpvfs.

    Uses HTTP Range requests to query a SQLite database, fetching only the
    bytes needed for each search instead of downloading the full dataset.

    Args:
        top_queries: List of (query, unique_users, total_searches) tuples (for count only)
        db_config_url: URL path to the sql.js-httpvfs config JSON

    Returns:
        HTML string with interactive table and search
    """
    total_count = len(top_queries)

    return f'''
    <div class="queries-table-container">
        <div class="table-header">
            <h3>Complete Query Dataset ({total_count:,} queries)</h3>
            <div class="table-controls">
                <input
                    type="text"
                    id="table_search"
                    class="table-search-input"
                    placeholder="Search queries (e.g. radiohead, pink floyd...)"
                    autocomplete="off"
                    disabled
                />
            </div>
            <div class="search-results" id="search_results">
                Loading query engine...
            </div>
        </div>

        <div class="table-scroll-container">
            <table class="queries-table">
                <thead>
                    <tr>
                        <th class="rank-col">Rank</th>
                        <th class="query-col">Query</th>
                        <th class="users-col">Unique Users</th>
                        <th class="searches-col">Total Searches</th>
                    </tr>
                </thead>
                <tbody id="content_area">
                    <tr><td colspan="4" class="loading-cell">Loading query engine...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
    (function() {{
        var baseUrl = '{{{{ site.baseurl }}}}';
        var configUrl = baseUrl + '/{db_config_url}';
        var workerUrl = baseUrl + '/assets/sqljs/sqlite.worker.js';
        var wasmUrl = baseUrl + '/assets/sqljs/sql-wasm.wasm';
        var totalCount = {total_count};

        var searchInput = document.getElementById('table_search');
        var resultsDiv = document.getElementById('search_results');
        var tbody = document.getElementById('content_area');
        var db = null;
        var debounceTimer = null;

        function escapeHtml(s) {{
            return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }}

        function renderRows(rows) {{
            if (!rows || rows.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="4" class="loading-cell">No results found</td></tr>';
                return;
            }}
            var html = '';
            for (var i = 0; i < rows.length; i++) {{
                var r = rows[i];
                var queryEscaped = escapeHtml(r.query);
                var queryCell = r.slug
                    ? '<a href="' + baseUrl + '/queries/' + r.slug + '.html" class="query-link">' + queryEscaped + '</a>'
                    : queryEscaped;
                html += '<tr>' +
                    '<td class="rank-col">' + r.rank + '</td>' +
                    '<td class="query-col">' + queryCell + '</td>' +
                    '<td class="users-col">' + r.unique_users.toLocaleString() + '</td>' +
                    '<td class="searches-col">' + r.total_searches.toLocaleString() + '</td>' +
                    '</tr>';
            }}
            tbody.innerHTML = html;
        }}

        function loadTop() {{
            return db.query(
                'SELECT rank, query, unique_users, total_searches, slug FROM queries ORDER BY rank LIMIT 100'
            ).then(function(rows) {{
                renderRows(rows);
                resultsDiv.textContent = 'Showing top 100 of ' + totalCount.toLocaleString() + ' queries';
            }});
        }}

        function search(term) {{
            var words = term.toLowerCase().trim().split(/\\s+/).filter(Boolean);
            if (words.length === 0) return loadTop();

            // Build query: intersect word_index lookups per word (prefix match via B-tree)
            var conditions = words.map(function() {{
                return 'q.rank IN (SELECT rank FROM word_index WHERE word LIKE ?)';
            }});
            var params = words.map(function(w) {{ return w + '%'; }});
            var sql = 'SELECT q.rank, q.query, q.unique_users, q.total_searches, q.slug ' +
                'FROM queries q WHERE ' + conditions.join(' AND ') +
                ' ORDER BY q.unique_users DESC LIMIT 200';

            resultsDiv.textContent = 'Searching...';
            return db.query(sql, params).then(function(rows) {{
                renderRows(rows);
                var msg = rows.length >= 200
                    ? 'Showing first 200 matches'
                    : 'Showing ' + rows.length.toLocaleString() + ' matching queries';
                resultsDiv.textContent = msg;
            }}).catch(function() {{
                // Query error (e.g. special characters) -- fall back to top
                loadTop();
            }});
        }}

        // Initialize: reuse nav worker for all-time db, or create new one for period db
        var isAllTime = configUrl.indexOf('queries_all') !== -1;
        var dbReady = (isAllTime && window._queryDb)
            ? window._queryDb
            : fetch(configUrl)
                .then(function(r) {{ return r.json(); }})
                .then(function(config) {{
                    config.urlPrefix = baseUrl + '/data/' + config.urlPrefix;
                    return SqliteHttpvfs.createDbWorker(
                        [{{ from: "inline", config: config }}],
                        workerUrl,
                        wasmUrl
                    );
                }})
                .then(function(worker) {{ return worker.db; }});

        dbReady.then(function(readyDb) {{
                db = readyDb;
                searchInput.disabled = false;
                return loadTop();
            }})
            .catch(function(err) {{
                resultsDiv.textContent = 'Failed to load query database';
                console.error('sql.js-httpvfs init error:', err);
            }});

        searchInput.addEventListener('input', function() {{
            if (!db) return;
            clearTimeout(debounceTimer);
            var term = this.value.trim();
            debounceTimer = setTimeout(function() {{
                if (!term || term.length < 2) {{
                    if (!term) loadTop();
                    else resultsDiv.textContent = 'Type at least 2 characters to search';
                    return;
                }}
                search(term);
            }}, 300);
        }});
    }})();
    </script>

    <style>
        .queries-table-container {{
            margin: 30px 0;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: white;
        }}

        .table-header {{
            padding: 20px;
            background: #f5f5f5;
            border-bottom: 1px solid #ddd;
        }}

        .table-header h3 {{
            margin: 0 0 15px 0;
            color: #333;
            font-size: 18px;
        }}

        .table-controls {{
            display: flex;
            gap: 10px;
            margin-bottom: 10px;
        }}

        .table-search-input {{
            flex: 1;
            padding: 10px 15px;
            font-size: 15px;
            border: 2px solid #333;
            border-radius: 4px;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
        }}

        .table-search-input:focus {{
            outline: none;
            border-color: #000;
            background: #fff;
        }}

        .table-search-input:disabled {{
            background: #f0f0f0;
            color: #999;
            cursor: wait;
        }}

        .search-results {{
            font-size: 14px;
            color: #666;
            font-style: italic;
        }}

        .loading-cell {{
            text-align: center;
            padding: 40px 15px;
            color: #999;
            font-style: italic;
        }}

        .table-scroll-container {{
            max-height: 600px;
            overflow-y: auto;
            position: relative;
        }}

        .queries-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}

        .queries-table thead {{
            position: sticky;
            top: 0;
            background: white;
            z-index: 10;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .queries-table th {{
            padding: 12px 15px;
            text-align: left;
            font-weight: 600;
            color: #333;
            border-bottom: 2px solid #333;
        }}

        .queries-table td {{
            padding: 10px 15px;
            border-bottom: 1px solid #eee;
        }}

        .queries-table tbody tr:hover {{
            background: #f9f9f9;
        }}

        .rank-col {{
            width: 80px;
            text-align: right;
            color: #999;
        }}

        .query-col {{
            min-width: 300px;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
        }}

        .query-link {{
            color: #333;
            text-decoration: none;
            border-bottom: 1px solid #ccc;
        }}

        .query-link:hover {{
            color: #000;
            border-color: #333;
        }}

        .users-col, .searches-col {{
            width: 120px;
            text-align: right;
            font-variant-numeric: tabular-nums;
        }}
    </style>
    '''


def create_query_length_chart(df: pd.DataFrame) -> go.Figure:
    """Create histogram of query length distribution (by word count)"""
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
    """Get all available weeks and months from period_summary_stats.

    Derives periods from precomputed stats rather than the searches table,
    so archived months/weeks (deleted from searches) are still included.
    """
    cursor = conn.cursor()

    # Get weeks from period_summary_stats
    cursor.execute("""
        SELECT period_id, first_date, last_date
        FROM period_summary_stats
        WHERE period_type = 'week'
        ORDER BY period_id
    """)
    weeks = []
    for period_id, first_date, last_date in cursor.fetchall():
        # period_id is like "2026-W03"
        parts = period_id.split('-W')
        iso_year, iso_week = int(parts[0]), int(parts[1])
        week_label = f"{iso_week:02d}/{iso_year}"
        week_start = datetime.fromisocalendar(iso_year, iso_week, 1).replace(tzinfo=timezone.utc)
        week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        weeks.append({'id': period_id, 'label': week_label, 'start': week_start, 'end': week_end})

    # Get months from period_summary_stats
    cursor.execute("""
        SELECT period_id, first_date, last_date
        FROM period_summary_stats
        WHERE period_type = 'month'
        ORDER BY period_id
    """)
    months = []
    for period_id, first_date, last_date in cursor.fetchall():
        # period_id is like "2026-01"
        year, month = int(period_id[:4]), int(period_id[5:7])
        month_start = datetime(year, month, 1, tzinfo=timezone.utc)
        month_label = month_start.strftime('%B %Y')
        if month == 12:
            next_month = month_start.replace(year=year + 1, month=1)
        else:
            next_month = month_start.replace(month=month + 1)
        month_end = next_month - timedelta(seconds=1)
        months.append({'id': period_id, 'label': month_label, 'start': month_start, 'end': month_end})

    cursor.close()

    # Filter out periods that start after max_date if specified
    if max_date:
        weeks = [w for w in weeks if w['start'] <= max_date]
        months = [m for m in months if m['start'] <= max_date]

    return {'weeks': weeks, 'months': months}


def generate_jekyll_data_files(periods: Dict[str, List[Dict]]):
    """Generate Jekyll _data files for navigation"""
    os.makedirs('docs/_data', exist_ok=True)

    months_data = [{'id': m['id'], 'label': m['label']} for m in reversed(periods['months'])]
    with open('docs/_data/months.yml', 'w', encoding='utf-8') as f:
        yaml.dump(months_data, f, default_flow_style=False, allow_unicode=True)

    weeks_data = [{'id': w['id'], 'label': w['label']} for w in reversed(periods['weeks'])]
    with open('docs/_data/weeks.yml', 'w', encoding='utf-8') as f:
        yaml.dump(weeks_data, f, default_flow_style=False, allow_unicode=True)


def generate_all_time_page(conn, cutoff_date=None, query_slug_map=None, blacklist=None) -> str:
    """Generate the all-time dashboard page using cumulative stats + materialized views"""
    print("  Computing all-time statistics from cumulative + live data...")

    # Get cumulative stats (archived + live combined)
    stats = get_cumulative_stats(conn)

    if stats['total_searches'] == 0:
        print("  No data available, skipping all-time page")
        return None

    # Cap last_search at cutoff_date (exclude incomplete current day)
    if cutoff_date and stats['last_search']:
        cutoff_iso = cutoff_date.isoformat()
        if stats['last_search'] > cutoff_iso:
            stats['last_search'] = (cutoff_date - timedelta(seconds=1)).isoformat()

    print(f"  Total all-time: {stats['total_searches']:,} searches, {stats['total_users']:,} users")

    # Get chart data from materialized views (exclude incomplete current day)
    print("  Loading chart data from materialized views...")
    daily_stats = get_daily_stats(conn, end_date=cutoff_date)
    daily_unique_users = get_daily_unique_users(conn, end_date=cutoff_date)
    top_queries = get_top_queries(conn)
    if blacklist:
        top_queries = [q for q in top_queries if not is_blacklisted(q[0], blacklist)]
    query_length_dist = get_query_length_distribution(conn)

    # Create figures
    figures = {
        'daily_flow': create_daily_flow_chart(daily_stats) if not daily_stats.empty else None,
        'daily_unique_users': create_daily_unique_users_chart(daily_unique_users) if not daily_unique_users.empty else None,
        'client_distribution': create_client_distribution_chart(stats['client_totals']) if stats['client_totals'] else None,
        'top_queries': create_top_queries_chart(top_queries) if top_queries else None,
        'query_length': create_query_length_chart(query_length_dist) if not query_length_dist.empty else None
    }

    # Check for article mode
    article_content = load_article_content('docs/article.md')
    if article_content:
        print("  Using article mode for all-time page")
        sections = parse_article_sections(article_content)
        html = generate_article_html_with_jekyll(stats, figures, sections, top_queries_data=top_queries, query_slug_map=query_slug_map, data_file_id='all')
    else:
        html = generate_period_html(stats, figures, 'all', None, top_queries_data=top_queries, query_slug_map=query_slug_map, data_file_id='all')

    output_file = "docs/index.html"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"  Generated {output_file}")
    return output_file


def generate_period_page(conn, period_type: str, period_info: Dict, cutoff_date=None,
                         query_slug_map=None, blacklist=None) -> Optional[str]:
    """Generate a dashboard page for a specific period"""
    period_label = period_info['label']
    period_id = period_info['id']
    start_date = period_info['start']
    end_date = period_info['end']
    # Cap end_date to cutoff to exclude incomplete current day
    if cutoff_date and end_date > cutoff_date:
        end_date = cutoff_date

    # Get stats for this period
    stats = get_period_stats(conn, start_date, end_date, period_type, period_id)
    if stats is None:
        print(f"  No data for {period_label}, skipping")
        return None

    print(f"  Found {stats['total_searches']:,} searches for {period_label}")

    # Get chart data (using precomputed tables for top_queries and query_length_dist)
    daily_stats = get_daily_stats(conn, start_date, end_date)
    daily_unique_users = get_daily_unique_users(conn, start_date, end_date)
    top_queries = get_period_top_queries(conn, period_type, period_id)
    if blacklist:
        top_queries = [q for q in top_queries if not is_blacklisted(q[0], blacklist)]
    query_length_dist = get_period_query_length_dist(conn, period_type, period_id)

    # Create figures
    figures = {
        'daily_flow': create_daily_flow_chart(daily_stats) if not daily_stats.empty else None,
        'daily_unique_users': create_daily_unique_users_chart(daily_unique_users) if not daily_unique_users.empty else None,
        'client_distribution': create_client_distribution_chart(stats['client_totals']) if stats['client_totals'] else None,
        'top_queries': create_top_queries_chart(top_queries) if top_queries else None,
        'query_length': create_query_length_chart(query_length_dist) if not query_length_dist.empty else None
    }

    # Generate HTML
    html = generate_period_html(stats, figures, period_type, period_info, top_queries_data=top_queries, query_slug_map=query_slug_map, data_file_id=period_info['id'])

    # Determine output path
    if period_type == 'week':
        output_file = f"docs/weeks/{period_info['id']}.html"
    else:  # month
        output_file = f"docs/months/{period_info['id']}.html"

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"  Generated {output_file}")
    return output_file


def generate_article_html_with_jekyll(stats: Dict, figures: Dict[str, go.Figure],
                                      sections: List[Dict[str, Any]], top_queries_data: List[tuple] = None,
                                      query_slug_map: Dict[str, str] = None,
                                      data_file_id: str = 'all') -> str:
    """Generate article mode HTML with Jekyll front matter"""
    front_matter = "---\nlayout: dashboard\nperiod: all\ntitle: All Time Statistics\n---\n\n"

    chart_html = {}
    for name, fig in figures.items():
        if fig is not None:
            # For top_queries, combine chart + table
            if name == 'top_queries' and top_queries_data:
                chart_part = fig.to_html(full_html=False, include_plotlyjs=False)
                write_queries_db_file(top_queries_data, 'docs/data', data_file_id, query_slug_map)
                db_config_url = f'data/queries_{data_file_id}_config.json'
                table_part = create_queries_data_table(top_queries_data, db_config_url)
                chart_html[name] = f'{chart_part}\n{table_part}'
            else:
                chart_html[name] = fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            chart_html[name] = '<p>Not enough data for visualization</p>'

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

    article_css = '''
    <style>
        .prose { max-width: 800px; margin: 0 auto 30px auto; line-height: 1.7; color: #333; }
        .prose p { margin: 1em 0; font-size: 16px; }
        .prose h2 { margin-top: 50px; }
        .prose h3 { color: #333; margin-top: 30px; font-size: 20px; font-weight: 500; }
        .prose ul, .prose ol { margin: 1em 0; padding-left: 2em; }
        .prose li { margin: 0.5em 0; }
        .prose code { background: #f5f5f5; padding: 2px 6px; border-radius: 3px; font-size: 14px; }
        .prose pre { background: #f5f5f5; padding: 15px; border-radius: 4px; overflow-x: auto; }
        .prose pre code { background: none; padding: 0; }
        .prose blockquote { border-left: 4px solid #333; margin: 1.5em 0; padding-left: 20px; color: #666; font-style: italic; }
        .prose a { color: #333; text-decoration: underline; }
        .prose strong { font-weight: 600; }
        .prose em { font-style: italic; }
        .chart { max-width: 1200px; margin: 30px auto; }
    </style>
    '''

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
                         period_type: str, period_info: Optional[Dict] = None,
                         top_queries_data: List[tuple] = None,
                         query_slug_map: Dict[str, str] = None,
                         data_file_id: str = 'all') -> str:
    """Generate HTML for a period page with Jekyll front matter"""
    if period_type == 'all':
        front_matter = "---\nlayout: dashboard\nperiod: all\ntitle: All Time Statistics\n---\n\n"
        period_title = "All Time Statistics"
    elif period_type == 'week':
        front_matter = f"---\nlayout: dashboard\nperiod: week\nperiod_id: {period_info['id']}\ntitle: CW {period_info['label']}\n---\n\n"
        period_title = f"Calendar Week {period_info['label']}"
    else:  # month
        front_matter = f"---\nlayout: dashboard\nperiod: month\nperiod_id: {period_info['id']}\ntitle: {period_info['label']}\n---\n\n"
        period_title = period_info['label']

    chart_html = {}
    for name, fig in figures.items():
        if fig is not None:
            # For top_queries, combine chart + table
            if name == 'top_queries' and top_queries_data:
                chart_part = fig.to_html(full_html=False, include_plotlyjs=False)
                write_queries_db_file(top_queries_data, 'docs/data', data_file_id, query_slug_map)
                db_config_url = f'data/queries_{data_file_id}_config.json'
                table_part = create_queries_data_table(top_queries_data, db_config_url)
                chart_html[name] = f'{chart_part}\n{table_part}'
            else:
                chart_html[name] = fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            chart_html[name] = '<p style="color: #999">Not enough data for visualization</p>'

    days = format_days(stats['first_search'], stats['last_search'])

    stats_grid = f'''
        <div class="stats-tables">
            <table class="stats-table">
                <caption>Volume</caption>
                <tbody>
                    <tr><td class="stats-label">Total Searches</td><td class="stats-value">{stats['total_searches']:,}</td></tr>
                    <tr><td class="stats-label">Unique Users</td><td class="stats-value">{stats['total_users']:,}</td></tr>
                    <tr><td class="stats-label">Unique Queries</td><td class="stats-value">{stats['total_queries']:,}</td></tr>
                    <tr><td class="stats-label">Period</td><td class="stats-value">{days} days</td></tr>
                </tbody>
            </table>
            <table class="stats-table">
                <caption>Per User</caption>
                <tbody>
                    <tr><td class="stats-label">Avg Searches</td><td class="stats-value">{stats['avg_searches_per_user']:.1f}</td></tr>
                    <tr><td class="stats-label">Avg Unique Queries</td><td class="stats-value">{stats['avg_unique_queries_per_user']:.1f}</td></tr>
                </tbody>
            </table>
        </div>
    '''

    if stats['first_search'] and stats['last_search']:
        first_dt = datetime.fromisoformat(stats['first_search'])
        last_dt = datetime.fromisoformat(stats['last_search'])
        date_range_str = f"{first_dt.strftime('%Y-%m-%d %H:%M')} to {last_dt.strftime('%Y-%m-%d %H:%M')} UTC"
    else:
        date_range_str = "No data"

    content = f'''<h1>{period_title}</h1>
<p class="period-range">{date_range_str}</p>
<p class="timestamp">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

<h2>Summary Statistics</h2>
{stats_grid}

<h2>User Activity Trends <span style="font-weight: normal; font-size: 14px; color: #999;">(Unique Users per Day)</span></h2>
<div class="chart">
    {chart_html.get('daily_unique_users', '<p>Not enough data</p>')}
</div>

<h2>Top Search Terms <span style="font-weight: normal; font-size: 14px; color: #999;">(Count once per user)</span> <span class="info-icon" onclick="this.classList.toggle('open')">i<span class="info-tooltip">Queries are ranked by unique users, not raw search count. Only queries with 5 or more unique users are shown. Query detail pages are available for queries with 35 or more unique users.</span></span></h2>
<div class="chart">
    {chart_html.get('top_queries', '<p>Not enough data</p>')}
</div>

<h2>Query Length Distribution <span style="font-weight: normal; font-size: 14px; color: #999;">(Unique Queries)</span></h2>
<div class="chart">
    {chart_html.get('query_length', '<p>Not enough data</p>')}
</div>

<h2>Data Collection Overview <span style="font-weight: normal; font-size: 14px; color: #999;">(Raw Counts)</span> <span class="info-icon" onclick="this.classList.toggle('open')">i<span class="info-tooltip">Total search events show the raw count of search requests received by the research client, including duplicate queries by the same user(s). Top queries are ranked by unique users, not raw event count.</span></span></h2>
<div class="chart">
    {chart_html['daily_flow']}
</div>

<div class="chart">
    {chart_html.get('client_distribution', '<p>Not enough data</p>')}
</div>
'''
    return front_matter + content


def compute_query_similarities(conn, eligible_queries: set, top_n: int = 20,
                                min_shared_users: int = 5) -> Dict[str, list]:
    """Compute cosine similarity between queries based on user co-occurrence.

    Builds a binary user-query co-occurrence matrix and computes cosine
    similarity in chunks to stay within memory budget.

    Args:
        conn: Database connection
        eligible_queries: Set of query_normalized strings to include
        top_n: Number of most similar queries to return per query
        min_shared_users: Minimum shared users to include a pair

    Returns:
        Dict mapping query_normalized -> [(similar_query, score, shared_users), ...]
    """
    import time
    t0 = time.time()
    print("  Loading user-query pairs from database...")

    # Build index mappings and sparse matrix data in a single pass
    query_to_idx = {}
    user_to_idx = {}
    rows, cols = [], []

    cursor = conn.cursor('similarity_cursor')
    cursor.itersize = 50000
    cursor.execute("""
        SELECT username, query_normalized
        FROM user_query_pairs
        WHERE query_normalized = ANY(%s)
          AND last_seen >= CURRENT_DATE - INTERVAL '90 days'
    """, (list(eligible_queries),))

    for username, query_norm in cursor:
        if query_norm not in query_to_idx:
            query_to_idx[query_norm] = len(query_to_idx)
        if username not in user_to_idx:
            user_to_idx[username] = len(user_to_idx)
        rows.append(query_to_idx[query_norm])
        cols.append(user_to_idx[username])

    cursor.close()

    n_queries = len(query_to_idx)
    n_users = len(user_to_idx)
    print(f"  Built index: {n_queries} queries x {n_users} users, {len(rows)} pairs "
          f"({time.time() - t0:.1f}s)")

    if n_queries < 2:
        return {}

    # Build sparse binary matrix (queries x users)
    data = np.ones(len(rows), dtype=np.float32)
    matrix = csr_matrix((data, (rows, cols)), shape=(n_queries, n_users))

    # Reverse mapping for output
    idx_to_query = {idx: q for q, idx in query_to_idx.items()}

    # Compute similarities in chunks
    chunk_size = 500
    similarities = {}

    for start in range(0, n_queries, chunk_size):
        end = min(start + chunk_size, n_queries)
        chunk = matrix[start:end]

        # Cosine similarity: chunk (500 x users) vs full matrix (queries x users)
        sim_chunk = cosine_similarity(chunk, matrix)

        # Shared user counts: dot product of binary matrices
        shared_chunk = (chunk @ matrix.T).toarray()

        for i in range(end - start):
            global_idx = start + i
            query = idx_to_query[global_idx]

            # Zero out self-similarity
            sim_chunk[i, global_idx] = 0.0

            # Filter by min shared users
            valid_mask = shared_chunk[i] >= min_shared_users
            if not valid_mask.any():
                continue

            # Get top-N from valid entries
            valid_indices = np.where(valid_mask)[0]
            scores = sim_chunk[i, valid_indices]

            if len(valid_indices) <= top_n:
                top_local = np.argsort(-scores)
            else:
                top_local = np.argpartition(-scores, top_n)[:top_n]
                top_local = top_local[np.argsort(-scores[top_local])]

            result = []
            for li in top_local:
                gi = valid_indices[li]
                score = float(scores[li])
                if score <= 0:
                    continue
                result.append((idx_to_query[gi], score, int(shared_chunk[i, gi])))
            if result:
                similarities[query] = result

        if end % 2000 == 0 or end == n_queries:
            print(f"  Similarity progress: {end}/{n_queries} queries ({time.time() - t0:.1f}s)")

    print(f"  Computed similarities for {len(similarities)} queries ({time.time() - t0:.1f}s)")
    return similarities


def build_similar_queries_html(query_norm: str, query_similarities: Dict[str, list],
                               slug_map: Dict[str, str]) -> str:
    """Build HTML for the 'Users who searched this also searched' section.

    Returns empty string if no similar queries exist for this query.
    """
    similar = query_similarities.get(query_norm)
    if not similar:
        return ''

    items = []
    for sim_query, score, shared_users in similar:
        sim_slug = slug_map.get(sim_query)
        sim_escaped = (sim_query
                       .replace('&', '&amp;')
                       .replace('<', '&lt;')
                       .replace('>', '&gt;')
                       .replace('"', '&quot;'))
        pct = f"{score * 100:.0f}%"

        if sim_slug:
            link = f'<a href="{{{{ site.baseurl }}}}/queries/{sim_slug}.html">{sim_escaped}</a>'
        else:
            link = sim_escaped

        items.append(
            f'<li>{link} '
            f'<span class="sim-meta">{pct} similar, {shared_users:,} shared users</span></li>'
        )

    return f'''<div class="similar-queries">
<h2>Users who searched this also searched <span style="font-weight: normal; font-size: 14px; color: #999;">(last 90 days)</span></h2>
<ul>
{"".join(items)}
</ul>
</div>'''


def generate_query_pages(conn, cutoff_date=None, query_similarities=None,
                         blacklist=None) -> Dict[str, str]:
    """Generate individual query detail pages with SVG charts.

    Args:
        conn: Database connection
        cutoff_date: Exclude data after this date
        query_similarities: Dict mapping query -> [(similar_query, score, shared_users), ...]
        blacklist: List of lowercase fnmatch patterns to exclude

    Returns:
        Dict mapping query_normalized -> slug for linking
    """
    if query_similarities is None:
        query_similarities = {}
    cursor = conn.cursor()

    # Fetch all daily data from query_daily_stats
    if cutoff_date:
        cursor.execute("""
            SELECT query_normalized, date, search_count, unique_users
            FROM query_daily_stats
            WHERE date <= %s
            ORDER BY query_normalized, date
        """, (cutoff_date.date(),))
    else:
        cursor.execute("""
            SELECT query_normalized, date, search_count, unique_users
            FROM query_daily_stats
            ORDER BY query_normalized, date
        """)

    all_rows = cursor.fetchall()
    cursor.close()

    if not all_rows:
        print("  No query daily stats data found")
        return {}

    # Group by query_normalized
    query_daily = defaultdict(list)
    for query_norm, date, search_count, unique_users in all_rows:
        query_daily[query_norm].append((date, search_count, unique_users))

    # Remove blacklisted queries
    if blacklist:
        before = len(query_daily)
        query_daily = {q: v for q, v in query_daily.items()
                       if not is_blacklisted(q, blacklist)}
        print(f"  Blacklist removed {before - len(query_daily)} queries from detail pages")

    print(f"  Found daily data for {len(query_daily)} queries")

    # Fetch all-time stats per query from period_top_queries
    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_normalized, unique_users, total_searches
        FROM period_top_queries
        WHERE period_type = 'all_time' AND period_id = 'all_time'
          AND unique_users >= 35
    """)
    query_stats = {row[0]: {'unique_users': row[1], 'total_searches': row[2]} for row in cursor.fetchall()}
    cursor.close()

    # Generate pages
    os.makedirs('docs/queries', exist_ok=True)

    # Pre-build slug map so we can link between similar query pages
    slug_map = {q: slugify_query(q) for q in query_daily}
    count = 0

    for query_norm, daily_data in query_daily.items():
        slug = slug_map[query_norm]

        stats = query_stats.get(query_norm, {'unique_users': 0, 'total_searches': 0})
        first_seen = daily_data[0][0]
        last_seen = daily_data[-1][0]
        total_days = len(daily_data)

        # Display name: capitalize first letter of each word for readability
        display_name = query_norm

        # Generate SVG chart
        svg_chart = create_svg_line_chart(daily_data)

        # Format dates
        first_str = first_seen.strftime('%Y-%m-%d') if hasattr(first_seen, 'strftime') else str(first_seen)
        last_str = last_seen.strftime('%Y-%m-%d') if hasattr(last_seen, 'strftime') else str(last_seen)

        # Escape HTML in query display
        display_escaped = (display_name
                           .replace('&', '&amp;')
                           .replace('<', '&lt;')
                           .replace('>', '&gt;')
                           .replace('"', '&quot;'))

        content = f'''---
layout: query
query_display: "{display_escaped}"
---

<div class="breadcrumb">
    <a href="{{{{ site.baseurl }}}}/">All Time</a> / Query Detail
</div>

<h1>{display_escaped}</h1>

<div class="stats-grid">
    <div class="stat-card">
        <h3>Unique Users</h3>
        <div class="value">{stats["unique_users"]:,}</div>
        <div class="label">All-time unique searchers</div>
    </div>
    <div class="stat-card">
        <h3>Total Searches</h3>
        <div class="value">{stats["total_searches"]:,}</div>
        <div class="label">All-time search events</div>
    </div>
    <div class="stat-card">
        <h3>First Seen</h3>
        <div class="value">{first_str}</div>
    </div>
    <div class="stat-card">
        <h3>Last Seen</h3>
        <div class="value">{last_str}</div>
    </div>
    <div class="stat-card">
        <h3>Active Days</h3>
        <div class="value">{total_days:,}</div>
        <div class="label">Days with at least 1 search</div>
    </div>
</div>

<h2>Daily Search Activity</h2>
<div class="chart-container">
    {svg_chart}
</div>
<p style="font-size: 12px; color: #999; margin-top: 8px;">Unique users are counted per day based on distinct (hashed) usernames. A user searching the same query multiple times in one day counts once for that day. All dates are in UTC.</p>

{build_similar_queries_html(query_norm, query_similarities, slug_map)}

<div class="back-link">
    <a href="{{{{ site.baseurl }}}}/">Back to dashboard</a>
</div>
'''

        output_file = f"docs/queries/{slug}.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)

        count += 1
        if count % 500 == 0:
            print(f"  Generated {count}/{len(query_daily)} query pages...")

    print(f"  Generated {count} query pages in docs/queries/")

    return slug_map


def main():
    """Main execution using materialized views"""
    print("Connecting to PostgreSQL database...")
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        raise

    # Calculate cutoff: end of yesterday UTC (exclude incomplete current day)
    cutoff_date = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                   - timedelta(seconds=1))
    print(f"Data cutoff: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    try:
        # Load query blacklist patterns
        blacklist = parse_blacklist(os.environ.get('QUERY_BLACKLIST', ''))
        if blacklist:
            print(f"Query blacklist: {len(blacklist)} patterns loaded")

        # Get available periods from database
        periods = get_available_periods(conn, max_date=cutoff_date)
        print(f"Found {len(periods['months'])} months and {len(periods['weeks'])} weeks")

        # Generate Jekyll data files for navigation
        generate_jekyll_data_files(periods)
        print("Generated Jekyll navigation data files")

        # Ensure user_query_pairs table exists
        print("Checking user_query_pairs table...")
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
        # Quick existence check (avoids full table scan)
        cursor.execute("SELECT EXISTS (SELECT 1 FROM user_query_pairs LIMIT 1)")
        has_data = cursor.fetchone()[0]
        if has_data:
            print("  user_query_pairs table is populated")
        else:
            print("  WARNING: user_query_pairs is empty. Run archive.py or seed manually.")
        # Remove blacklisted queries from user_query_pairs
        if blacklist:
            for pattern in blacklist:
                sql_pattern = pattern.replace('*', '%').replace('?', '_')
                cursor.execute(
                    "DELETE FROM user_query_pairs WHERE LOWER(query_normalized) LIKE %s",
                    (sql_pattern,)
                )
            conn.commit()
            print(f"  Removed blacklisted query pairs")
        cursor.close()

        # Get eligible queries for similarity computation (queries with detail pages)
        print("=" * 60)
        print("COMPUTING QUERY SIMILARITIES")
        print("=" * 60)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT query_normalized FROM query_daily_stats")
        eligible_queries = {row[0] for row in cursor.fetchall()
                           if not is_blacklisted(row[0], blacklist)}
        cursor.close()
        print(f"  {len(eligible_queries)} eligible queries")
        query_similarities = compute_query_similarities(conn, eligible_queries)

        # Generate query detail pages (to get slug map for linking)
        print("=" * 60)
        print("GENERATING QUERY DETAIL PAGES")
        print("=" * 60)
        query_slug_map = generate_query_pages(conn, cutoff_date, query_similarities, blacklist)

        # Generate all-time page (uses cumulative stats + materialized views)
        print("=" * 60)
        print("GENERATING ALL-TIME PAGE")
        print("=" * 60)
        generate_all_time_page(conn, cutoff_date, query_slug_map, blacklist)

        # Generate monthly pages
        for month in periods['months']:
            print("=" * 60)
            print(f"GENERATING MONTHLY PAGE: {month['label']}")
            print("=" * 60)
            generate_period_page(conn, 'month', month, cutoff_date, query_slug_map, blacklist)

        # Generate weekly pages
        for week in periods['weeks']:
            print("=" * 60)
            print(f"GENERATING WEEKLY PAGE: CW {week['label']}")
            print("=" * 60)
            generate_period_page(conn, 'week', week, cutoff_date, query_slug_map, blacklist)

        total_pages = 1 + len(periods['months']) + len(periods['weeks']) + len(query_slug_map)
        print("\n" + "=" * 60)
        print(f"Generated {total_pages} total pages")
        print(f"  - 1 all-time, {len(periods['months'])} monthly, {len(periods['weeks'])} weekly, {len(query_slug_map)} query detail")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
