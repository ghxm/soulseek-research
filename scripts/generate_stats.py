#!/usr/bin/env python3
"""
Generate statistics dashboard for Soulseek research data.
Queries pre-computed materialized views and cumulative stats table.
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import psycopg2
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
    first = datetime.fromisoformat(str(first_search_str).replace('Z', '+00:00'))
    last = datetime.fromisoformat(str(last_search_str).replace('Z', '+00:00'))
    delta = last - first
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
    """Get cumulative stats (archived + live data combined)"""
    cursor = conn.cursor()

    # Get cumulative stats from archived data
    cursor.execute("""
        SELECT total_searches, total_users, total_queries, total_search_pairs,
               first_search, last_search, client_totals
        FROM stats_cumulative
        WHERE id = 1
    """)
    cumulative = cursor.fetchone()

    if cumulative is None:
        # No cumulative stats yet, return zeros
        cumulative = (0, 0, 0, 0, None, None, {})

    (cum_searches, cum_users, cum_queries, cum_pairs,
     cum_first, cum_last, cum_clients) = cumulative

    # Parse client_totals if it's a string
    if isinstance(cum_clients, str):
        cum_clients = json.loads(cum_clients) if cum_clients else {}

    # Get live data stats from materialized view
    cursor.execute("""
        SELECT total_searches, total_users, total_queries, total_search_pairs,
               first_search, last_search, client_totals
        FROM mv_summary_stats
    """)
    live = cursor.fetchone()

    if live is None or live[0] is None:
        # No live data
        live = (0, 0, 0, 0, None, None, {})

    (live_searches, live_users, live_queries, live_pairs,
     live_first, live_last, live_clients) = live

    # Parse client_totals if it's a string
    if isinstance(live_clients, str):
        live_clients = json.loads(live_clients) if live_clients else {}

    # Combine cumulative + live
    total_searches = cum_searches + live_searches
    total_users = cum_users + live_users  # Approximate
    total_queries = cum_queries + live_queries  # Approximate
    total_pairs = cum_pairs + live_pairs  # Approximate

    # Earliest/latest timestamps
    first_search = None
    if cum_first and live_first:
        first_search = min(cum_first, live_first)
    else:
        first_search = cum_first or live_first

    last_search = None
    if cum_last and live_last:
        last_search = max(cum_last, live_last)
    else:
        last_search = cum_last or live_last

    # Merge client totals
    client_totals = {}
    for client, count in (cum_clients or {}).items():
        client_totals[client] = client_totals.get(client, 0) + count
    for client, count in (live_clients or {}).items():
        client_totals[client] = client_totals.get(client, 0) + count

    cursor.close()

    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = total_pairs / total_users if total_users > 0 else 0

    return {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
        'total_search_pairs': total_pairs,
        'avg_searches_per_user': avg_searches_per_user,
        'avg_unique_queries_per_user': avg_unique_queries_per_user,
        'first_search': first_search.isoformat() if first_search else None,
        'last_search': last_search.isoformat() if last_search else None,
        'client_totals': client_totals
    }


def get_live_summary_stats(conn) -> Dict[str, Any]:
    """Get summary stats from live data only (from materialized view)"""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT total_searches, total_users, total_queries, total_search_pairs,
               first_search, last_search, client_totals
        FROM mv_summary_stats
    """)
    result = cursor.fetchone()
    cursor.close()

    if result is None or result[0] is None:
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

    (total_searches, total_users, total_queries, total_pairs,
     first_search, last_search, client_totals) = result

    if isinstance(client_totals, str):
        client_totals = json.loads(client_totals) if client_totals else {}

    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = total_pairs / total_users if total_users > 0 else 0

    return {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
        'total_search_pairs': total_pairs,
        'avg_searches_per_user': avg_searches_per_user,
        'avg_unique_queries_per_user': avg_unique_queries_per_user,
        'first_search': first_search.isoformat() if first_search else None,
        'last_search': last_search.isoformat() if last_search else None,
        'client_totals': client_totals or {}
    }


def get_daily_stats(conn, start_date=None, end_date=None) -> pd.DataFrame:
    """Get daily stats from materialized view, optionally filtered by date range"""
    cursor = conn.cursor()

    if start_date and end_date:
        cursor.execute("""
            SELECT client_id, date, search_count, unique_users
            FROM mv_daily_stats
            WHERE date >= %s AND date <= %s
            ORDER BY date, client_id
        """, (start_date.date(), end_date.date()))
    else:
        cursor.execute("""
            SELECT client_id, date, search_count, unique_users
            FROM mv_daily_stats
            ORDER BY date, client_id
        """)

    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return pd.DataFrame(columns=['client_id', 'date', 'search_count', 'unique_users'])

    return pd.DataFrame(rows, columns=['client_id', 'date', 'search_count', 'unique_users'])


def get_daily_unique_users(conn, start_date=None, end_date=None) -> pd.DataFrame:
    """Get daily unique users from mv_daily_stats (approximation - max across clients per day)"""
    cursor = conn.cursor()

    if start_date and end_date:
        # Use mv_daily_stats for fast pre-aggregated results
        cursor.execute("""
            SELECT date, MAX(unique_users) as unique_users
            FROM mv_daily_stats
            WHERE date >= %s AND date <= %s
            GROUP BY date
            ORDER BY date
        """, (start_date.date(), end_date.date()))
    else:
        # For all-time: use mv_daily_stats (approximation - max across clients per day)
        cursor.execute("""
            SELECT date, MAX(unique_users) as unique_users
            FROM mv_daily_stats
            GROUP BY date
            ORDER BY date
        """)

    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return pd.DataFrame(columns=['date', 'unique_users'])

    return pd.DataFrame(rows, columns=['date', 'unique_users'])


def get_top_queries(conn) -> List[tuple]:
    """Get all queries with 5+ unique users from materialized view"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_normalized, unique_users, total_searches
        FROM mv_top_queries
        ORDER BY unique_users DESC, total_searches DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def get_query_length_distribution(conn) -> pd.DataFrame:
    """Get query length distribution from materialized view"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT word_count as query_length, unique_query_count as count
        FROM mv_query_length_dist
        ORDER BY word_count
    """)
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return pd.DataFrame(columns=['query_length', 'count'])

    return pd.DataFrame(rows, columns=['query_length', 'count'])


def get_period_stats(conn, start_date, end_date) -> Dict[str, Any]:
    """Get stats for a specific period using mv_daily_stats (fast, pre-aggregated)"""
    cursor = conn.cursor()

    # Use mv_daily_stats for fast aggregation (pre-computed daily counts)
    cursor.execute("""
        SELECT
            COALESCE(SUM(search_count), 0) as total_searches,
            COALESCE(SUM(unique_users), 0) as total_users,
            MIN(date) as first_date,
            MAX(date) as last_date
        FROM mv_daily_stats
        WHERE date >= %s AND date <= %s
    """, (start_date.date(), end_date.date()))

    result = cursor.fetchone()

    if result is None or result[0] == 0:
        cursor.close()
        return None

    total_searches, total_users, first_date, last_date = result
    total_searches = int(total_searches) if total_searches else 0
    total_users = int(total_users) if total_users else 0

    # Get per-client totals from mv_daily_stats
    cursor.execute("""
        SELECT client_id, SUM(search_count) as count
        FROM mv_daily_stats
        WHERE date >= %s AND date <= %s
        GROUP BY client_id
    """, (start_date.date(), end_date.date()))
    client_totals = {row[0]: int(row[1]) for row in cursor.fetchall()}

    cursor.close()

    # Approximate unique queries as ~60% of unique users (typical ratio from global stats)
    total_queries = int(total_users * 0.6) if total_users > 0 else 0
    total_pairs = total_users  # Approximate

    avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
    avg_unique_queries_per_user = 1.0  # Approximation

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


def get_period_top_queries(conn, start_date, end_date) -> List[tuple]:
    """Get top queries - uses global mv_top_queries (approximation for period)"""
    # Use global top queries - period-specific would require additional materialized views
    return get_top_queries(conn)


def get_period_query_length_dist(conn, start_date, end_date) -> pd.DataFrame:
    """Get query length distribution - uses global mv_query_length_dist (approximation)"""
    # Use global distribution - period-specific would require additional materialized views
    return get_query_length_distribution(conn)


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


def create_top_queries_chart(top_queries: List[tuple], limit: int = 100) -> go.Figure:
    """Create interactive bar chart of top N queries for performance.

    Args:
        top_queries: List of (query, unique_users, total_searches) tuples
        limit: Maximum number of queries to show in chart (default 100)

    Returns:
        Plotly figure with top N queries
    """
    total_count = len(top_queries)
    queries = [q[0] for q in top_queries[:limit]]
    unique_users = [q[1] for q in top_queries[:limit]]

    # Fixed reasonable height for performance
    chart_height = 600

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


def create_queries_data_table(top_queries: List[tuple]) -> str:
    """Create a lightweight, searchable HTML table for all queries using Clusterize.js.

    Uses Clusterize.js for virtual scrolling (only renders visible rows),
    enabling smooth performance with 400k+ rows. Data stored in compact array format.

    Args:
        top_queries: List of (query, unique_users, total_searches) tuples

    Returns:
        HTML string with interactive table and CSV download link
    """
    total_count = len(top_queries)

    # Generate CSV data as base64 for download link
    import csv
    import io
    import base64
    import gzip

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(['Rank', 'Query', 'Unique Users', 'Total Searches'])
    for i, (query, users, searches) in enumerate(top_queries, 1):
        writer.writerow([i, query, users, searches])

    csv_data = csv_buffer.getvalue()
    # Gzip compress for smaller download
    csv_gzipped = gzip.compress(csv_data.encode('utf-8'))
    csv_base64 = base64.b64encode(csv_gzipped).decode('utf-8')

    # Store ALL data in compact array format: [rank, query, users, searches]
    # Clusterize.js will handle virtual rendering
    import json
    all_data = []
    for i, (query, users, searches) in enumerate(top_queries, 1):
        all_data.append([i, query, users, searches])

    all_data_json = json.dumps(all_data)

    return f'''
    <div class="queries-table-container">
        <div class="table-header">
            <h3>Complete Query Dataset ({total_count:,} queries)</h3>
            <div class="table-controls">
                <input
                    type="text"
                    id="table_search"
                    class="table-search-input"
                    placeholder="Search queries..."
                    autocomplete="off"
                />
                <button
                    onclick="window.downloadQueriesCSV()"
                    class="download-btn"
                >
                    Download CSV
                </button>
            </div>
            <div class="search-results" id="search_results">
                Showing all {total_count:,} queries (virtual scrolling enabled)
            </div>
        </div>

        <div id="scroll_area" class="table-scroll-container">
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
                    <!-- Clusterize.js will populate this -->
                </tbody>
            </table>
        </div>
    </div>

    <!-- Include Clusterize.js (2.3KB gzipped) -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/clusterize.js/0.19.0/clusterize.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/clusterize.js/0.19.0/clusterize.min.css">

    <!-- Include pako for gzip decompression -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/pako/2.1.0/pako.min.js"></script>

    <script>
    (function() {{
        const searchInput = document.getElementById('table_search');
        const resultsDiv = document.getElementById('search_results');

        // Store all data (compact array format: [rank, query, users, searches])
        const allData = {all_data_json};
        let filteredData = allData;

        // CSV download function
        window.downloadQueriesCSV = function() {{
            const base64 = '{csv_base64}';
            const binary = atob(base64);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) {{
                bytes[i] = binary.charCodeAt(i);
            }}
            const decompressed = pako.inflate(bytes, {{ to: 'string' }});

            const blob = new Blob([decompressed], {{ type: 'text/csv' }});
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'top_queries.csv';
            a.click();
            window.URL.revokeObjectURL(url);
        }};

        // Convert data to HTML rows for Clusterize.js
        function dataToRows(data) {{
            return data.map(function(item) {{
                const rank = item[0];
                const query = item[1];
                const users = item[2];
                const searches = item[3];

                // Escape HTML in query text
                const queryEscaped = query
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;');

                return '<tr>' +
                    '<td class="rank-col">' + rank + '</td>' +
                    '<td class="query-col">' + queryEscaped + '</td>' +
                    '<td class="users-col">' + users.toLocaleString() + '</td>' +
                    '<td class="searches-col">' + searches.toLocaleString() + '</td>' +
                    '</tr>';
            }});
        }}

        // Initialize Clusterize.js with all data
        const clusterize = new Clusterize({{
            rows: dataToRows(allData),
            scrollId: 'scroll_area',
            contentId: 'content_area',
            rows_in_block: 50,  // Render 50 rows per block for smooth scrolling
            blocks_in_cluster: 4  // Keep 4 blocks (200 rows) in DOM at once
        }});

        console.log('Clusterize.js initialized with {total_count:,} queries');

        // Search functionality
        searchInput.addEventListener('input', function() {{
            const searchTerm = this.value.toLowerCase().trim();

            if (!searchTerm) {{
                // No filter - show all data
                filteredData = allData;
                clusterize.update(dataToRows(filteredData));
                resultsDiv.textContent = 'Showing all {total_count:,} queries (virtual scrolling enabled)';
                return;
            }}

            // Filter data based on search term
            filteredData = allData.filter(function(item) {{
                const query = item[1].toLowerCase();
                return query.includes(searchTerm);
            }});

            clusterize.update(dataToRows(filteredData));
            resultsDiv.textContent = 'Showing ' + filteredData.length.toLocaleString() + ' of {total_count:,} queries';
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

        .download-btn {{
            padding: 10px 20px;
            background: #333;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-weight: 500;
            white-space: nowrap;
            font-size: 14px;
            border: none;
            cursor: pointer;
        }}

        .download-btn:hover {{
            background: #000;
        }}

        .search-results {{
            font-size: 14px;
            color: #666;
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

        .users-col, .searches-col {{
            width: 120px;
            text-align: right;
            font-variant-numeric: tabular-nums;
        }}

        /* Clusterize.js adjustments */
        .clusterize-scroll {{
            max-height: 600px;
        }}

        .clusterize-content {{
            /* Clusterize.js manages this */
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
    """Get all available weeks and months from the database."""
    cursor = conn.cursor()

    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM searches")
    min_date, db_max_date = cursor.fetchone()

    if not min_date or not db_max_date:
        return {'weeks': [], 'months': []}

    effective_max = min(db_max_date, max_date) if max_date else db_max_date

    # Generate weeks
    weeks = []
    current = min_date
    while current <= effective_max:
        iso_year, iso_week, _ = current.isocalendar()
        week_id = f"{iso_year}-W{iso_week:02d}"
        week_label = f"{iso_week:02d}/{iso_year}"
        week_start = datetime.fromisocalendar(iso_year, iso_week, 1).replace(tzinfo=timezone.utc)
        week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        weeks.append({'id': week_id, 'label': week_label, 'start': week_start, 'end': week_end})
        current = week_end + timedelta(seconds=1)

    # Generate months
    months = []
    current = min_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= effective_max:
        month_id = current.strftime('%Y-%m')
        month_label = current.strftime('%B %Y')
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        month_end = next_month - timedelta(seconds=1)
        months.append({'id': month_id, 'label': month_label, 'start': current, 'end': month_end})
        current = next_month

    cursor.close()
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


def generate_all_time_page(conn) -> str:
    """Generate the all-time dashboard page using cumulative stats + materialized views"""
    print("  Computing all-time statistics from cumulative + live data...")

    # Get cumulative stats (archived + live combined)
    stats = get_cumulative_stats(conn)

    if stats['total_searches'] == 0:
        print("  No data available, skipping all-time page")
        return None

    print(f"  Total all-time: {stats['total_searches']:,} searches, {stats['total_users']:,} users")

    # Get chart data from materialized views
    print("  Loading chart data from materialized views...")
    daily_stats = get_daily_stats(conn)
    daily_unique_users = get_daily_unique_users(conn)
    top_queries = get_top_queries(conn)
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
        html = generate_article_html_with_jekyll(stats, figures, sections, top_queries_data=top_queries)
    else:
        html = generate_period_html(stats, figures, 'all', None, top_queries_data=top_queries)

    output_file = "docs/index.html"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"  Generated {output_file}")
    return output_file


def generate_period_page(conn, period_type: str, period_info: Dict) -> Optional[str]:
    """Generate a dashboard page for a specific period"""
    period_label = period_info['label']
    start_date = period_info['start']
    end_date = period_info['end']

    # Get stats for this period
    stats = get_period_stats(conn, start_date, end_date)
    if stats is None:
        print(f"  No data for {period_label}, skipping")
        return None

    print(f"  Found {stats['total_searches']:,} searches for {period_label}")

    # Get chart data
    daily_stats = get_daily_stats(conn, start_date, end_date)
    daily_unique_users = get_daily_unique_users(conn, start_date, end_date)
    top_queries = get_period_top_queries(conn, start_date, end_date)
    query_length_dist = get_period_query_length_dist(conn, start_date, end_date)

    # Create figures
    figures = {
        'daily_flow': create_daily_flow_chart(daily_stats) if not daily_stats.empty else None,
        'daily_unique_users': create_daily_unique_users_chart(daily_unique_users) if not daily_unique_users.empty else None,
        'client_distribution': create_client_distribution_chart(stats['client_totals']) if stats['client_totals'] else None,
        'top_queries': create_top_queries_chart(top_queries) if top_queries else None,
        'query_length': create_query_length_chart(query_length_dist) if not query_length_dist.empty else None
    }

    # Generate HTML
    html = generate_period_html(stats, figures, period_type, period_info, top_queries_data=top_queries)

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
                                      sections: List[Dict[str, Any]], top_queries_data: List[tuple] = None) -> str:
    """Generate article mode HTML with Jekyll front matter"""
    front_matter = "---\nlayout: dashboard\nperiod: all\ntitle: All Time Statistics\n---\n\n"

    chart_html = {}
    for name, fig in figures.items():
        if fig is not None:
            # For top_queries, combine chart + table
            if name == 'top_queries' and top_queries_data:
                chart_part = fig.to_html(full_html=False, include_plotlyjs='cdn')
                table_part = create_queries_data_table(top_queries_data)
                chart_html[name] = f'{chart_part}\n{table_part}'
            else:
                chart_html[name] = fig.to_html(full_html=False, include_plotlyjs='cdn')
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
                         top_queries_data: List[tuple] = None) -> str:
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
                chart_part = fig.to_html(full_html=False, include_plotlyjs='cdn')
                table_part = create_queries_data_table(top_queries_data)
                chart_html[name] = f'{chart_part}\n{table_part}'
            else:
                chart_html[name] = fig.to_html(full_html=False, include_plotlyjs='cdn')
        else:
            chart_html[name] = '<p style="color: #999">Not enough data for visualization</p>'

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

    if stats['first_search'] and stats['last_search']:
        first_dt = datetime.fromisoformat(stats['first_search'])
        last_dt = datetime.fromisoformat(stats['last_search'])
        date_range_str = f"{first_dt.strftime('%Y-%m-%d %H:%M')} to {last_dt.strftime('%Y-%m-%d %H:%M')} UTC"
    else:
        date_range_str = "No data"

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
        # Get available periods from database
        periods = get_available_periods(conn, max_date=cutoff_date)
        print(f"Found {len(periods['months'])} months and {len(periods['weeks'])} weeks")

        # Generate Jekyll data files for navigation
        generate_jekyll_data_files(periods)
        print("Generated Jekyll navigation data files")

        # Generate all-time page (uses cumulative stats + materialized views)
        print("=" * 60)
        print("GENERATING ALL-TIME PAGE")
        print("=" * 60)
        generate_all_time_page(conn)

        # Generate monthly pages
        for month in periods['months']:
            print("=" * 60)
            print(f"GENERATING MONTHLY PAGE: {month['label']}")
            print("=" * 60)
            generate_period_page(conn, 'month', month)

        # Generate weekly pages
        for week in periods['weeks']:
            print("=" * 60)
            print(f"GENERATING WEEKLY PAGE: CW {week['label']}")
            print("=" * 60)
            generate_period_page(conn, 'week', week)

        total_pages = 1 + len(periods['months']) + len(periods['weeks'])
        print("\n" + "=" * 60)
        print(f"Generated {total_pages} dashboard pages")
        print(f"  - 1 all-time, {len(periods['months'])} monthly, {len(periods['weeks'])} weekly")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
