#!/usr/bin/env python3
"""
Generate statistics dashboard for Soulseek research data.
Queries the database and creates visualizations for GitHub Pages.
"""

import os
import json
import re
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from typing import List, Dict, Any, Tuple, Optional

import psycopg2
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import mistune


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


def get_deduplication_cte() -> str:
    """
    Returns a CTE that deduplicates searches within 5-minute windows.

    This handles the case where the same search is distributed to multiple
    clients, resulting in duplicate entries. We only keep the first occurrence
    within each 5-minute window for the same username+query combination.
    """
    return """
        WITH ranked_searches AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY username, query,
                                    FLOOR(EXTRACT(EPOCH FROM timestamp) / 300)
                       ORDER BY timestamp
                   ) as rn
            FROM searches
        ),
        deduplicated_searches AS (
            SELECT id, client_id, timestamp, username, query
            FROM ranked_searches
            WHERE rn = 1
        )
    """


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

    # Pattern to match chart markers and stats-grid
    pattern = r'(<!--\s*chart:\s*(\w+)\s*-->|<!--\s*stats-grid\s*-->)'

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


def query_top_queries(conn, limit: int = 100) -> List[tuple]:
    """Query most searched queries (deduplicated)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            query,
            COUNT(DISTINCT username) as unique_users,
            COUNT(*) as total_searches
        FROM deduplicated_searches
        GROUP BY query
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
    """Query distribution of search query lengths in words (unique queries)"""
    query = f"""
        {get_deduplication_cte()}
        SELECT
            array_length(string_to_array(TRIM(query), ' '), 1) as query_length,
            COUNT(DISTINCT query) as count
        FROM deduplicated_searches
        GROUP BY array_length(string_to_array(TRIM(query), ' '), 1)
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

    return {
        'total_searches': total_searches,
        'total_users': total_users,
        'total_queries': total_queries,
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
        title='Daily Search Volume by Client - Raw Data Collection (Last 7 Days)',
        xaxis_title='Date',
        yaxis_title='Number of Searches (Raw)',
        hovermode='x unified',
        height=500,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
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
        title='Daily Active Users - Deduplicated (Last 7 Days)',
        xaxis_title='Date',
        yaxis_title='Unique Users',
        hovermode='x unified',
        height=400,
        template='plotly_white',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black')
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
    """Create histogram of query length distribution"""
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
        title='Search Query Length Distribution (Characters)',
        xaxis_title='Query Length',
        yaxis_title='Number of Queries',
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
                <h3>Collection Period</h3>
                <div class="value">{days}</div>
                <div class="label">Days of data</div>
            </div>
        </div>
    '''


def generate_article_html(stats: Dict, figures: Dict[str, go.Figure],
                          sections: List[Dict[str, Any]]) -> str:
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
                  article_content: Optional[str] = None) -> str:
    """
    Generate HTML dashboard.

    If article_content is provided, generates article mode with prose.
    Otherwise, generates standard chart-only dashboard (existing behavior).
    """
    # Article mode: parse Markdown and generate article-style HTML
    if article_content:
        sections = parse_article_sections(article_content)
        return generate_article_html(stats, figures, sections)

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

        {generate_stats_grid_html(stats)}

        <h2>Data Collection Overview <span style="font-weight: normal; font-size: 14px; color: #999;">(Raw Counts)</span></h2>
        <div class="chart">
            {chart_html['daily_flow']}
        </div>

        <div class="chart">
            {chart_html['client_distribution']}
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


def main():
    """Main execution"""
    print("Connecting to database...")
    conn = get_db_connection()

    try:
        print("Querying statistics...")

        # Get all statistics
        stats = query_total_stats(conn)
        daily_stats = query_daily_stats(conn, days=7)
        daily_unique_users = query_daily_unique_users(conn, days=7)
        top_queries = query_top_queries(conn, limit=100)
        top_users = query_most_active_users(conn, limit=50)

        # Music-specific analyses
        print("Analyzing query patterns...")
        query_sample = query_all_queries_sample(conn, limit=10000)
        query_patterns = analyze_query_patterns(query_sample)

        print("Analyzing temporal patterns...")
        temporal_data = query_temporal_patterns(conn)
        query_length_data = query_query_length_distribution(conn)

        print("Extracting n-grams...")
        bigrams = analyze_ngrams(query_sample, n=2, limit=30)
        trigrams = analyze_ngrams(query_sample, n=3, limit=30)

        print("Analyzing term co-occurrence...")
        cooccurrences = analyze_term_cooccurrence(query_sample, min_freq=10)

        print(f"Found {stats['total_searches']:,} total searches (deduplicated)")
        print(f"Creating visualizations...")

        # Create all figures
        figures = {
            'daily_flow': create_daily_flow_chart(daily_stats),
            'daily_unique_users': create_daily_unique_users_chart(daily_unique_users),
            'top_queries': create_top_queries_chart(top_queries),
            'user_activity': create_user_activity_chart(top_users),
            'client_distribution': create_client_distribution_chart(stats['client_totals']),
            'query_patterns': create_query_pattern_chart(query_patterns),
            'temporal_hourly': create_temporal_hourly_chart(temporal_data),
            'query_length': create_query_length_chart(query_length_data),
            'ngrams': create_ngram_chart(bigrams, trigrams),
            'cooccurrence': create_cooccurrence_chart(cooccurrences)
        }

        # Check for article content
        article_content = load_article_content('docs/article.md')

        if article_content:
            print("Article mode: Found docs/article.md")
        else:
            print("Standard mode: No article.md found")

        print("Generating HTML...")
        html = generate_html(stats, figures, article_content=article_content)

        # Create docs directory
        os.makedirs('docs', exist_ok=True)

        # Write HTML file
        with open('docs/index.html', 'w', encoding='utf-8') as f:
            f.write(html)

        # Also save stats as JSON for potential API use
        with open('docs/stats.json', 'w') as f:
            json.dump(stats, f, indent=2)

        print("✅ Dashboard generated successfully at docs/index.html")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
