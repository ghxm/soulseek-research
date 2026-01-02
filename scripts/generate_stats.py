#!/usr/bin/env python3
"""
Generate statistics dashboard for Soulseek research data.
Queries the database and creates visualizations for GitHub Pages.
"""

import os
import json
from datetime import datetime, timedelta, timezone
from collections import Counter
from typing import List, Dict, Any

import psycopg2
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans


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


def query_daily_stats(conn, days: int = 7) -> pd.DataFrame:
    """Query daily search counts per client for the last N days"""
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


def query_top_queries(conn, limit: int = 100) -> List[tuple]:
    """Query most searched queries (deduplicated)"""
    query = """
        SELECT
            query,
            COUNT(DISTINCT username) as unique_users,
            COUNT(*) as total_searches
        FROM searches
        GROUP BY query
        ORDER BY total_searches DESC
        LIMIT %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    cursor.close()
    return results


def query_most_active_users(conn, limit: int = 50) -> List[tuple]:
    """Query most active users by search count"""
    query = """
        SELECT
            username,
            COUNT(*) as search_count,
            COUNT(DISTINCT query) as unique_queries
        FROM searches
        GROUP BY username
        ORDER BY search_count DESC
        LIMIT %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    cursor.close()
    return results


def query_all_queries_for_clustering(conn, limit: int = 5000) -> List[str]:
    """Query recent queries for clustering analysis"""
    query = """
        SELECT DISTINCT query
        FROM searches
        WHERE timestamp >= NOW() - INTERVAL '7 days'
        LIMIT %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (limit,))
    results = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return results


def query_total_stats(conn) -> Dict[str, Any]:
    """Query overall statistics"""
    cursor = conn.cursor()

    # Total searches
    cursor.execute("SELECT COUNT(*) FROM searches")
    total_searches = cursor.fetchone()[0]

    # Total unique users
    cursor.execute("SELECT COUNT(DISTINCT username) FROM searches")
    total_users = cursor.fetchone()[0]

    # Total unique queries
    cursor.execute("SELECT COUNT(DISTINCT query) FROM searches")
    total_queries = cursor.fetchone()[0]

    # Date range
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM searches")
    date_range = cursor.fetchone()

    # Per-client totals
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

    # Get unique clients
    clients = df['client_id'].unique()

    for client in clients:
        client_data = df[df['client_id'] == client].sort_values('date')
        fig.add_trace(go.Scatter(
            x=client_data['date'],
            y=client_data['search_count'],
            mode='lines+markers',
            name=client,
            line=dict(width=2),
            marker=dict(size=8)
        ))

    fig.update_layout(
        title='Daily Search Volume by Client (Last 7 Days)',
        xaxis_title='Date',
        yaxis_title='Number of Searches',
        hovermode='x unified',
        height=500,
        template='plotly_white'
    )

    return fig


def create_top_queries_chart(top_queries: List[tuple]) -> go.Figure:
    """Create bar chart of top queries"""
    queries = [q[0][:50] for q in top_queries[:20]]  # Truncate long queries
    counts = [q[2] for q in top_queries[:20]]

    fig = go.Figure(data=[
        go.Bar(
            y=queries[::-1],  # Reverse for better display
            x=counts[::-1],
            orientation='h',
            marker=dict(color='steelblue')
        )
    ])

    fig.update_layout(
        title='Top 20 Most Searched Queries',
        xaxis_title='Total Searches',
        yaxis_title='Query',
        height=600,
        template='plotly_white'
    )

    return fig


def create_query_clustering_chart(queries: List[str]) -> go.Figure:
    """Create 2D visualization of query clusters using word embeddings"""
    if len(queries) < 10:
        # Not enough data for clustering
        return None

    try:
        # Create TF-IDF vectors
        vectorizer = TfidfVectorizer(max_features=100, stop_words='english')
        X = vectorizer.fit_transform(queries)

        # Reduce to 2D using PCA
        pca = PCA(n_components=2, random_state=42)
        X_2d = pca.fit_transform(X.toarray())

        # Cluster queries
        n_clusters = min(8, len(queries) // 50)  # Adaptive number of clusters
        if n_clusters < 2:
            n_clusters = 2

        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(X.toarray())

        # Create scatter plot
        df_plot = pd.DataFrame({
            'x': X_2d[:, 0],
            'y': X_2d[:, 1],
            'query': [q[:50] for q in queries],
            'cluster': clusters
        })

        fig = px.scatter(
            df_plot,
            x='x',
            y='y',
            color='cluster',
            hover_data=['query'],
            title='Query Clusters (Word Embedding Visualization)',
            labels={'x': 'PCA Component 1', 'y': 'PCA Component 2'},
            height=600
        )

        fig.update_traces(marker=dict(size=8, opacity=0.6))
        fig.update_layout(template='plotly_white')

        return fig

    except Exception as e:
        print(f"Warning: Could not create clustering visualization: {e}")
        return None


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
            marker=dict(color='coral')
        )
    ])

    fig.update_layout(
        title='Top 20 Most Active Users (Anonymized)',
        xaxis_title='Total Searches',
        yaxis_title='User ID',
        height=600,
        template='plotly_white'
    )

    return fig


def create_client_distribution_chart(client_totals: Dict[str, int]) -> go.Figure:
    """Create pie chart of search distribution by client"""
    fig = go.Figure(data=[
        go.Pie(
            labels=list(client_totals.keys()),
            values=list(client_totals.values()),
            hole=0.3
        )
    ])

    fig.update_layout(
        title='Search Distribution by Geographic Client',
        height=400,
        template='plotly_white'
    )

    return fig


def generate_html(stats: Dict, figures: Dict[str, go.Figure]) -> str:
    """Generate HTML dashboard"""

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
            background: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 40px;
            border-left: 4px solid #4CAF50;
            padding-left: 15px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .stat-card h3 {{
            margin: 0;
            font-size: 14px;
            opacity: 0.9;
            font-weight: 500;
        }}
        .stat-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .stat-card .label {{
            font-size: 12px;
            opacity: 0.8;
        }}
        .chart {{
            margin: 30px 0;
        }}
        .footer {{
            margin-top: 50px;
            text-align: center;
            color: #999;
            font-size: 14px;
            border-top: 1px solid #eee;
            padding-top: 20px;
        }}
        .timestamp {{
            color: #666;
            font-size: 14px;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Soulseek Research Network Dashboard</h1>

        <p class="timestamp">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Searches</h3>
                <div class="value">{stats['total_searches']:,}</div>
                <div class="label">Collected searches</div>
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
                <div class="value">{(datetime.fromisoformat(stats['last_search']) - datetime.fromisoformat(stats['first_search'])).days}</div>
                <div class="label">Days of data</div>
            </div>
        </div>

        <h2>Data Collection Overview</h2>
        <div class="chart">
            {chart_html['daily_flow']}
        </div>

        <div class="chart">
            {chart_html['client_distribution']}
        </div>

        <h2>Search Patterns Analysis</h2>
        <div class="chart">
            {chart_html['top_queries']}
        </div>

        <div class="chart">
            {chart_html.get('query_clustering', '<p>Not enough data for clustering visualization</p>')}
        </div>

        <h2>User Activity</h2>
        <div class="chart">
            {chart_html['user_activity']}
        </div>

        <div class="footer">
            <p>Soulseek Research Project | Data collected from distributed geographic locations</p>
            <p>All usernames are cryptographically hashed for privacy | Research use only</p>
            <p><a href="https://github.com/maxhaag/soulseek-research">View on GitHub</a></p>
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
        top_queries = query_top_queries(conn, limit=100)
        top_users = query_most_active_users(conn, limit=50)
        clustering_queries = query_all_queries_for_clustering(conn, limit=5000)

        print(f"Found {stats['total_searches']:,} total searches")
        print(f"Creating visualizations...")

        # Create all figures
        figures = {
            'daily_flow': create_daily_flow_chart(daily_stats),
            'top_queries': create_top_queries_chart(top_queries),
            'user_activity': create_user_activity_chart(top_users),
            'client_distribution': create_client_distribution_chart(stats['client_totals']),
            'query_clustering': create_query_clustering_chart(clustering_queries)
        }

        print("Generating HTML...")
        html = generate_html(stats, figures)

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
