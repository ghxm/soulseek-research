"""Report how fresh the server-side per-query tables are.

Prints key=value lines for the dashboard workflow: the newest date in
query_daily_stats, the date the query similarities were computed, and
stale=true when either is more than MAX_AGE_DAYS old.
"""
import os
from datetime import date, timedelta

import psycopg2

MAX_AGE_DAYS = 2


def main() -> None:
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    with conn, conn.cursor() as cur:
        cur.execute("SELECT max(date) FROM query_daily_stats")
        daily_max = cur.fetchone()[0]
        cur.execute("SELECT computed_at::date FROM query_similarities_meta WHERE id = 1")
        row = cur.fetchone()
        similar_date = row[0] if row else None
    conn.close()

    limit = date.today() - timedelta(days=MAX_AGE_DAYS)
    stale = (daily_max is None or similar_date is None
             or daily_max < limit or similar_date < limit)
    print(f"query_daily_max_date={daily_max}")
    print(f"similarities_computed_date={similar_date}")
    print(f"stale={'true' if stale else 'false'}")


if __name__ == '__main__':
    main()
