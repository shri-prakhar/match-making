#!/usr/bin/env python3
"""List all skills from the skills table.

Usage:
    poetry run with-local-db python scripts/list_skills.py
    poetry run with-remote-db python scripts/list_skills.py
    On server: poetry run python scripts/list_skills.py --local

For remote: poetry run remote-ui or poetry run local-matchmaking must be running.
"""

import os
import sys

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.script_env import apply_local_db
from scripts.inspect_utils import get_connection


def main() -> None:
    apply_local_db()
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    print(f"DB: {host}:{port}")
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT name, slug, is_active, created_by
           FROM skills
           ORDER BY name"""
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Total skills: {len(rows)}\n")
    for r in rows:
        active = "" if r["is_active"] else " (inactive)"
        print(f"  {r['name']}  [{r['slug']}]{active}")


if __name__ == "__main__":
    main()
