"""Apply --local DB env so scripts can run on the server without a tunnel.

When --local is in sys.argv, loads .env and sets POSTGRES_HOST=localhost and
POSTGRES_PORT=5432. Call this at the start of any script that uses the DB,
before get_session() or get_connection().

  On the server: poetry run python scripts/foo.py --local
  From laptop (tunnel): poetry run with-remote-db python scripts/foo.py
"""

import os
import sys


def apply_local_db() -> None:
    """If --local is in argv, load .env and set local Postgres env. Idempotent."""
    if "--local" not in sys.argv:
        return
    from dotenv import load_dotenv

    load_dotenv()
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
