"""Reset concurrency_limits and concurrency_slots tables.

Clears stale/corrupt state so Dagster can reinitialize. Keeps concurrency
definitions (op_tags, tag_concurrency_limits) unchanged.

Run: poetry run with-local-db python scripts/fix_dagster_concurrency_limits.py
On server: poetry run python scripts/fix_dagster_concurrency_limits.py --local

Stop Dagster first. After running, start Dagster again.
"""

from talent_matching.script_env import apply_local_db  # noqa: E402

apply_local_db()

from sqlalchemy import text  # noqa: E402

from talent_matching.db import get_session  # noqa: E402

session = get_session()

session.execute(text("TRUNCATE TABLE concurrency_slots"))
session.execute(text("TRUNCATE TABLE concurrency_limits"))
session.commit()
session.close()
print("Truncated concurrency_slots and concurrency_limits")
