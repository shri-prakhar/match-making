import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

COMPOSE_FILE = "docker-compose.prod.yml"
DRAIN_TIMEOUT_SECONDS = 300
DRAIN_POLL_INTERVAL = 10


def remote_ui() -> None:
    script = PROJECT_ROOT / "scripts" / "dagster-remote-ui.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def setup_db() -> None:
    script = PROJECT_ROOT / "scripts" / "setup-db-local.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def migrate_dagster() -> None:
    """Run Dagster instance migration (fixes concurrency_limits schema). Uses local Postgres."""
    load_dotenv(PROJECT_ROOT / ".env")
    os.chdir(PROJECT_ROOT)
    os.environ.setdefault("DAGSTER_HOME", str(PROJECT_ROOT))
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
    subprocess.run(
        [sys.executable, "-m", "dagster", "instance", "migrate"],
        cwd=PROJECT_ROOT,
        check=True,
    )


def local_dev() -> None:
    """Run Dagster dev entirely locally: webserver, daemon, and code. Uses local Postgres."""
    load_dotenv(PROJECT_ROOT / ".env")
    os.chdir(PROJECT_ROOT)
    os.environ.setdefault("DAGSTER_HOME", str(PROJECT_ROOT))
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
    workspace = PROJECT_ROOT / "workspace.yaml"
    os.execvp(
        sys.executable,
        [sys.executable, "-m", "dagster", "dev", "-w", str(workspace)] + sys.argv[1:],
    )


def local_matchmaking() -> None:
    """Run matchmaking locally with remote DB (tunnel) for fast debug loops."""
    script = PROJECT_ROOT / "scripts" / "local-matchmaking-dev.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def _run_cmd(cmd: list[str] | str, **kwargs: Any) -> subprocess.CompletedProcess[Any]:
    """Run a command, printing it for visibility."""
    return subprocess.run(cmd, **kwargs)


def _ssh_cmd(remote_host: str, command: str, **kwargs: Any) -> subprocess.CompletedProcess[Any]:
    return _run_cmd(["ssh", remote_host, command], **kwargs)


def _get_in_progress_count(remote_host: str | None, remote_dir: str) -> int:
    """Query Postgres inside Docker for in-progress run count."""
    psql = (
        "docker exec talent_matching_db psql -U talent -d talent_matching -t -A "
        "-c \"SELECT COUNT(*) FROM runs WHERE status IN ('STARTED','STARTING')\""
    )
    if remote_host:
        result = _ssh_cmd(remote_host, psql, capture_output=True, text=True)
    else:
        result = _run_cmd(psql, shell=True, capture_output=True, text=True)
    return int(result.stdout.strip()) if result.returncode == 0 else -1


def _drain_runs(remote_host: str | None, remote_dir: str) -> None:
    """Stop daemon to prevent new runs, then wait for in-progress runs to finish."""
    compose = f"docker compose -f {COMPOSE_FILE}"

    print("  Stopping dagster-daemon (no new runs will be dequeued)...")
    stop_cmd = f"cd {remote_dir} && {compose} stop dagster-daemon"
    if remote_host:
        _ssh_cmd(remote_host, stop_cmd)
    else:
        _run_cmd(stop_cmd, shell=True)

    deadline = time.time() + DRAIN_TIMEOUT_SECONDS
    while time.time() < deadline:
        count = _get_in_progress_count(remote_host, remote_dir)
        if count <= 0:
            print(f"  All runs drained (count={count}).")
            return
        remaining = int(deadline - time.time())
        print(f"  {count} run(s) still in progress, waiting... ({remaining}s remaining)")
        time.sleep(DRAIN_POLL_INTERVAL)

    count = _get_in_progress_count(remote_host, remote_dir)
    if count > 0:
        print(f"  WARNING: Drain timeout reached with {count} run(s) still in progress.")
        print("  Proceeding with deploy — run_monitoring will mark orphaned runs as failed.")


def deploy() -> None:
    """Deploy to remote server (git pull + docker compose) and copy .env.

    Gracefully drains in-progress runs before rebuilding to avoid orphaned runs.
    Requires REMOTE_HOST in .env. Use --local when already SSH'd into the server.
    Use --force to skip the drain step.
    """
    load_dotenv(PROJECT_ROOT / ".env")
    os.chdir(PROJECT_ROOT)

    remote_host = os.environ.get("REMOTE_HOST", "").strip()
    remote_dir = os.environ.get("REMOTE_PROJECT_DIR", "/root/match-making").strip()

    args = sys.argv[1:]
    deploy_local = "--local" in args
    force = "--force" in args

    if not deploy_local and not remote_host:
        print("  Error: REMOTE_HOST not set in .env.")
        print("  Set REMOTE_HOST=user@your-server in .env to deploy to remote.")
        print("  Or run 'poetry run deploy --local' when already on the server.")
        sys.exit(1)

    target = remote_host if not deploy_local else None

    if not force:
        _drain_runs(target, remote_dir)
    else:
        print("  Skipping drain (--force).")

    if deploy_local:
        print("  Pulling latest code...")
        _run_cmd(["git", "pull"], check=True)
        print("  Rebuilding and restarting stack...")
        _run_cmd(
            ["docker", "compose", "-f", COMPOSE_FILE, "up", "--build", "-d"],
            check=True,
        )
    else:
        env_path = PROJECT_ROOT / ".env"
        if not env_path.exists():
            print("  Error: .env not found. Create it before deploying.")
            sys.exit(1)
        print(f"  Copying .env to {remote_host}:{remote_dir}/...")
        _run_cmd(
            ["scp", str(env_path), f"{remote_host}:{remote_dir}/.env"],
            check=True,
        )
        print(f"  Pulling code and rebuilding on {remote_host}...")
        _ssh_cmd(
            remote_host,
            f"cd {remote_dir} && git fetch origin && git reset --hard origin/main && docker compose -f {COMPOSE_FILE} up --build -d",
            check=True,
        )

    print()
    print("Deploy complete. Checking service status...")
    if deploy_local:
        _run_cmd(["docker", "compose", "-f", COMPOSE_FILE, "ps"], check=True)
    else:
        _ssh_cmd(
            remote_host,
            f"cd {remote_dir} && docker compose -f {COMPOSE_FILE} ps",
            check=True,
        )


def _run_with_db_env(port: str | int) -> None:
    """Set POSTGRES_* for DB access, then exec remaining args. Caller must load .env first."""
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = str(port)

    args = sys.argv[1:]
    if not args:
        print("Usage: poetry run with-local-db <command> [args...]", file=sys.stderr)
        print("       poetry run with-remote-db <command> [args...]", file=sys.stderr)
        print(
            "Example: poetry run with-remote-db python scripts/inspect_matches.py recXXX",
            file=sys.stderr,
        )
        sys.exit(1)

    os.execvp(args[0], args)


def with_local_db() -> None:
    """Run a command with local DB env (POSTGRES_HOST=localhost, POSTGRES_PORT=5432)."""
    load_dotenv(PROJECT_ROOT / ".env")
    port = os.environ.get("POSTGRES_PORT", "5432")
    _run_with_db_env(port)


def with_remote_db() -> None:
    """Run a command with remote DB tunnel env (POSTGRES_HOST=localhost, POSTGRES_PORT=15432)."""
    load_dotenv(PROJECT_ROOT / ".env")
    port = os.environ.get("POSTGRES_REMOTE_TUNNEL_PORT", "15432")
    _run_with_db_env(port)
