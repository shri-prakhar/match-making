import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def remote_ui():
    script = PROJECT_ROOT / "scripts" / "dagster-remote-ui.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def setup_db():
    script = PROJECT_ROOT / "scripts" / "setup-db-local.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def migrate_dagster():
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


def local_dev():
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


def local_matchmaking():
    """Run matchmaking locally with remote DB (tunnel) for fast debug loops."""
    script = PROJECT_ROOT / "scripts" / "local-matchmaking-dev.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def deploy():
    """Pull latest code, rebuild containers, restart the stack.

    If REMOTE_HOST is set in .env, deploys to the remote server: copies .env
    and runs git pull + docker compose there. Otherwise runs locally (for
    when you're already SSH'd into the server).
    """
    load_dotenv(PROJECT_ROOT / ".env")
    os.chdir(PROJECT_ROOT)

    remote_host = os.environ.get("REMOTE_HOST", "").strip()
    remote_dir = os.environ.get("REMOTE_PROJECT_DIR", "/root/match-making").strip()

    if remote_host:
        # Deploy to remote: copy .env, then run deploy commands over SSH
        env_path = PROJECT_ROOT / ".env"
        if not env_path.exists():
            print("  Error: .env not found. Create it before deploying.")
            sys.exit(1)
        print(f"  Copying .env to {remote_host}:{remote_dir}/...")
        subprocess.run(
            ["scp", str(env_path), f"{remote_host}:{remote_dir}/.env"],
            check=True,
        )
        print(f"  Running deploy on {remote_host}...")
        subprocess.run(
            [
                "ssh",
                remote_host,
                f"cd {remote_dir} && git pull && docker compose -f docker-compose.prod.yml up --build -d",
            ],
            check=True,
        )
    else:
        # Run locally (e.g. already on the server)
        steps = [
            ("Pulling latest code", ["git", "pull"]),
            (
                "Rebuilding and restarting stack",
                [
                    "docker",
                    "compose",
                    "-f",
                    "docker-compose.prod.yml",
                    "up",
                    "--build",
                    "-d",
                ],
            ),
        ]
        for label, cmd in steps:
            print(f"  {label}...")
            subprocess.run(cmd, check=True)

    print()
    print("Deploy complete. Checking service status...")
    if remote_host:
        subprocess.run(
            [
                "ssh",
                remote_host,
                f"cd {remote_dir} && docker compose -f docker-compose.prod.yml ps",
            ],
            check=True,
        )
    else:
        subprocess.run(["docker", "compose", "-f", "docker-compose.prod.yml", "ps"], check=True)


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


def with_local_db():
    """Run a command with local DB env (POSTGRES_HOST=localhost, POSTGRES_PORT=5432)."""
    load_dotenv(PROJECT_ROOT / ".env")
    port = os.environ.get("POSTGRES_PORT", "5432")
    _run_with_db_env(port)


def with_remote_db():
    """Run a command with remote DB tunnel env (POSTGRES_HOST=localhost, POSTGRES_PORT=15432)."""
    load_dotenv(PROJECT_ROOT / ".env")
    port = os.environ.get("POSTGRES_REMOTE_TUNNEL_PORT", "15432")
    _run_with_db_env(port)
