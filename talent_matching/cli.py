import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def remote_ui():
    script = PROJECT_ROOT / "scripts" / "dagster-remote-ui.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def setup_db():
    script = PROJECT_ROOT / "scripts" / "setup-db-local.sh"
    os.execvp("bash", ["bash", str(script)] + sys.argv[1:])


def local_dev():
    os.chdir(PROJECT_ROOT)
    os.environ.setdefault("DAGSTER_HOME", str(PROJECT_ROOT))
    os.execvp(
        sys.executable,
        [sys.executable, "-m", "dagster", "dev", "-m", "talent_matching.definitions"]
        + sys.argv[1:],
    )


def deploy():
    """Pull latest code, rebuild containers, restart the stack."""
    os.chdir(PROJECT_ROOT)

    steps = [
        ("Pulling latest code", ["git", "pull"]),
        (
            "Rebuilding and restarting stack",
            ["docker", "compose", "-f", "docker-compose.prod.yml", "up", "--build", "-d"],
        ),
    ]

    for label, cmd in steps:
        print(f"  {label}...")
        subprocess.run(cmd, check=True)

    print()
    print("Deploy complete. Checking service status...")
    subprocess.run(["docker", "compose", "-f", "docker-compose.prod.yml", "ps"])
