#!/usr/bin/env python3
"""Check whether materialized candidate partitions are on the latest code version.

Queries the Dagster instance (event log / asset storage) for each candidate-partitioned
asset that has a code_version, and reports partitions whose latest materialization used
an older code version than the current one in code.

Prerequisites:
  - Dagster instance must use the same Postgres as the one you want to check (local or
    remote). Use poetry run with-local-db or poetry run with-remote-db so POSTGRES_*
    points at the right DB; DAGSTER_HOME can be unset (defaults to project root).

Usage:
  poetry run with-remote-db python scripts/check_candidate_partitions_code_version.py
  poetry run with-local-db python scripts/check_candidate_partitions_code_version.py
  DAGSTER_HOME=/path/to/dagster_home poetry run python scripts/check_candidate_partitions_code_version.py

Note: With many candidate partitions, the script may take a few minutes (one event-log query
per asset per materialized partition).
"""

import os
import sys
from pathlib import Path

from dagster import AssetKey, DagsterInstance
from dagster._core.definitions.data_version import extract_data_provenance_from_entry
from dagster._core.event_api import AssetRecordsFilter
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Ensure we find dagster.yaml when DAGSTER_HOME is not set (e.g. when using with-remote-db)
if not os.environ.get("DAGSTER_HOME"):
    os.environ["DAGSTER_HOME"] = str(PROJECT_ROOT)

CANDIDATE_PARTITIONS_DEF_NAME = "candidates"


def get_current_code_versions_by_asset() -> dict[AssetKey, str]:
    """Build map of asset_key -> current code_version for candidate-partitioned assets with a code_version."""
    from talent_matching.definitions import all_assets

    out: dict[AssetKey, str] = {}
    for asset_def in all_assets:
        partitions_def = getattr(asset_def, "partitions_def", None)
        if (
            not partitions_def
            or getattr(partitions_def, "name", None) != CANDIDATE_PARTITIONS_DEF_NAME
        ):
            continue
        code_versions = getattr(asset_def, "code_versions_by_key", None) or {}
        for key, version in code_versions.items():
            if version is not None and isinstance(version, str):
                out[key] = version
    return out


def main() -> int:
    instance = DagsterInstance.get()
    current = get_current_code_versions_by_asset()
    if not current:
        print("No candidate-partitioned assets with code_version found in definitions.")
        return 0

    print("Candidate assets with code_version (current in code):")
    for key in sorted(current.keys(), key=lambda k: k.to_user_string()):
        print(f"  {key.to_user_string()}: {current[key]}")
    print()

    stale: list[
        tuple[str, AssetKey, str, str]
    ] = []  # (partition_key, asset_key, materialized_version, current_version)
    up_to_date_counts: dict[AssetKey, int] = {k: 0 for k in current}

    for asset_key, expected_version in current.items():
        materialized_partitions = instance.get_materialized_partitions(asset_key)
        for partition_key in materialized_partitions:
            result = instance.fetch_materializations(
                AssetRecordsFilter(asset_key=asset_key, asset_partitions=[partition_key]),
                limit=1,
                ascending=False,
            )
            if not result.records:
                continue
            record = result.records[0]
            entry = record.event_log_entry
            provenance = extract_data_provenance_from_entry(entry)
            materialized_version = provenance.code_version if provenance else None
            if materialized_version is None:
                stale.append((partition_key, asset_key, "<none>", expected_version))
            elif materialized_version != expected_version:
                stale.append((partition_key, asset_key, materialized_version, expected_version))
            else:
                up_to_date_counts[asset_key] += 1

    # Summary
    for asset_key in current:
        total = len(instance.get_materialized_partitions(asset_key))
        ok = up_to_date_counts[asset_key]
        print(
            f"{asset_key.to_user_string()}: {ok}/{total} partitions on latest code version ({current[asset_key]})"
        )
    print()

    if stale:
        print("Partitions NOT on latest code version:")
        for partition_key, asset_key, materialized_version, expected_version in sorted(
            stale, key=lambda x: (x[1].to_user_string(), x[0])
        ):
            print(
                f"  {asset_key.to_user_string()} partition={partition_key}  materialized={materialized_version}  current={expected_version}"
            )
        return 1

    print("All materialized candidate partitions are on the latest code version.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
