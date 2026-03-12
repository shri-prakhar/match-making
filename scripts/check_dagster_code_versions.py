#!/usr/bin/env python3
"""Pre-commit hook to verify Dagster asset code_version is updated when asset code changes.

This script checks staged files for changes to @asset decorated functions and ensures
that the code_version parameter was also updated when the asset's implementation changes.

Usage:
    python scripts/check_dagster_code_versions.py [--verbose]

Exit codes:
    0: All checks passed
    1: Asset code changed without code_version update
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AssetInfo:
    """Information about a Dagster asset."""

    name: str
    file_path: str
    line_number: int
    code_version: str | None


def get_staged_python_files() -> list:
    """Get list of staged Python files."""
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        capture_output=True,
        text=True,
    )
    files = []
    for line in result.stdout.strip().split("\n"):
        if line.endswith(".py"):
            path = Path(line)
            if path.exists():
                files.append(path)
    return files


def get_file_content_before_staging(file_path: Path) -> str:
    """Get the file content from the last commit (before staging)."""
    result = subprocess.run(
        ["git", "show", f"HEAD:{file_path}"],
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else ""


def extract_code_version_value(decorator_node: ast.Call) -> str | None:
    """Extract code_version literal value from decorator.

    Only extracts string literals. Variable references are not supported
    to keep the hook simple and the versioning explicit.
    """
    for keyword in decorator_node.keywords:
        if keyword.arg == "code_version":
            if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                return keyword.value.value
            # If it's a variable reference, return a marker so we know it exists
            elif isinstance(keyword.value, ast.Name):
                return f"<variable:{keyword.value.id}>"
    return None


def find_assets_in_code(code: str, file_path: str) -> dict:
    """Parse Python code and find all @asset decorated functions."""
    assets = {}

    tree = ast.parse(code)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                decorator_name = None
                code_version = None

                if isinstance(decorator, ast.Name) and decorator.id == "asset":
                    decorator_name = "asset"
                elif isinstance(decorator, ast.Call):
                    if isinstance(decorator.func, ast.Name) and decorator.func.id == "asset":
                        decorator_name = "asset"
                        code_version = extract_code_version_value(decorator)

                if decorator_name == "asset":
                    assets[node.name] = AssetInfo(
                        name=node.name,
                        file_path=file_path,
                        line_number=node.lineno,
                        code_version=code_version,
                    )

    return assets


def get_asset_body_hash(code: str, asset_name: str) -> str | None:
    """Get a hash-like representation of an asset function's body.

    This excludes comments and docstrings for comparison purposes.
    """
    tree = ast.parse(code)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == asset_name:
            # Remove docstring from body for comparison
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body = body[1:]

            # Convert AST back to a normalized string representation
            return ast.dump(ast.Module(body=body, type_ignores=[]))

    return None


def check_asset_code_versions(verbose: bool = False) -> list:
    """Check all staged files for asset changes without code_version updates.

    Returns a list of error messages for assets that changed without version bumps.
    """
    errors = []
    warnings = []
    staged_files = get_staged_python_files()

    if verbose:
        print(f"Checking {len(staged_files)} staged Python files...")

    for file_path in staged_files:
        if verbose:
            print(f"  Checking {file_path}...")

        new_code = file_path.read_text()
        old_code = get_file_content_before_staging(file_path)

        # Skip new files - they don't need version comparison
        if not old_code:
            if verbose:
                print("    New file, skipping...")
            continue

        new_assets = find_assets_in_code(new_code, str(file_path))
        old_assets = find_assets_in_code(old_code, str(file_path))

        for asset_name, new_asset in new_assets.items():
            # Skip assets without code_version - they rely on auto-detection
            if new_asset.code_version is None:
                if verbose:
                    print(f"    {asset_name}: No code_version, using auto-detection (OK)")
                continue

            # Warn about variable references
            if new_asset.code_version.startswith("<variable:"):
                var_name = new_asset.code_version[10:-1]
                warnings.append(
                    f"{file_path}:{new_asset.line_number}: "
                    f"Asset '{asset_name}' uses variable '{var_name}' for code_version. "
                    f"Use a literal string for proper pre-commit checking."
                )
                if verbose:
                    print(f"    {asset_name}: Uses variable reference (WARNING)")
                continue

            # Get old asset info if it existed
            old_asset = old_assets.get(asset_name)

            if old_asset is None:
                # New asset, no version check needed
                if verbose:
                    print(f"    {asset_name}: New asset (OK)")
                continue

            # Compare asset body
            new_body = get_asset_body_hash(new_code, asset_name)
            old_body = get_asset_body_hash(old_code, asset_name)

            if new_body == old_body:
                if verbose:
                    print(f"    {asset_name}: No code changes (OK)")
                continue

            # Asset body changed - check if version was updated
            new_version = new_asset.code_version
            old_version = old_asset.code_version

            if new_version == old_version:
                error_msg = (
                    f"{file_path}:{new_asset.line_number}: "
                    f"Asset '{asset_name}' code changed but code_version "
                    f"was not updated (still '{new_version}')"
                )
                errors.append(error_msg)
                if verbose:
                    print(f"    {asset_name}: FAILED - code changed without version bump")
            else:
                if verbose:
                    print(f"    {asset_name}: Version updated {old_version} -> {new_version} (OK)")

    return errors, warnings


def main():
    parser = argparse.ArgumentParser(description="Check Dagster asset code_version updates")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show verbose output")
    args = parser.parse_args()

    errors, warnings = check_asset_code_versions(verbose=args.verbose)

    if warnings:
        print("\n⚠️  Warnings:\n")
        for warning in warnings:
            print(f"  {warning}\n")

    if errors:
        print("\n❌ Dagster code_version check failed:\n")
        for error in errors:
            print(f"  {error}\n")
        print("Assets with code_version must update it when their code changes.")
        print("This ensures Dagster's staleness detection works correctly.\n")
        print("To fix:")
        print("  1. Bump the code_version string in the @asset decorator, OR")
        print("  2. Remove code_version to use automatic detection instead")
        sys.exit(1)
    else:
        if args.verbose:
            print("\n✓ All Dagster code_version checks passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
