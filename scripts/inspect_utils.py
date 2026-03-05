"""Shared helpers for inspect_* scripts (DB connection, formatting, section/field printing)."""

import os
from datetime import datetime

import psycopg2


def get_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def format_value(value, max_length: int | None = None) -> str:
    """Format a value for display, optionally truncating.

    Args:
        value: The value to format
        max_length: Maximum length before truncating. None = no truncation.
    """
    if value is None:
        return "—"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, list):
        if not value:
            return "[]"
        items = ", ".join(str(v) for v in value)
        return f"[{items}]"
    val_str = str(value)
    if max_length and len(val_str) > max_length:
        return val_str[:max_length] + "..."
    return val_str


def print_section(title: str):
    """Print a section header."""
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_field(name: str, value, indent: int = 0, max_length: int | None = None):
    """Print a field with optional indentation.

    Args:
        name: Field name
        value: Field value
        indent: Number of indentation levels
        max_length: Optional max length for truncation
    """
    prefix = "  " * indent
    formatted = format_value(value, max_length=max_length)
    print(f"{prefix}{name}: {formatted}")
