"""Tests for scripts/inspect_utils (shared inspect helpers)."""

import os
import sys
from datetime import datetime
from io import StringIO
from unittest.mock import patch

# Import from scripts package; tests run from project root so scripts is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.inspect_utils import format_value, print_field, print_section


class TestFormatValue:
    """Tests for format_value."""

    def test_none_returns_em_dash(self):
        assert format_value(None) == "—"

    def test_datetime_formatted(self):
        dt = datetime(2025, 3, 2, 14, 30, 0)
        assert "2025-03-02" in format_value(dt)
        assert "14:30" in format_value(dt)

    def test_list_formatted(self):
        assert format_value([]) == "[]"
        assert format_value([1, 2, 3]) == "[1, 2, 3]"

    def test_string_no_truncation(self):
        assert format_value("hello") == "hello"

    def test_truncation_with_max_length(self):
        out = format_value("hello world", max_length=5)
        assert out == "hello..."
        assert len(out) == 8

    def test_truncation_not_applied_when_short(self):
        assert format_value("hi", max_length=10) == "hi"

    def test_number_formatted(self):
        assert format_value(42) == "42"
        assert format_value(3.14) == "3.14"


class TestPrintSection:
    """Tests for print_section."""

    def test_prints_header_with_equal_signs(self):
        buf = StringIO()
        with patch("sys.stdout", buf):
            print_section("Test Section")
        out = buf.getvalue()
        assert "Test Section" in out
        assert "=" * 60 in out


class TestPrintField:
    """Tests for print_field."""

    def test_prints_name_and_value(self):
        buf = StringIO()
        with patch("sys.stdout", buf):
            print_field("name", "value")
        assert "name:" in buf.getvalue()
        assert "value" in buf.getvalue()

    def test_indent_adds_spaces(self):
        buf = StringIO()
        with patch("sys.stdout", buf):
            print_field("x", 1, indent=2)
        line = buf.getvalue()
        assert line.startswith("    ")

    def test_uses_format_value(self):
        buf = StringIO()
        with patch("sys.stdout", buf):
            print_field("n", None)
        assert "—" in buf.getvalue()
