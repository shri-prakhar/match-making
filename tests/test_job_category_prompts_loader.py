"""Tests for per-job-category prompt loader (DB or in-code default).

Ensures get_cv_extraction_prompt and get_refinement_prompt always return
usable text: in-code default when category is missing or has no DB row.
"""

from unittest.mock import MagicMock, patch

from talent_matching.llm.job_category_prompts_loader import (
    DEFAULT_CV_EXTRACTION_PROMPT,
    DEFAULT_REFINEMENT_PROMPT,
    get_cv_extraction_prompt,
    get_refinement_prompt,
)


class TestGetCvExtractionPrompt:
    """get_cv_extraction_prompt returns in-code default when no DB row."""

    def test_none_returns_default(self):
        out = get_cv_extraction_prompt(None)
        assert out == DEFAULT_CV_EXTRACTION_PROMPT
        assert len(out) > 50

    def test_empty_string_returns_default(self):
        out = get_cv_extraction_prompt("")
        assert out == DEFAULT_CV_EXTRACTION_PROMPT

    def test_whitespace_only_returns_default(self):
        out = get_cv_extraction_prompt("   ")
        assert out == DEFAULT_CV_EXTRACTION_PROMPT

    def test_unknown_category_returns_default(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.return_value = None
        with patch(
            "talent_matching.llm.job_category_prompts_loader.get_session", return_value=mock_session
        ):
            out = get_cv_extraction_prompt("UnknownCategory")
        assert out == DEFAULT_CV_EXTRACTION_PROMPT
        assert "technical" in out.lower() or "skills" in out.lower()


class TestGetRefinementPrompt:
    """get_refinement_prompt returns in-code default when no DB row."""

    def test_none_returns_default(self):
        out = get_refinement_prompt(None)
        assert out == DEFAULT_REFINEMENT_PROMPT
        assert len(out) > 20

    def test_empty_string_returns_default(self):
        out = get_refinement_prompt("")
        assert out == DEFAULT_REFINEMENT_PROMPT

    def test_unknown_category_returns_default(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.return_value = None
        with patch(
            "talent_matching.llm.job_category_prompts_loader.get_session", return_value=mock_session
        ):
            out = get_refinement_prompt("UnknownCategory")
        assert out == DEFAULT_REFINEMENT_PROMPT
        assert "must-have" in out.lower() or "fit" in out.lower()


class TestDefaultPromptsNonEmpty:
    """In-code defaults are non-empty and usable."""

    def test_cv_default_non_empty(self):
        assert DEFAULT_CV_EXTRACTION_PROMPT
        assert len(DEFAULT_CV_EXTRACTION_PROMPT.strip()) > 30

    def test_refinement_default_non_empty(self):
        assert DEFAULT_REFINEMENT_PROMPT
        assert len(DEFAULT_REFINEMENT_PROMPT.strip()) > 20


class TestBuildSystemPromptConcatenation:
    """Category-aware CV prompt concatenates per-category prompts."""

    def test_build_system_prompt_with_categories_includes_header_and_default(self):
        from talent_matching.llm.operations.normalize_cv import (
            SYSTEM_PROMPT,
            _build_system_prompt,
        )

        with patch(
            "talent_matching.llm.operations.normalize_cv.get_cv_extraction_prompt",
            return_value=DEFAULT_CV_EXTRACTION_PROMPT,
        ):
            base_len = len(SYSTEM_PROMPT)
            out = _build_system_prompt(SYSTEM_PROMPT, ["Backend Developer"])
        assert len(out) > base_len
        assert "For this candidate we match to the following role categories" in out
        assert "Backend Developer" in out
        assert DEFAULT_CV_EXTRACTION_PROMPT in out

    def test_build_system_prompt_empty_categories_returns_base_unchanged(self):
        from talent_matching.llm.operations.normalize_cv import (
            SYSTEM_PROMPT,
            _build_system_prompt,
        )

        assert _build_system_prompt(SYSTEM_PROMPT, None) == SYSTEM_PROMPT
        assert _build_system_prompt(SYSTEM_PROMPT, []) == SYSTEM_PROMPT
