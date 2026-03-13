"""Job category normalization and resolution to canonical (Talent) taxonomy."""


def norm_cat(s: str) -> str:
    """Normalize category for comparison: strip, remove surrounding quotes, lower."""
    s = (s or "").strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    return s.lower()


def resolve_desired_job_categories_to_canonical(
    raw_list: list[str],
    canonical_list: list[str],
) -> list[str]:
    """Resolve raw desired job category strings to canonical (Talent) values only.

    Each raw value is normalized and matched to exactly one canonical value (exact
    match after normalization). Non-matching values are dropped. Returns
    deduplicated list of canonical strings in order of first occurrence.
    """
    if not canonical_list:
        return []
    canonical_norm_to_value = {norm_cat(c): c for c in canonical_list if (c or "").strip()}
    if not canonical_norm_to_value:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for raw in raw_list or []:
        n = norm_cat(raw)
        if not n:
            continue
        canonical = canonical_norm_to_value.get(n)
        if canonical is not None and canonical not in seen:
            seen.add(canonical)
            result.append(canonical)
    return result
