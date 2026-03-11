"""Tests for talent_matching.skills.resolver.

get_or_create_skill uses .scalars().first() for alias lookup; that returns a
single scalar (UUID), not a Row. Subscripting it would raise TypeError.
"""

import uuid
from unittest.mock import MagicMock

from talent_matching.skills.resolver import get_or_create_skill


def test_get_or_create_skill_returns_alias_skill_id_as_scalar():
    """When name matches an alias, get_or_create_skill returns that skill's ID.

    .scalars().first() returns the scalar value (UUID), not a Row. Code must
    not subscript the result (e.g. alias_skill_id[0]) or it raises
    TypeError: 'UUID' object is not subscriptable.
    """
    skill_id = uuid.uuid4()
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = skill_id
    session = MagicMock()
    session.execute.return_value = mock_result

    out = get_or_create_skill(session, "Python 3")

    assert out == skill_id
    # Alias path was used (execute called for alias lookup)
    session.execute.assert_called()
