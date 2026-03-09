"""Centralized scoring weights for matchmaking.

Used by the matches asset, analysis scripts, and reporting tools.
Tune these based on human vs system analysis to improve match quality.
"""

# Vector similarity sub-weights (role 40%, domain 35%, culture 25%)
ROLE_WEIGHT = 0.4
DOMAIN_WEIGHT = 0.35
CULTURE_WEIGHT = 0.25

# Combined score blend: 35% vector + 40% skill fit + 10% comp + 15% location − seniority deduction
VECTOR_WEIGHT = 0.35
SKILL_FIT_WEIGHT = 0.40
COMPENSATION_WEIGHT = 0.10
LOCATION_WEIGHT = 0.15

# Skill fit: when at least one required skill matches, 80% rating-based coverage, 20% semantic
SKILL_RATING_WEIGHT = 0.8
SKILL_SEMANTIC_WEIGHT = 0.2
