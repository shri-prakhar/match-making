"""Dagster sensors for the talent matching pipeline."""

from talent_matching.sensors.airtable_sensor import (
    airtable_candidate_sensor,
)
from talent_matching.sensors.alerting import system_health_sensor
from talent_matching.sensors.ats_matchmaking_sensor import ats_matchmaking_sensor
from talent_matching.sensors.run_failure_sensor import run_failure_tagger

__all__ = [
    "airtable_candidate_sensor",
    "ats_matchmaking_sensor",
    "run_failure_tagger",
    "system_health_sensor",
]
