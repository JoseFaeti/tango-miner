"""Shared grammar rule contract."""
from dataclasses import dataclass


@dataclass(frozen=True)
class GrammarRule:
    id: str
    expression: str
    reading: str
    meaning: str
    match: object
