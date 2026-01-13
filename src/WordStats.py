from dataclasses import dataclass

@dataclass(frozen=False)
class WordStats:
    index: int
    index_score: float
    frequency: int
    score: float
    reading: str
    definition: str
    tags: set[str]