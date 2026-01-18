from dataclasses import dataclass

@dataclass(frozen=False)
class WordStats:
    index: int
    frequency: int
    score: float
    reading: str
    definition: str
    tags: set[str]
    sentences: list[str]
    lemma: str
    pos: list[str]