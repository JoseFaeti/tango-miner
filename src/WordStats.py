from dataclasses import dataclass

@dataclass(frozen=False)
class WordStats:
    index: int
    frequency: int
    reading: str
    definition: str
    tags: set[str]
    sources: set[str]
