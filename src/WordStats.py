from dataclasses import dataclass


@dataclass(frozen=True)
class Sentence:
    text: str
    tag: str
    origin: str
    
@dataclass(frozen=False)
class WordStats:
    index: int
    frequency: int
    score: float
    reading: str
    definition: str
    tags: set[str]
    sentences: list[Sentence]
    lemma: str
    pos: list[str]