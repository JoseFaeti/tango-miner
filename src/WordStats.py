import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Sentence:
    text: str
    tag: str
    origin: str
    surface_form: str
    score: float

    def __str__(self) -> str:
        return f'{self.text}'#' [{self.tag}][{self.origin}][{self.surface_form}]'

    def to_html(self) -> str:
        text = self.text

        pattern = re.escape(self.surface_form)
        text = re.sub(
            pattern,
            rf"<span class='highlight'>{self.surface_form}</span>",
            text,
            count=1
        )

        return f"{text}<br><span class='tag sentence-tag'>{self.tag} - {self.score}</span>"

@dataclass(frozen=False)
class WordStats:
    __slots__ = (
        "index", "index_count", "frequency", "score", "reading",
        "definition", "tags", "sentences", "lemma",
        "pos", "invalid"
    )
    index: float       # running mean of normalized position (0=start, 1=end)
    index_count: int   # number of files that have contributed to index
    frequency: int
    score: float
    reading: str
    definition: str
    tags: set[str]
    sentences: list[Sentence]
    lemma: str
    pos: tuple[str, ...]
    invalid: bool