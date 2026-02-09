import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Sentence:
    text: str
    tag: str
    origin: str
    surface_form: str

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

        return f"{text}<br><span class='tag sentence-tag'>{self.tag}</span>"

@dataclass(frozen=False)
class WordStats:
    __slots__ = (
        "index", "frequency", "score", "reading",
        "definition", "tags", "sentences", "lemma",
        "pos", "invalid"
    )
    index: int
    frequency: int
    score: float
    reading: str
    definition: str
    tags: set[str]
    sentences: list[Sentence]
    lemma: str
    pos: tuple[str, ...]
    invalid: bool