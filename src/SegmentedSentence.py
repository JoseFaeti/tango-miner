from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class SegmentedSentence:
    """
    A sentence as extracted by the tokenizer, before being attached to any word.

    text:           The sentence string.
    tag:            Source tag derived from the filename (e.g. "[anime_s01]"), or None.
    origin:         Path to the file the sentence came from.
    lemma_surfaces: Maps every meaningful lemma that appeared in this sentence
                    to its surface form (the exact characters in the original text).
                    Used both for highlighting and for difficulty scoring.
    """
    text: str
    tag: str | None
    origin: Path
    lemma_surfaces: dict[str, str]