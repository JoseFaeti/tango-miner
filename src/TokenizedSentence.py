from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TokenizedSentence:
    """A normalized sentence together with its token stream and source metadata."""

    text: str
    tag: str | None
    origin: Path
    tokens: list[dict]