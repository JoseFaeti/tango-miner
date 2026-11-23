# ruff: noqa: N999 -- Preserve established public module names.
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Artifact:
    data: Any
    tmpdir: Path | None = None
    is_path: bool = True
    sentences: list = field(default_factory=list)
