from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass
class Artifact:
    data: Any
    tmpdir: Optional[Path] = None
    is_path: bool = True
    sentences: list = field(default_factory=list)