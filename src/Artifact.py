from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

@dataclass
class Artifact:
    data: Any
    tmpdir: Optional[Path] = None
    is_path: bool = True
