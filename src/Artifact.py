from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass
class Artifact:
    data: Any            # could be Path, tokens, dict, etc.
    is_path: bool = True # hint, not a rule
