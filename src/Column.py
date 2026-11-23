# ruff: noqa: N999 -- Preserve established public module names.
from enum import IntEnum


class Column(IntEnum):
    WORD = 0
    INDEX = 1
    FREQUENCY = 2
    NORMALIZED_FREQUENCY = 3
    KANA = 4
    DEFINITION = 5
