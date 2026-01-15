from enum import IntEnum

class ProcessingStep(IntEnum):
    TOKENIZING = 0
    FILTERING = 1
    READINGS = 2
    DEFINITIONS = 3
    SCORING = 4
    ANKI_EXPORT = 5