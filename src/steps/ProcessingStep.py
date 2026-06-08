from enum import IntEnum

class ProcessingStep(IntEnum):
    TOKENIZING = 0
    FILTERING = 1
    READINGS = 2
    DEFINITIONS = 3
    SCORING = 4
    SENTENCES = 5
    ANKI_EXPORT = 6
    SENTENCE_EXTRACTION = 7