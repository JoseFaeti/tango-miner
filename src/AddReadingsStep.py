import csv

from collections import OrderedDict
from fugashi import Tagger
from pathlib import Path

from .Artifact import Artifact
from .Column import Column
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep

class AddReadingsStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        # output_path = artifact.tmpdir / "-3.readings.tmp"
        data = add_readings(artifact.data, self.progress)
        return Artifact(data)


def add_readings(input, progress_handler):
    def kata_to_hira(text: str) -> str:
        """Convert katakana to hiragana."""
        result = []
        for ch in text:
            code = ord(ch)
            if 0x30A1 <= code <= 0x30F3:  # Katakana range
                ch = chr(code - 0x60)
            result.append(ch)
        return "".join(result)

    tagger = Tagger()
    total = len(input)
    kept = OrderedDict()

    for i, word in enumerate(input, start=1):
        original_word = word
        word = word.strip()
        if not word or not input.get(original_word):
            continue

        parsed = list(tagger(word))
        readings = []
        for m in parsed:
            reading = getattr(m.feature, "reading", "") or getattr(m.feature, "kana", "") or m.surface
            readings.append(reading)
        kana = kata_to_hira("".join(readings))
        input[original_word].reading = kana  # use the original key, not stripped

        kept[original_word] = input[original_word]
        print(word)
        progress_handler(ProcessingStep.READINGS, i, total)#, f'{processed}/{total}')

    return kept
