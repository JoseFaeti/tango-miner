import csv

from fugashi import Tagger
from pathlib import Path

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep

class AddReadingsStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        output_path = Path("-3.readings.tmp")
        add_readings(artifact.data, output_path, self.progress)
        return Artifact(output_path, is_path=True)


def add_readings(input_file, output_file, progress_handler):
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

    total = get_total_lines(input_file)
    processed = 0

    with open(input_file, "r", encoding="utf-8") as fin, \
         open(output_file, "w", encoding="utf-8", newline="") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout)

        for row in reader:
            if not row:
                continue
            word = row[0].strip()
            parsed = list(tagger(word))
            readings = []
            for m in parsed:
                reading = getattr(m.feature, "reading", "") or getattr(m.feature, "kana", "") or m.surface
                readings.append(reading)
            kana = kata_to_hira("".join(readings))
            writer.writerow(row + [kana])

            processed += 1
            progress_handler(ProcessingStep.READINGS, processed, total)#, f'{processed}/{total}')


def get_total_lines(input_file):
    total = 0

    with open(input_file, "r", encoding="utf-8") as f:
        total = sum(1 for _ in f)

    return total