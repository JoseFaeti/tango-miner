import csv

from pathlib import Path

from .Artifact import Artifact
from .PipelineStep import PipelineStep


class WriteOutputStep(PipelineStep):
    def __init__(self, output_path: Path):
        self.output_path = output_path

    def process(self, artifact: Artifact) -> Artifact:
        write_final_file(artifact.data, self.output_path, self.progress)
        return artifact


def write_final_file(input, output_file, progress_handler=None):
    with open(output_file, "w", encoding="utf-8", newline="") as outfile:
        sorted_items = sorted(
            input.items(),
            key=lambda item: item[1].score,  # sort by score
            reverse=True                  # highest score first
        )

        sorted_words_by_score = dict(sorted_items)

        writer = csv.writer(outfile)

        for word in sorted_words_by_score:
            word_data = sorted_words_by_score[word]

            writer.writerow([
                word,
                word_data.reading,
                word_data.index,
                word_data.frequency,
                word_data.score,
                word_data.definition,
                " ".join(sorted(word_data.tags)),
                "<br><br>".join(
                    f"{s.text}<br><small>{s.tag}</small>"
                    for s in word_data.sentences
                ) if word_data.sentences else ""
            ])

    if progress_handler:
        progress_handler(None, 1, 1, f'Output written to {Path(output_file).resolve()}.')