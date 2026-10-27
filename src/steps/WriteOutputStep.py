import csv
import json

from pathlib import Path

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep


class WriteOutputStep(PipelineStep):
    def __init__(self, output_path: Path, diagnostics_path: Path | None = None):
        self._processing_step = None
        self.output_path = output_path
        self.diagnostics_path = diagnostics_path

    def process(self, artifact: Artifact) -> Artifact:
        write_final_file(artifact.data, self.output_path, self.progress)
        if self.diagnostics_path is not None:
            write_candidate_diagnostics(artifact.data, self.diagnostics_path)
        return artifact


def write_candidate_diagnostics(words, path):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w', encoding='utf-8', newline='\n') as stream:
        for word, stats in words.items():
            status = 'invalid' if stats.invalid else 'review' if stats.needs_review else 'valid'
            row = {'candidate': word, 'status': status, 'frequency': stats.frequency,
                   'reading': stats.reading, 'definition': stats.definition}
            stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
    temporary.replace(path)


def write_final_file(input, output_file, progress_handler=None):
    p = output_file

    sorted_items = sorted(
        input.items(),
        key=lambda item: item[1].score,  # sort by score
        reverse=True                  # highest score first
    )

    sorted_words_by_score = dict(sorted_items)

    with (
        open(p, "w", encoding="utf-8", newline="") as outfile,
        open(p.with_name(p.stem + ".dropped" + p.suffix), "w", encoding="utf-8", newline="") as dropfile
    ):
        normal_writer = csv.writer(outfile)
        drop_writer = csv.writer(dropfile)

        for word in sorted_words_by_score:
            word_data = sorted_words_by_score[word]

            writer = drop_writer if word_data.invalid or word_data.needs_review else normal_writer

            writer.writerow([
                word_data.score,
                word,
                word_data.reading,
                f"{word_data.index:.2f}",
                word_data.frequency,
                word_data.definition,
                " ".join(sorted(word_data.tags)),
                f"sentences: {len(word_data.sentences)}",
                "<br><br>".join(
                    s.to_html() for s in word_data.sentences
                ) if word_data.sentences else "no sentences :("
            ])

    p = output_file
    with open(p.with_name(p.stem + ".sentence" + p.suffix), 'w', encoding='utf-8', newline="") as sentence_file:
        sentence_writer = csv.writer(sentence_file)
        sentence_writer.writerow(["status", "word", "score", "sentence", "tag", "origin", "surface_form"])

        for word, word_data in sorted_words_by_score.items():
            if not word_data.sentences:
                continue

            status = "review" if word_data.needs_review else "dropped" if word_data.invalid else "kept"

            for sentence in word_data.sentences:
                sentence_writer.writerow([
                    status,
                    word,
                    word_data.score,
                    sentence.text,
                    sentence.tag,
                    sentence.origin,
                    sentence.surface_form,
                ])

    if progress_handler:
        progress_handler(1, 1, f'Output written to {Path(output_file).resolve()}.')
