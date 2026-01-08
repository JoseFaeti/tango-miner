import csv

from pathlib import Path

from .Artifact import Artifact
from .PipelineStep import PipelineStep


# Order in which the word data is written to the final CSV file
CSV_FIELD_ORDER = [
    "word",
    "index",
    "frequency",
    "frequency_normalized",
    "reading",
    "definition",
    "tags",
]


class WriteOutputStep(PipelineStep):
    def __init__(self, output_path: Path):
        self.output_path = output_path

    def process(self, artifact: Artifact) -> Artifact:
        write_final_file(artifact.data, self.output_path)
        return artifact


def write_final_file(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as infile, \
         open(output_file, "w", encoding="utf-8", newline="") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            if not row or not row[0].strip():
                continue

            record = row_to_record(row)
            ordered_row = record_to_row(record, CSV_FIELD_ORDER)

            writer.writerow(ordered_row)


def row_to_record(row):
    return {
        "word": row[0] if len(row) > 0 else "",
        "index": row[1] if len(row) > 1 else "",
        "frequency": row[2] if len(row) > 2 else "",
        "frequency_normalized": row[3] if len(row) > 3 else "",
        "tags": row[4] if len(row) > 4 else "",
        "reading": row[5] if len(row) > 5 else "",
        "definition": row[6] if len(row) > 6 else "",
    }


def record_to_row(record, field_order):
    return [record.get(field, "") for field in field_order]