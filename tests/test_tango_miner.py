import unittest
import tempfile
from pathlib import Path

from src.AddDefinitionsStep import AddDefinitionsStep
from src.AttachSentencesStep import AttachSentencesStep
from src.FilterFrequencyStep import FilterFrequencyStep
from src.ScoreWordStep import ScoreWordStep
from src.TokenizeStep import TokenizeStep
from src.WriteOutputStep import WriteOutputStep
from tango_miner import build_mining_pipeline, resolve_directory_output_path


class TangoMinerTests(unittest.TestCase):
    def test_single_file_pipeline_uses_defined_steps_and_path_output(self):
        pipeline = build_mining_pipeline("output.csv", min_frequency=4)

        self.assertEqual(
            [type(step) for step in pipeline.steps],
            [
                TokenizeStep,
                FilterFrequencyStep,
                AddDefinitionsStep,
                ScoreWordStep,
                AttachSentencesStep,
                WriteOutputStep,
            ],
        )
        self.assertEqual(pipeline.steps[-1].output_path, Path("output.csv"))

    def test_directory_output_defaults_to_csv_inside_input_directory(self):
        input_path = Path("corpus")

        self.assertEqual(
            resolve_directory_output_path(input_path, None),
            Path("corpus") / "corpus.csv",
        )

    def test_directory_output_existing_directory_gets_default_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "out"
            output_dir.mkdir()

            self.assertEqual(
                resolve_directory_output_path(Path("corpus"), str(output_dir)),
                output_dir / "corpus.csv",
            )

    def test_directory_output_file_path_creates_parent_and_returns_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_file = Path(tmp) / "missing" / "out.csv"

            resolved = resolve_directory_output_path(Path("corpus"), str(output_file))

            self.assertEqual(resolved, output_file)
            self.assertTrue(output_file.parent.exists())

    def test_directory_output_new_directory_gets_default_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "new-output"

            resolved = resolve_directory_output_path(Path("corpus"), str(output_dir))

            self.assertEqual(resolved, output_dir / "corpus.csv")
            self.assertTrue(output_dir.exists())


if __name__ == "__main__":
    unittest.main()
