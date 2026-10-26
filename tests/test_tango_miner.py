import unittest
import tempfile
from pathlib import Path

from tango_miner import MIN_FREQUENCY_DEFAULT, resolve_directory_output_path


class TangoMinerTests(unittest.TestCase):
    def test_default_minimum_frequency_is_positive(self):
        self.assertGreater(MIN_FREQUENCY_DEFAULT, 0)

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
