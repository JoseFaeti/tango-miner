import unittest
from pathlib import Path
from unittest.mock import patch

from src.steps.AddDefinitionsStep import add_and_filter_for_definitions, AddDefinitionsStep
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.ProcessingStep import ProcessingStep

from tests.helpers import make_stats


class FakeJMDict:
    def __init__(self, path):
        self.path = path

    def get_most_common_definition(self, word):
        return {"猫": "cat"}.get(word)


class AddDefinitionsStepTests(unittest.TestCase):
    def test_definition_step_uses_configured_dictionary(self):
        words = {"猫": make_stats()}
        with patch("src.steps.AddDefinitionsStep.JMDict", wraps=FakeJMDict) as dictionary:
            Pipeline([AddDefinitionsStep(dictionary_path="custom.xml")]).run(Artifact(words))
        dictionary.assert_called_once_with(Path("custom.xml"))
        self.assertEqual(words["猫"].definition, "cat")

    def test_adds_definitions_and_marks_missing_entries_invalid(self):
        words = {
            "猫": make_stats(definition="", invalid=False),
            "謎語": make_stats(definition="", invalid=False),
        }
        events = []
        processed = []

        with patch("src.steps.AddDefinitionsStep.JMDict", FakeJMDict):
            result, valid_count, invalid_count = add_and_filter_for_definitions(
                words,
                progress_handler=lambda *args: events.append(args),
                on_definition_processed=lambda word, result: processed.append((word, result)),
            )

        self.assertIs(result["猫"], words["猫"])
        self.assertEqual((valid_count, invalid_count), (1, 1))
        self.assertEqual(result["猫"].definition, "cat")
        self.assertFalse(result["猫"].invalid)
        self.assertIsNone(result["謎語"].definition)
        self.assertTrue(result["謎語"].invalid)
        self.assertEqual(processed, [("猫", "cat"), ("謎語", None)])
        self.assertEqual(events[0], (0, 2, "Initializing dictionary..."))
        self.assertEqual(events[-1], (1, 2, "1/2 1 definitions found"))


if __name__ == "__main__":
    unittest.main()
