import unittest
from pathlib import Path
from unittest.mock import patch

from src.steps.AddDefinitionsStep import add_and_filter_for_definitions, AddDefinitionsStep
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.ProcessingStep import ProcessingStep
from src.steps.MergeTokensStep import MergeTokensStep

from tests.helpers import make_stats


class FakeJMDict:
    def __init__(self, path):
        self.path = path

    def get_most_common_definition(self, word, *, component_counts=None):
        return {"猫": "cat"}.get(word)


class AddDefinitionsStepTests(unittest.TestCase):
    def test_definition_step_uses_configured_dictionary(self):
        words = {"猫": make_stats()}
        with patch("src.steps.AddDefinitionsStep.get_jmdict", return_value=FakeJMDict("custom.xml")) as dictionary:
            Pipeline([AddDefinitionsStep(dictionary_path="custom.xml")]).run(Artifact(words))
        dictionary.assert_called_once_with("custom.xml")
        self.assertEqual(words["猫"].definition, "cat")

    def test_coverage_review_survives_dictionary_definition(self):
        stats = make_stats()
        stats.coverage_review = True
        with patch("src.steps.AddDefinitionsStep.get_jmdict", return_value=FakeJMDict(None)):
            Pipeline([AddDefinitionsStep()]).run(Artifact({"猫": stats}))
        self.assertEqual(stats.definition, "cat")
        self.assertTrue(stats.needs_review)

    def test_merge_and_definitions_share_dictionary(self):
        words = {"猫": make_stats()}
        with patch("src.steps.MergeTokensStep._jmdict", None), \
             patch("src.steps.MergeTokensStep._jmdict_path", None), \
             patch("src.steps.MergeTokensStep.JMDict", wraps=FakeJMDict) as dictionary:
            Pipeline([MergeTokensStep(dictionary_path="custom.xml")]).run(Artifact([]))
            Pipeline([AddDefinitionsStep(dictionary_path="custom.xml")]).run(Artifact(words))
        dictionary.assert_called_once_with(Path("custom.xml").resolve())
        self.assertEqual(words["猫"].definition, "cat")

    def test_adds_definitions_and_marks_missing_entries_for_review(self):
        words = {
            "猫": make_stats(definition="", invalid=False),
            "謎語": make_stats(definition="", invalid=False),
        }
        events = []
        processed = []

        with patch("src.steps.AddDefinitionsStep.get_jmdict", return_value=FakeJMDict(None)):
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
        self.assertFalse(result["謎語"].invalid)
        self.assertTrue(result["謎語"].needs_review)
        self.assertEqual(processed, [("猫", "cat"), ("謎語", None)])
        self.assertEqual(events[0], (0, 2, "Loading dictionary..."))
        self.assertEqual(events[-1], (1, 2, "1/2 1 definitions found"))


if __name__ == "__main__":
    unittest.main()
