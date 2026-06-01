import unittest
from unittest.mock import patch

from src.AddDefinitionsStep import add_and_filter_for_definitions
from src.ProcessingStep import ProcessingStep

from tests.helpers import make_stats


class FakeJMDict:
    def __init__(self, path):
        self.path = path

    def get_most_common_definition(self, word):
        return {"猫": "cat"}.get(word)


class AddDefinitionsStepTests(unittest.TestCase):
    def test_adds_definitions_and_marks_missing_entries_invalid(self):
        words = {
            "猫": make_stats(definition="", invalid=False),
            "謎語": make_stats(definition="", invalid=False),
        }
        events = []
        processed = []

        with patch("src.AddDefinitionsStep.JMDict", FakeJMDict):
            result = add_and_filter_for_definitions(
                words,
                progress_handler=lambda *args: events.append(args),
                on_definition_processed=lambda word, result: processed.append((word, result)),
            )

        self.assertIs(result["猫"], words["猫"])
        self.assertEqual(result["猫"].definition, "cat")
        self.assertFalse(result["猫"].invalid)
        self.assertIsNone(result["謎語"].definition)
        self.assertTrue(result["謎語"].invalid)
        self.assertEqual(processed, [("猫", "cat"), ("謎語", None)])
        self.assertEqual(events[0][0], ProcessingStep.DEFINITIONS)
        self.assertEqual(events[-1], (ProcessingStep.DEFINITIONS, 1, 1, "1 definitions found"))


if __name__ == "__main__":
    unittest.main()
