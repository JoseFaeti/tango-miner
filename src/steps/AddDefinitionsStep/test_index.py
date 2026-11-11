import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.steps.AddDefinitionsStep.index import add_and_filter_for_definitions, AddDefinitionsStep
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.ProcessingStep.index import ProcessingStep
from src.steps.MergeTokensStep.index import MergeTokensStep

from tests.helpers import make_stats


class FakeJMDict:
    def __init__(self, path):
        self.path = path

    def get_most_common_definition(self, word, *, component_counts=None):
        return {"猫": "cat"}.get(word)


class AddDefinitionsStepTests(unittest.TestCase):
    def test_definition_step_uses_configured_dictionary(self):
        words = {"猫": make_stats()}
        with patch("src.steps.AddDefinitionsStep.index.get_jmdict", return_value=FakeJMDict("custom.xml")) as dictionary:
            Pipeline([AddDefinitionsStep(dictionary_path="custom.xml")]).run(Artifact(words))
        dictionary.assert_called_once_with("custom.xml")
        self.assertEqual(words["猫"].definition, "cat")

    def test_dictionary_definition_keeps_candidate_with_review_evidence(self):
        stats = make_stats()
        stats.coverage_review = True
        with patch("src.steps.AddDefinitionsStep.index.get_jmdict", return_value=FakeJMDict(None)):
            Pipeline([AddDefinitionsStep()]).run(Artifact({"猫": stats}))
        self.assertEqual(stats.definition, "cat")
        self.assertFalse(stats.needs_review)
        self.assertTrue(stats.coverage_review)
        self.assertTrue(stats.jmdict_definition)

    def test_ambiguous_dictionary_match_is_not_sent_to_review(self):
        from src.JMDict import JMDict
        from src.TokenProvenance import TokenComponent
        xml = """<JMdict>
        <entry><r_ele><reb>そば</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>nearby</gloss></sense></entry>
        <entry><r_ele><reb>そば</reb></r_ele><sense><pos>noun (common)</pos><gloss>noodles</gloss></sense></entry>
        </JMdict>"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'dictionary.xml'
            path.write_text(xml, encoding='utf-8')
            token = (TokenComponent('そば', 'そば', 'そば', 'そば', ('名詞',)),)
            stats = make_stats()
            stats.component_counts = {token: 2}
            with patch('src.steps.AddDefinitionsStep.index.get_jmdict', return_value=JMDict(path)):
                Pipeline([AddDefinitionsStep()]).run(Artifact({'そば': stats}))
            self.assertEqual(stats.definition, 'nearby')
            self.assertFalse(stats.needs_review)
            self.assertIsNone(stats.definition_components)

    def test_archived_expression_with_jmdict_definition_is_kept_despite_coverage(self):
        stats = make_stats(lemma='それもそうだ')
        stats.coverage_review = True
        stats.expression_review = True
        Pipeline([AddDefinitionsStep()]).run(Artifact({'それもそうだ': stats}))
        self.assertIn("that's true", stats.definition)
        self.assertFalse(stats.needs_review)
        self.assertTrue(stats.coverage_review)
        self.assertTrue(stats.expression_review)

    def test_merge_and_definitions_share_dictionary(self):
        words = {"猫": make_stats()}
        with patch("src.steps.MergeTokensStep.index._jmdict", None), \
             patch("src.steps.MergeTokensStep.index._jmdict_path", None), \
             patch("src.steps.MergeTokensStep.index.JMDict", wraps=FakeJMDict) as dictionary:
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

        with patch("src.steps.AddDefinitionsStep.index.get_jmdict", return_value=FakeJMDict(None)):
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
