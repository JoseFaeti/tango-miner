import unittest
import json
from unittest.mock import Mock
from unittest.mock import patch

from src.steps.AddWordsToAnkiStep import (
    AddWordsToAnkiStep,
    anki_invoke,
    anki_fields_differ_from_stats,
    export_words_to_anki,
    word_to_anki_fields,
)
from src.RunConfig import AnkiConfig
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.WordStats import Sentence

from tests.helpers import make_stats


def note(note_id, expression, tags=None, fields=None):
    values = {
        "Expression": expression,
        "Reading": "よみ",
        "Index": "1",
        "Frequency": "2",
        "Score": "123.45",
        "Meaning": "definition",
        "Sentence": "",
    }
    values.update(fields or {})
    return {
        "noteId": note_id,
        "tags": tags or [],
        "fields": {name: {"value": value} for name, value in values.items()},
    }


class AddWordsToAnkiStepTests(unittest.TestCase):
    def test_step_passes_configured_destination_and_batches(self):
        config = AnkiConfig(enabled=True, endpoint="http://localhost:9999", deck="Deck", model="Custom", timeout_seconds=2, batch_size=3, note_fetch_batch_size=7)
        with patch("src.steps.AddWordsToAnkiStep.export_words_to_anki", return_value=(0, 0, 0)) as export:
            Pipeline([AddWordsToAnkiStep(config)]).run(Artifact({}))
        args = export.call_args.kwargs
        self.assertEqual((args["deck_name"], args["model_name"]), ("Deck", "Custom"))
        self.assertEqual((args["batch_size"], args["note_fetching_batch_size"]), (3, 7))
        self.assertEqual(args["invoke"].keywords, {"endpoint": config.endpoint, "timeout": 2})

    def test_request_uses_configured_endpoint_and_timeout(self):
        response = Mock()
        response.read.return_value = json.dumps({"error": None, "result": 42}).encode()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch("src.steps.AddWordsToAnkiStep.urllib.request.urlopen", return_value=response) as open_url:
            self.assertEqual(anki_invoke("version", endpoint="http://localhost:9999", timeout=3), 42)
        self.assertEqual(open_url.call_args.args[0].full_url, "http://localhost:9999")
        self.assertEqual(open_url.call_args.kwargs["timeout"], 3)

    def test_export_uses_requested_model_and_fetch_batch_size(self):
        calls = []
        def fake(action, params=None):
            calls.append((action, params or {}))
            if action == "modelNames": return ["Custom"]
            if action == "findNotes": return [1, 2, 3]
            if action == "notesInfo": return [note(i, str(i)) for i in params["notes"]]
            if action == "deleteNotes": return None
            raise AssertionError(action)
        result = export_words_to_anki("Deck", {}, "Custom", invoke=fake, batch_size=2, note_fetching_batch_size=2)
        self.assertEqual(result, (0, 0, 3))
        self.assertEqual([p["notes"] for action, p in calls if action == "notesInfo"], [[1, 2], [3]])
        self.assertEqual([len(p["notes"]) for action, p in calls if action == "deleteNotes"], [2, 1])
        self.assertIn('note:"Custom"', next(p["query"] for action, p in calls if action == "findNotes"))
        self.assertFalse(any(action == "createModel" for action, _ in calls))

    def test_word_to_anki_fields_serializes_stats_and_sentences(self):
        sentence = Sentence("猫がいる", "tag", "origin.txt", "猫", 100)
        stats = make_stats(
            index=1,
            frequency=2,
            score=123.45,
            reading="ねこ",
            definition="cat",
            sentences=[sentence],
        )

        fields = word_to_anki_fields("猫", stats)

        self.assertEqual(fields["Expression"], "猫")
        self.assertEqual(fields["Reading"], "ねこ")
        self.assertEqual(fields["Frequency"], "2")
        self.assertIn("<span class='highlight'>猫</span>", fields["Sentence"])

    def test_anki_fields_differ_from_stats_detects_changed_values(self):
        stats = make_stats(index=1, frequency=2, score=123.45)

        self.assertFalse(anki_fields_differ_from_stats(note(1, "猫"), stats))
        self.assertTrue(
            anki_fields_differ_from_stats(
                note(1, "猫", fields={"Frequency": "3"}),
                stats,
            )
        )

    def test_export_adds_updates_tags_and_deletes_obsolete_notes(self):
        calls = []
        existing = note(1, "猫", tags=["old"], fields={"Meaning": "old definition"})
        obsolete = note(2, "古い", tags=["old"])
        words = {
            "猫": make_stats(index=1, frequency=2, score=123.45, tags={"new"}),
            "犬": make_stats(index=3, frequency=4, score=200, tags={"new"}),
        }

        def fake_invoke(action, params=None):
            calls.append((action, params or {}))
            if action == "modelNames":
                return ["TangoMiner:Japanese"]
            if action == "findNotes" and "Expression:" not in params["query"]:
                return [1, 2]
            if action == "notesInfo":
                return [existing, obsolete]
            if action == "findNotes":
                return []
            if action in {"deleteNotes", "multi", "addNotes"}:
                return None
            raise AssertionError(f"unexpected action {action}")

        with patch("src.steps.AddWordsToAnkiStep.anki_invoke", side_effect=fake_invoke):
            export_words_to_anki("Deck", words, "TangoMiner:Japanese")

        delete_calls = [params for action, params in calls if action == "deleteNotes"]
        add_calls = [params for action, params in calls if action == "addNotes"]
        multi_calls = [params for action, params in calls if action == "multi"]

        self.assertEqual(delete_calls, [{"notes": [2]}])
        self.assertEqual(add_calls[0]["notes"][0]["fields"]["Expression"], "犬")
        actions = [action["action"] for action in multi_calls[0]["actions"]]
        self.assertIn("updateNoteFields", actions)
        self.assertIn("addTags", actions)
        self.assertIn("removeTags", actions)

        duplicate_queries = [
            params["query"]
            for action, params in calls
            if action == "findNotes" and "Expression:" in params["query"]
        ]
        self.assertEqual(
            duplicate_queries,
            ['deck:"Deck" note:"TangoMiner:Japanese" Expression:"犬"'],
        )


if __name__ == "__main__":
    unittest.main()
