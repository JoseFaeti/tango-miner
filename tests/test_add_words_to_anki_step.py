import unittest
import json
import tempfile
from pathlib import Path
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

    def test_output_only_reports_plan_after_progress_without_mutations(self):
        from io import StringIO
        from tango_miner import print_step_progress

        calls = []
        def fake(action, params=None, **kwargs):
            calls.append(action)
            if action == "modelNames": return ["Custom"]
            if action == "findNotes": return [] if "Expression:" in params["query"] else [1, 2]
            if action == "notesInfo": return [note(1, "猫", fields={"Meaning": "old"}), note(2, "古い")]
            raise AssertionError(f"unexpected mutation: {action}")

        words = {"猫": make_stats(index=1, frequency=2, score=123.45), "犬": make_stats()}
        config = AnkiConfig(enabled=True, model="Custom", output_only=True, prune=True)
        output = StringIO()
        with patch("src.steps.AddWordsToAnkiStep.anki_invoke", side_effect=fake), \
             patch("sys.stdout", output), patch("builtins.input") as prompt, \
             patch("src.steps.AddWordsToAnkiStep.build_mutation_actions") as build_actions:
            Pipeline([AddWordsToAnkiStep(config)], on_progress=print_step_progress).run(Artifact(words))
        prompt.assert_not_called()
        build_actions.assert_not_called()
        rendered = output.getvalue()
        self.assertIn("Changes: 1 to add, 1 to modify, 1 to delete.", rendered)
        self.assertIn("Anki output-only mode: no changes applied.", rendered)
        self.assertLess(rendered.index("Changes:"), rendered.index("Anki output-only mode:"))
        self.assertNotIn("Anki plan:", rendered)
        self.assertNotIn("0 added, 0 updated, 0 deleted", rendered)
        self.assertFalse(set(calls) & {"deleteNotes", "multi", "addNotes", "createModel"})

    def test_detailed_plan_saved_before_confirmation_and_without_mutation(self):
        calls = []
        def fake(action, params=None, **kwargs):
            calls.append(action)
            if action == "modelNames": return ["Custom"]
            if action == "findNotes": return [] if "Expression:" in params["query"] else [1, 2]
            if action == "notesInfo": return [note(1, "猫", tags=["old"], fields={"Meaning": "old"}), note(2, "古い")]
            raise AssertionError(f"unexpected mutation: {action}")

        words = {"猫": make_stats(index=1, frequency=2, score=123.45), "犬": make_stats()}
        for output_only in (True, False):
            with self.subTest(output_only=output_only), tempfile.TemporaryDirectory() as directory:
                plan_path = Path(directory) / "anki-plan.json"
                def decline(_prompt):
                    self.assertTrue(plan_path.exists())
                    return "n"
                step = AddWordsToAnkiStep(AnkiConfig(model="Custom", output_only=output_only, prune=True), plan_path)
                with patch("src.steps.AddWordsToAnkiStep.anki_invoke", side_effect=fake), \
                     patch("builtins.input", side_effect=decline), patch("builtins.print"):
                    Pipeline([step]).run(Artifact(words))
                plan = json.loads(plan_path.read_text(encoding="utf-8"))
                self.assertEqual([item["expression"] for item in plan["add"]], ["犬"])
                self.assertEqual(plan["modify"][0]["fields"]["Meaning"], {"before": "old", "after": "definition"})
                self.assertEqual(plan["modify"][0]["tags_removed"], ["old"])
                self.assertEqual(plan["delete"], [{"expression": "古い", "note_id": 2}])
                self.assertEqual(step.outcome["status"], "preview_only" if output_only else "cancelled")
        self.assertFalse(set(calls) & {"deleteNotes", "multi", "addNotes", "createModel"})

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
        with patch("builtins.input", return_value="y"):
            result = export_words_to_anki("Deck", {}, "Custom", invoke=fake, batch_size=2, note_fetching_batch_size=2, prune=True)
        self.assertEqual(result, (0, 0, 3))
        self.assertEqual([p["notes"] for action, p in calls if action == "notesInfo"], [[1, 2], [3]])
        self.assertEqual([len(p["notes"]) for action, p in calls if action == "deleteNotes"], [2, 1])
        self.assertIn('note:"Custom"', next(p["query"] for action, p in calls if action == "findNotes"))
        self.assertFalse(any(action == "createModel" for action, _ in calls))

    def test_pruning_disabled_retains_absent_notes_even_when_changes_are_approved(self):
        calls, details = [], []
        def fake(action, params=None):
            calls.append(action)
            if action == "modelNames": return ["Custom"]
            if action == "findNotes": return [] if "Expression:" in params["query"] else [1]
            if action == "notesInfo": return [note(1, "古い")]
            if action == "addNotes": return None
            raise AssertionError(action)
        with patch("builtins.input", return_value="y"), patch("builtins.print"):
            result = export_words_to_anki("Deck", {"犬": make_stats()}, "Custom", invoke=fake,
                                          details_handler=details.append)
        self.assertEqual(result, (1, 0, 0))
        self.assertNotIn("deleteNotes", calls)
        self.assertEqual(details[0]["delete"], [])
        self.assertEqual(details[0]["retained_absent"], [{"expression": "古い", "note_id": 1}])
        self.assertFalse(details[0]["prune"])

    def test_preview_and_confirmation_gate_all_mutations(self):
        words = {"犬": make_stats(index=1, frequency=2, score=123.45)}
        for output_only, answer in [(True, "y"), (False, "n"), (False, None), (False, "y")]:
            with self.subTest(output_only=output_only, answer=answer):
                calls = []
                def fake(action, params=None):
                    calls.append(action)
                    if action == "modelNames": return []
                    if action == "findNotes": return []
                    if action in {"createModel", "addNotes"}: return None
                    raise AssertionError(action)
                with patch("builtins.input", side_effect=EOFError if answer is None else None, return_value=answer) as prompt, patch("builtins.print") as output:
                    result = export_words_to_anki("Deck", words, "Missing", invoke=fake, output_only=output_only)
                self.assertIn("Anki plan: 1 to add, 0 to modify, 0 to delete.", [c.args[0] for c in output.call_args_list])
                if output_only:
                    prompt.assert_not_called()
                if not output_only and answer == "y":
                    self.assertEqual(result, (1, 0, 0))
                    self.assertIn("createModel", calls)
                    self.assertIn("addNotes", calls)
                else:
                    self.assertEqual(result, (0, 0, 0))
                    self.assertNotIn("createModel", calls)
                    self.assertNotIn("addNotes", calls)

    def test_anki_plan_status_records_preview_cancel_and_failure(self):
        words = {"犬": make_stats()}
        for mode, answer, fail, expected in [(True, None, False, "preview_only"),
                                             (False, "n", False, "cancelled"),
                                             (False, "y", True, "applying")]:
            with self.subTest(mode=mode, answer=answer):
                def fake(action, params=None):
                    if action == "modelNames": return ["Custom"]
                    if action == "findNotes": return []
                    if action == "addNotes" and fail: raise RuntimeError("network failure")
                    raise AssertionError(action)
                plan, statuses = [], []
                with patch("builtins.input", return_value=answer), patch("builtins.print"):
                    if fail:
                        with self.assertRaises(RuntimeError):
                            export_words_to_anki("Deck", words, "Custom", invoke=fake, output_only=mode,
                                                 preview_handler=plan.append, status_handler=statuses.append)
                    else:
                        export_words_to_anki("Deck", words, "Custom", invoke=fake, output_only=mode,
                                             preview_handler=plan.append, status_handler=statuses.append)
                self.assertEqual(plan, [(1, 0, 0, False)])
                self.assertEqual(statuses[-1], expected)

    def test_unresolved_words_preserve_existing_notes_without_updates_or_new_notes(self):
        reviewed = make_stats()
        reviewed.needs_review = True
        new_reviewed = make_stats()
        new_reviewed.needs_review = True
        words = {"未解決": reviewed, "新語": new_reviewed}
        calls = []
        def fake(action, params=None):
            calls.append((action, params))
            if action == "modelNames": return ["Custom"]
            if action == "findNotes": return [1, 2]
            if action == "notesInfo": return [note(1, "未解決"), note(2, "本当に不要")]
            raise AssertionError(action)
        plans = []
        with patch("builtins.print"):
            result = export_words_to_anki("Deck", words, "Custom", invoke=fake, output_only=True, preview_handler=plans.append, prune=True)
        self.assertEqual(result, (0, 0, 0))
        self.assertEqual(plans, [(0, 0, 1, False)])
        self.assertFalse(any(action in {"deleteNotes", "multi", "addNotes"} for action, _ in calls))
        self.assertEqual(sum(action == "findNotes" for action, _ in calls), 1)

    def test_declining_mixed_plan_never_writes(self):
        calls = []
        def fake(action, params=None):
            calls.append(action)
            if action == "modelNames": return ["Custom"]
            if action == "findNotes": return [] if "Expression:" in params["query"] else [1, 2]
            if action == "notesInfo": return [note(1, "猫", fields={"Meaning": "old"}), note(2, "古い")]
            raise AssertionError(f"unexpected mutation: {action}")
        words = {"猫": make_stats(index=1, frequency=2, score=123.45), "犬": make_stats()}
        with patch("builtins.input", return_value="n"), patch("builtins.print") as output:
            self.assertEqual(export_words_to_anki("Deck", words, "Custom", invoke=fake, prune=True), (0, 0, 0))
        output.assert_any_call("Anki plan: 1 to add, 1 to modify, 1 to delete.")
        self.assertFalse(set(calls) & {"deleteNotes", "multi", "addNotes", "createModel"})

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

        with patch("src.steps.AddWordsToAnkiStep.anki_invoke", side_effect=fake_invoke), patch("builtins.input", return_value="y"):
            export_words_to_anki("Deck", words, "TangoMiner:Japanese", prune=True)

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
