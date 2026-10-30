import unittest
import json
import tempfile
from pathlib import Path

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.PipelineStep import PipelineStep
from src.RunArchive import write_stage_record
from src.steps.ProcessingStep import ProcessingStep


class AppendStep(PipelineStep):
    def __init__(self, suffix):
        super().__init__()
        self.suffix = suffix

    def process(self, artifact):
        self._processing_step = ProcessingStep.FILTERING
        self.progress(1, 1, self.suffix)
        return Artifact(artifact.data + self.suffix)


class PipelineTests(unittest.TestCase):
    def test_ordered_reports_include_completed_and_failed_stages(self):
        class FailingStep(PipelineStep):
            def process(self, artifact):
                raise ValueError("broken input")
        with tempfile.TemporaryDirectory() as temp:
            pipeline = Pipeline([AppendStep("b"), AppendStep("c"), FailingStep()],
                on_stage_record=lambda i, step, record: write_stage_record(temp, i, step, record))
            with self.assertRaisesRegex(ValueError, "broken input"):
                pipeline.run(Artifact("a"))
            paths = sorted((Path(temp) / "steps").glob("*.json"))
            self.assertEqual([p.name for p in paths],
                             ["01-AppendStep.json", "02-AppendStep.json", "03-FailingStep.json"])
            first, second, failed = [json.loads(p.read_text(encoding="utf-8")) for p in paths]
            self.assertEqual(first["message"], "b")
            self.assertEqual(second["status"], "completed")
            self.assertEqual(failed["status"], "failed")
            self.assertIn("broken input", failed["error"])
            self.assertGreaterEqual(failed["duration_seconds"], 0)

    def test_grammar_report_records_each_rule(self):
        from src.Grammar import GrammarStep
        from tests.test_grammar_te_iru import TeIruCardTests
        source = TeIruCardTests().valid()
        with tempfile.TemporaryDirectory() as temp:
            pipeline = Pipeline([GrammarStep()],
                on_stage_record=lambda i, step, record: write_stage_record(temp, i, step, record))
            pipeline.run(Artifact([source]))
            report = json.loads((Path(temp) / "steps" / "01-GrammarStep.json").read_text(encoding="utf-8"))
            self.assertEqual(report["input_count"], 1)
            self.assertEqual(report["grammar_rules"]["te_iru"]["occurrences"], 1)
            self.assertEqual(report["grammar_rules"]["te_iru"]["examples"], 1)
            self.assertIn("tai", report["grammar_rules"])

    def test_failed_stage_retains_error_and_elapsed_time(self):
        class FailingStep(PipelineStep):
            def process(self, artifact):
                raise ValueError("broken input")
        pipeline = Pipeline([AppendStep("b"), FailingStep()])
        with self.assertRaisesRegex(ValueError, "broken input"):
            pipeline.run(Artifact("a"))
        self.assertEqual([record["status"] for record in pipeline.stage_records], ["completed", "failed"])
        self.assertIn("broken input", pipeline.stage_records[-1]["error"])
        self.assertGreaterEqual(pipeline.duration, 0)

    def test_run_passes_data_through_steps_and_preserves_tmpdir(self):
        tmpdir = Path("tmp")
        events = []
        pipeline = Pipeline(
            [AppendStep("b"), AppendStep("c")],
            on_progress=lambda *args, **kwargs: events.append((args, kwargs)),
        )

        result = pipeline.run(Artifact("a", tmpdir=tmpdir))

        self.assertEqual(result.data, "abc")
        self.assertEqual(result.tmpdir, tmpdir)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0][0], (ProcessingStep.FILTERING, 1, 1))
        self.assertEqual(events[0][1]["additional_text"], "b")
        self.assertIsInstance(events[0][1]["duration"], float)


if __name__ == "__main__":
    unittest.main()
