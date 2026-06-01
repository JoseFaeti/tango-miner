import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.PipelineStep import PipelineStep
from src.ProcessingStep import ProcessingStep


class AppendStep(PipelineStep):
    def __init__(self, suffix):
        super().__init__()
        self.suffix = suffix

    def process(self, artifact):
        self.progress(ProcessingStep.FILTERING, 1, 1, self.suffix)
        return Artifact(artifact.data + self.suffix)


class PipelineTests(unittest.TestCase):
    def test_run_passes_data_through_steps_and_preserves_tmpdir(self):
        tmpdir = Path("tmp")
        events = []
        pipeline = Pipeline(
            [AppendStep("b"), AppendStep("c")],
            on_progress=lambda *args: events.append(args),
        )

        result = pipeline.run(Artifact("a", tmpdir=tmpdir))

        self.assertEqual(result.data, "abc")
        self.assertEqual(result.tmpdir, tmpdir)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0], (ProcessingStep.FILTERING, 1, 1, "b"))


if __name__ == "__main__":
    unittest.main()
