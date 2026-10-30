import io
import unittest
from unittest.mock import patch

from src.Artifact import Artifact
from src.ConsoleProgress import ConsoleProgress
from src.Pipeline import Pipeline
from src.PipelineStep import PipelineStep


class GrammarStep(PipelineStep):
    rules = ()
    def process(self, artifact):
        self.progress(1, 2, "working")
        self.done("found grammar")
        return artifact


class GrammarPositionStep(PipelineStep):
    def process(self, artifact):
        print("Step output remains visible.")
        self.done("positions ready")
        return artifact


class SilentStep(PipelineStep):
    def process(self, artifact):
        return artifact


class ConsoleProgressTests(unittest.TestCase):
    def run_pipeline(self, verbose):
        output = io.StringIO()
        steps = [GrammarStep(), GrammarPositionStep(), SilentStep()]
        display = ConsoleProgress(output, len(steps), verbose)
        pipeline = Pipeline(steps, on_progress=display.progress, on_stage_record=display.stage)
        with display.capture_step_output():
            pipeline.run(Artifact([]))
        return output.getvalue()

    def test_default_replaces_progress_and_preserves_step_output(self):
        output = self.run_pipeline(False)
        self.assertIn('[00:00 1/3] Finding grammar occurrences... 50.0% working', output)
        self.assertIn('[00:00 2/3] Calculating grammar positions', output)
        self.assertIn('[00:00 3/3] Silent', output)
        self.assertIn('Step output remains visible.\n', output)
        self.assertNotIn('found grammar\n', output)
        self.assertTrue(output.endswith('\r'))

    def test_verbose_retains_stage_results_and_direct_output(self):
        output = self.run_pipeline(True)
        self.assertIn('found grammar\n', output)
        self.assertIn('positions ready\n', output)
        self.assertIn('Step output remains visible.\n', output)
        self.assertIn('[00:00 3/3] Silent... completed', output)

    def test_log_keeps_progress_and_step_output_in_default_mode(self):
        output, log = io.StringIO(), io.StringIO()
        steps = [GrammarStep(), GrammarPositionStep()]
        display = ConsoleProgress(output, len(steps), log=log)
        with display.capture_step_output():
            Pipeline(steps, on_progress=display.progress, on_stage_record=display.stage).run(Artifact([]))
        text = log.getvalue()
        self.assertIn('[00:00 1/2] Finding grammar occurrences...\n', text)
        self.assertNotIn('50.0%', text)
        self.assertIn('found grammar\n', text)
        self.assertIn('Step output remains visible.\n', text)
        self.assertIn('positions ready\n', text)
        self.assertNotIn('\r', text)

    def test_elapsed_time_updates_across_stages_and_in_log(self):
        output, log = io.StringIO(), io.StringIO()
        display = ConsoleProgress(output, 2, log=log)
        with patch('src.ConsoleProgress.time.monotonic', side_effect=[10, 10, 75, 75, 75, 75, 3671, 3671]):
            display.stage(1, SilentStep(), {'status': 'running'})
            display.progress(None, 1, 2)
            display.stage(1, SilentStep(), {'status': 'completed'})
            display.stage(2, SilentStep(), {'status': 'running'})
            display.progress(None, 1, 2)
        self.assertIn('[01:05 1/2]', output.getvalue())
        self.assertIn('[61:01 2/2]', output.getvalue())
        self.assertIn('[01:05 1/2]', log.getvalue())

    def test_long_japanese_progress_stays_on_one_line_and_is_erased(self):
        output = io.StringIO()
        display = ConsoleProgress(output, 1)
        with patch('src.ConsoleProgress.shutil.get_terminal_size', return_value=__import__('os').terminal_size((40, 24))):
            display._line('A' * 20 + '猫' * 30)
            display._line('short 50%')
        updates = output.getvalue().split('\r')
        self.assertEqual(updates[1], 'A' * 20 + '猫' * 9)
        self.assertEqual(updates[2], 'short 50%' + ' ' * 29)

    def test_failed_stage_is_visible_and_not_swallowed(self):
        class BrokenStep(PipelineStep):
            def process(self, artifact):
                raise ValueError('broken')
        output = io.StringIO()
        display = ConsoleProgress(output, 1)
        pipeline = Pipeline([BrokenStep()], on_progress=display.progress, on_stage_record=display.stage)
        with self.assertRaisesRegex(ValueError, 'broken'):
            with display.capture_step_output():
                pipeline.run(Artifact([]))
        self.assertIn('[00:00 1/1] Broken... failed', output.getvalue())


if __name__ == '__main__':
    unittest.main()
