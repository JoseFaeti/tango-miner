import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.ProcessingStep import ProcessingStep
from src.TokenizedSentence import TokenizedSentence
from src.steps.MergeTokensStep import combine_tokens_jmdict
from src.steps.OccurrenceDiagnosticsStep import OccurrenceDiagnosticsStep
from src.steps.WriteOutputStep import write_candidate_diagnostics
from tests.helpers import make_stats
from tests.test_token_provenance import Dictionary, token


class OccurrenceDiagnosticsTests(unittest.TestCase):
    def test_progress_reports_sentences_then_completes_after_file_is_written(self):
        sentence = TokenizedSentence('猫', 'tag', Path('source'), [token('猫', '猫', ['名詞', '*'])])
        events = []
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'occurrences.jsonl'
            with patch('src.steps.OccurrenceDiagnosticsStep.PROGRESS_INTERVAL', 1):
                pipeline = Pipeline([OccurrenceDiagnosticsStep(path)], on_progress=lambda *a, **kw: events.append((a, kw)))
                pipeline.run(Artifact([sentence, sentence, sentence]))
            self.assertTrue(path.exists())
            self.assertEqual(len(path.read_text(encoding='utf-8').splitlines()), 3)
        self.assertEqual([event[0][1:3] for event in events], [(0, 3), (1, 3), (2, 3), (1, 1)])
        self.assertTrue(all(event[0][0] == ProcessingStep.OCCURRENCE_DIAGNOSTICS for event in events))
        self.assertIn('Writing occurrences.jsonl', events[0][1]['additional_text'])
        self.assertIn('written', events[-1][1]['additional_text'])

    def test_every_occurrence_and_excluded_proper_noun_have_evidence(self):
        raw = [token('ない', 'ない', ['助動詞', '*']), token('で', 'で', ['助詞', '*'])]
        merged = combine_tokens_jmdict(raw, Dictionary())[0]
        name = token('風花', '風花', ['名詞', '固有名詞'])
        sentence = TokenizedSentence('風花ないでないで', 'tag', Path('source.txt'), [name, merged, merged])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'occurrences.jsonl'
            artifact = Artifact([sentence])
            self.assertIs(OccurrenceDiagnosticsStep(path).process(artifact), artifact)
            rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]
        self.assertEqual(len(rows), 3)
        self.assertFalse(rows[0]['eligible_for_collection'])
        self.assertEqual(rows[0]['source'], 'source.txt')
        self.assertEqual(rows[0]['span'], [0, 2])
        self.assertEqual([row['span'] for row in rows[1:]], [[2, 5], [5, 8]])
        self.assertEqual(rows[1]['analysis_rule'], 'jmdict_surface_merge')
        self.assertEqual([c['surface'] for c in rows[1]['components']], ['ない', 'で'])
        self.assertTrue(rows[1]['eligible_for_collection'])

    def test_unaligned_token_has_no_invented_span(self):
        sentence = TokenizedSentence('猫と犬', None, Path('a'), [token('狐', '狐', ['名詞', '*']), token('犬', '犬', ['名詞', '*'])])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'occurrences.jsonl'
            OccurrenceDiagnosticsStep(path).process(Artifact([sentence]))
            rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]
        self.assertEqual([row['span'] for row in rows], [None, None])

    def test_candidate_summary_distinguishes_review_invalid_and_valid(self):
        words = {word: make_stats(lemma=word) for word in ['valid', 'review', 'invalid']}
        words['review'].needs_review = True
        words['invalid'].invalid = True
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'candidate-diagnostics.jsonl'
            write_candidate_diagnostics(words, path)
            rows = {r['candidate']: r for r in map(json.loads, path.read_text(encoding='utf-8').splitlines())}
        self.assertEqual({word: row['status'] for word, row in rows.items()},
                         {'valid': 'valid', 'review': 'review', 'invalid': 'invalid'})


if __name__ == '__main__':
    unittest.main()
