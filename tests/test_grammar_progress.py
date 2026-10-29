import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.Pipeline import Pipeline
from src.TokenizedSentence import TokenizedSentence
from src.steps.ProcessingStep import ProcessingStep


class GrammarProgressTests(unittest.TestCase):
    def test_reports_progress_even_when_sentence_alignment_fails(self):
        sentence = TokenizedSentence('猫', None, Path('source'),
                                     [{'surface': '犬', 'lemma': '犬', 'pos': ('名詞',)}])
        events = []
        pipeline = Pipeline([GrammarStep(rules=())],
                            on_progress=lambda *args, **kwargs: events.append((args, kwargs)))
        pipeline.run(Artifact([sentence] * 3))
        self.assertEqual([args[1:3] for args, _ in events], [(0, 3), (1, 3), (2, 3), (1, 1)])
        self.assertTrue(all(args[0] == ProcessingStep.GRAMMAR_SHADOW for args, _ in events))
        self.assertIn('source-supported grammar occurrences', events[-1][1]['additional_text'])


if __name__ == '__main__':
    unittest.main()
