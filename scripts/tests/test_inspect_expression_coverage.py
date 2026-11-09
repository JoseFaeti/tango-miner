import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.inspect_expression_coverage import inspect
from src.TokenizedSentence import TokenizedSentence


class ExpressionCoverageTests(unittest.TestCase):
    def test_checks_written_and_reading_forms_and_reports_tokens(self):
        tokens = [dict(surface='A', lemma='a', reading='x', pos=('noun',)),
                  dict(surface='B', lemma='b', reading='y', pos=('verb',))]
        sentence = TokenizedSentence('AB', None, Path('test'), tokens)
        with patch('scripts.inspect_expression_coverage.get_jmdict') as get_dictionary, \
                patch('scripts.inspect_expression_coverage.tokenize_raw_sentences', return_value=[sentence]):
            get_dictionary.return_value.lookup_word.side_effect = lambda word: [1] if word == 'xy' else []
            self.assertEqual(inspect(['AB']), [{
                'expression': 'AB', 'jmdict': False, 'reading': 'xy',
                'reading_in_jmdict': True,
                'tokens': [{'surface': 'A', 'lemma': 'a', 'pos': ['noun']},
                           {'surface': 'B', 'lemma': 'b', 'pos': ['verb']}],
            }])
