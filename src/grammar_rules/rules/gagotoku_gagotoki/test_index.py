import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.gagotoku_gagotoki.index import (
    RULE,
    match_gagotoku_gagotoki,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GagotokuGagotokiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '風がごとく走る。': 'がごとく',
            '飛ぶが如く走る。': 'が如く',
            '彼がごとき人物だ。': 'がごとき',
            '夢が如き話だ。': 'が如き',
            '夢のごとく消えた。': None,
            '彼がごとしと語る。': None,
            '風が ごとく走る。': None,
            '風 がごとく走る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_gagotoku_gagotoki(aligned)],
                                 [expected] if expected else [])

    def test_reservation_example_and_registry(self):
        source = sentence('風がごとき速さで彼は走った。', [
            tok('風', '風', ('名詞', '普通名詞')),
            tok('が', 'が', ('助詞', '格助詞')),
            tok('ごとき', 'ごとし', ('助動詞',)),
            tok('速さ', '速さ', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('彼', '彼', ('代名詞',)), tok('は', 'は', ('助詞',)),
            tok('走っ', '走る', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=RULES)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['が', 'ごとき'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {'風': object(), '速さ': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'がごとき')
        self.assertIn('風', words)
        self.assertIn('速さ', words)
