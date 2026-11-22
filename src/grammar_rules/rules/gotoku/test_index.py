import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.gotoku.index import RULE, match_gotoku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GotokuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '夢のごとく消えた。': 'のごとく',
            '風の如く走る。': 'の如く',
            '鹿のごとく走る。': 'のごとく',
            '知っているかのごとく話す。': None,
            '風がごとく走る。': None,
            '夢のごとき話だ。': None,
            '夢の ごとく消えた。': None,
            '夢 のごとく消えた。': None,
            '五徳を買った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_gotoku(aligned)],
                                 [expected] if expected else [])

    def test_reservation_example_and_registry(self):
        source = sentence('あの美しい夢のごとく消えた。', [
            tok('あの', 'あの', ('連体詞',)), tok('美しい', '美しい', ('形容詞',)),
            tok('夢', '夢', ('名詞', '普通名詞')),
            tok('の', 'の', ('助詞', '格助詞')),
            tok('ごとく', 'ごとし', ('助動詞',)),
            tok('消え', '消える', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=RULES)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['の', 'ごとく'])
        self.assertFalse(result.data[0].tokens[2].get('grammar_reserved'))
        words = {'夢': object(), '消える': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'のごとく')
        self.assertIn('夢', words)
        self.assertIn('消える', words)
