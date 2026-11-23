import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ngatameni.index import RULE, match_ngatameni
from src.grammar_rules.rules.nu_n.index import match_nu_n
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NgatameniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'勝たんがために努力する。': 'んがために',
                 '生きんがために働く。': 'んがために',
                 '成功せんがために戦う。': 'んがために',
                 '人がために働く。': None, '勝つために働く。': None,
                 '勝たんがための努力。': None, '勝たぬがために働く。': None,
                 '勝たん がために働く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ngatameni(aligned)],
                                 [expected] if expected else [])
                if expected:
                    self.assertEqual(list(match_nu_n(aligned)), [])

    def test_card_and_reservation(self):
        text = '勝たんがために努力する。'
        raw = [tok('勝た', '勝つ', ('動詞', '一般', '*', '*', '五段-タ行', '未然形-一般')),
               tok('ん', 'ぬ', ('助動詞',)), tok('が', 'が', ('助詞', '接続助詞')),
               tok('ため', 'ため', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
               tok('努力する。', '努力する', ('動詞',))]
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([sentence(text, raw)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ん', 'が', 'ため', 'に'])
        words = {'勝つ': object()}
        stage.add_cards(words)
        self.assertIn('勝つ', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'んがために')
