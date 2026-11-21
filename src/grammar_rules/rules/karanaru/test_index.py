import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.karanaru.index import RULE, match_karanaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaranaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'水と塩からなる。': 'からなる', '三人から成る。': 'から成る',
                 '水からなった。': 'からなった', '水からなっている。': 'からなって',
                 'これからなる。': 'からなる', '家から鳴る音。': None,
                 'そこからなるべく早く。': None, '子供から大人になる。': None,
                 '水から なる。': None, '水からならない。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karanaru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('この物質は水からなっている。', [
            tok('この', 'この', ('連体詞',)), tok('物質', '物質', ('名詞',)),
            tok('は', 'は', ('助詞',)), tok('水', '水', ('名詞',)),
            tok('から', 'から', ('助詞', '格助詞')),
            tok('なっ', 'なる', ('動詞',)), tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['から', 'なっ', 'て'])
        words = {'水': object()}
        stage.add_cards(words)
        self.assertIn('水', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'からなって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
