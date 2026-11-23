import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mono_ga_aru.index import RULE, match_mono_ga_aru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonoGaAruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'感動するものがある。': 'ものがある',
                 '美しいものがある。': 'ものがある',
                 '立派なものがある。': 'ものがある',
                 '考えさせられるものがあった。': 'ものがあった',
                 '光るものがあります。': 'ものがあります',
                 '本があります。': None, 'そのものがある。': None,
                 'ものがある。': None, '感動するものがない。': None,
                 '感動するものを見た。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mono_ga_aru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_keeps_modifier(self):
        source = sentence('彼の作品には光るものがある。', [
            tok('彼の作品には', '作品', ('名詞',)),
            tok('光る', '光る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('ある', 'ある', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,), prefer_expressions=False)
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['もの', 'が', 'ある'])
        words = {'光る': object()}
        stage.add_cards(words)
        self.assertIn('光る', words)
        self.assertIn(RULE.expression, words)
