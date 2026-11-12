import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.to_iu_kiku.index import RULE, match_to_iu_kiku
from src.grammar_rules.rules.toiu.index import match_toiu


class ToIuKikuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行くと言う。': 'と言う', '雨だと言った。': 'と言った',
                 '行くと聞きました。': 'と聞きました', '行くと聞いた。': 'と聞いた',
                 '行くと言わない。': 'と言わない', '行くと聞いている。': 'と聞いて',
                 '行くという話。': None, '友達と言う。': None,
                 '音を聞く。': None, '行くと 聞く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_to_iu_kiku(tokens)],
                                 [] if expected is None else [expected])
                if text == '行くという話。':
                    self.assertEqual([text[a:b] for a, b in match_toiu(tokens)], ['という'])

    def test_reservation_and_identity(self):
        verb = ('動詞', '一般', '*', '*', '五段', '終止形-一般')
        source = sentence('明日は行くと言うよ。', [tok('明日は', '明日', ('名詞',)), tok('行く', '行く', verb),
                          tok('と', 'と', ('助詞', '格助詞')),
                          tok('言う', '言う', verb), tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '言う'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言う')
