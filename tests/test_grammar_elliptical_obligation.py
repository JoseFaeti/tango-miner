import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.elliptical_obligation import RULE, match_elliptical_obligation
from src.grammar_rules.rules.obligation import match_obligation
from tests.test_grammar_te_iru import sentence, tok


class EllipticalObligationTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かないと。': 'ないと', '食べなくちゃ。': 'なくちゃ',
            '行かなきゃ。': 'なきゃ', '帰らなきゃね。': 'なきゃ',
            '行かないと間に合わない。': None,
            '行かなきゃ困る。': None, '行かなくちゃならない。': None,
            '行かないといけない。': None, 'お金がない。': None,
            '行か なきゃ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_elliptical_obligation(aligned)],
                                 [] if expected is None else [expected])

    def test_split_tokens_reservation_and_complete_control(self):
        verb = ('動詞', '一般', '*', '*', '五段', '未然形-一般')
        parts = [tok('もう学校へ', '学校', ('名詞',)),
                 tok('行か', '行く', verb), tok('ない', 'ない', ('助動詞',)),
                 tok('と', 'と', ('助詞',)), tok('ね', 'ね', ('助詞',)),
                 tok('。', '。', ('補助記号',))]
        source = sentence('もう学校へ行かないとね。', parts)
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(len(stage.occurrences[RULE.id].examples), 1)
        result = ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'と'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないと')

        complete = [tok('行か', '行く', verb), tok('ないと', 'ないと', ('助詞',)),
                    tok('いけ', 'いける', ('動詞',)), tok('ない', 'ない', ('助動詞',))]
        aligned, error = aligned_tokens('行かないといけない', complete)
        self.assertIsNone(error)
        self.assertEqual(list(match_elliptical_obligation(aligned)), [])
        self.assertEqual(len(list(match_obligation(aligned))), 1)
