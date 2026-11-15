import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.bekida_bekidehanai.index import RULE, match_bekida
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BekiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くべきだ。': 'べきだ', 'するべきです。': 'べきです',
            'すべきだった。': 'べきだった', '食べるべきでした。': 'べきでした',
            '行くべきではない。': 'べきではない', '行くべきじゃなかった。': 'べきじゃなかった',
            '行くべきではありません。': 'べきではありません',
            '行くべきではありませんでした。': 'べきではありませんでした',
            '行くべきではないです。': 'べきではないです',
            '行くべき人。': None, 'べきだ。': None, '食べきる。': None,
            '行くべき。': None, '行くべき だ。': None, '行くべきだっ': None,
            '行くべきではなかっ': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bekida(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_example(self):
        source = sentence('明日は行くべきではない。', [
            tok('明日は', '明日', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('べき', 'べし', ('助動詞',)), tok('で', 'だ', ('助動詞',)),
            tok('は', 'は', ('助詞',)), tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['べき', 'で', 'は', 'ない'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'べきではない')
        self.assertIn('行く', words)
