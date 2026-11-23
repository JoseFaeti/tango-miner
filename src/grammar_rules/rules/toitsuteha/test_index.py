import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toitsuteha.index import RULE, match_toitsuteha
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class ToitsutehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '暑いといっては窓を開け、寒いと言っては閉める。': ['といっては', 'と言っては'],
            '何かといっては文句を言う。': ['といっては'],
            '友達と行っては帰る。': [],
            '暑いといっても窓を開けない。': [],
            '暑いと いっては窓を開ける。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsuteha(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('暑いといっては窓を開ける。', [
            tok('暑い', '暑い', ('形容詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('いっ', 'いう', ('動詞',)), tok('て', 'て', ('助詞', '接続助詞')),
            tok('は', 'は', ('助詞', '係助詞')), tok('窓を開ける', '窓', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いっ', 'て', 'は'])
        words = {'暑い': object()}
        stage.add_cards(words)
        self.assertIn('暑い', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'といっては')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
