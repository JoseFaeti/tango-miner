import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.sobakara.index import RULE, match_sobakara
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SobakaraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '覚えるそばから忘れる。': 'そばから',
            '片付けたそばから散らかす。': 'そばから',
            '読んだそばから忘れる。': 'そばから',
            '家のそばから見る。': None,
            'そばから離れる。': None,
            '静かなそばから見える。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_sobakara(aligned)],
                                 [] if expected is None else [expected])

    def test_card_retains_attached_verb(self):
        source = sentence('覚えるそばから忘れてしまう。', [
            tok('覚える', '覚える', ('動詞', '一般', '*', '*', '下一段', '連体形-一般')),
            tok('そば', 'そば', ('名詞',)), tok('から', 'から', ('助詞',)),
            tok('忘れてしまう。', '忘れる', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'覚える': object()}
        stage.add_cards(words)
        self.assertIn('覚える', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'そばから')
