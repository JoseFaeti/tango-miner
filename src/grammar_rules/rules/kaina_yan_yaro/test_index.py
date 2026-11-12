import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import KAINA_YAN_YARO_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kaina_yan_yaro.index import RULE, match_kaina_yan_yaro
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DialectEndingsTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行くかいな。': 'かいな', 'いいやん。': 'やん',
                 '行くやん。': 'やん', '学生やん。': 'やん',
                 '行くやろ。': 'やろ', '行かんやろ。': 'やろ',
                 'かいなを掴む。': None, '食べてやろう。': None,
                 'やろう。': None, '行くかい。': None, '行くか いな。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kaina_yan_yaro(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('明日もあの場所へ行くやろ。', [
            tok('明日もあの場所へ', '場所', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('やろ', 'や', ('助動詞', '*', '*', '*', '助動詞-ヤ', '意志推量形')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        reserved = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens if t.get('grammar_reserved')], ['やろ'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn(RULE, RULES)
        self.assertEqual(words[KAINA_YAN_YARO_EXPRESSION].frequency, 2)
        self.assertEqual(words[KAINA_YAN_YARO_EXPRESSION].sentences[0].surface_form, 'やろ')
