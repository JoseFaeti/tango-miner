import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.zaruoenai.index import RULE, match_zaruoenai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ZaruOenaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かざるを得ない。': 'ざるを得ない', 'せざるを得なかった。': 'ざるを得なかった',
            '食べざるをえません。': 'ざるをえません', '来ざるを得ず。': 'ざるを得ず',
            '言わざるを得ぬ。': 'ざるを得ぬ', 'ざるを得ない。': None,
            'ざるを得た。': None, '行かざるを得た。': None,
            '行かざる を得ない。': None, '行かざるを得なかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zaruoenai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_preserves_verb_and_card_example(self):
        parts = [tok('もう学校へ', '学校', ('名詞',)), tok('行か', '行く', ('動詞',)), tok('ざる', 'ぬ', ('助動詞',)),
                 tok('を', 'を', ('助詞',)), tok('得', '得る', ('動詞',)),
                 tok('ない', 'ない', ('助動詞',))]
        source = sentence('もう学校へ行かざるを得ない', parts)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ざる', 'を', '得', 'ない'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ざるを得ない')
        self.assertIn('行く', words)
