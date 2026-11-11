import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nakuteha_nakereba.index import RULE, match_nakuteha_nakereba
from src.grammar_rules.rules.obligation.index import match_obligation
from src.grammar_rules.rules.ba.index import match_ba
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NakutehaNakerebaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かなくては。': 'なくては', '食べなければね。': 'なければ',
            '行かなければ困る。': None, '行かなくてはならない。': None,
            '行かなければいけない。': None, 'お金がなくては困る。': None,
            '行か なければ。': None, '行かなけれ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nakuteha_nakereba(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_complete_obligation(self):
        verb = ('動詞', '一般', '*', '*', '五段', '未然形-一般')
        parts = [tok('もう学校へ', '学校', ('名詞',)), tok('行か', '行く', verb), tok('なけれ', 'ない', ('助動詞',)),
                 tok('ば', 'ば', ('助詞',)), tok('。', '。', ('補助記号',))]
        source = sentence('もう学校へ行かなければ。', parts)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['なけれ', 'ば'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なければ')
        self.assertIn('行く', words)
        complete = parts[1:4] + [tok('なら', 'なる', ('動詞',)), tok('ない', 'ない', ('助動詞',))]
        aligned, error = aligned_tokens('行かなければならない', complete)
        self.assertIsNone(error)
        self.assertEqual(list(match_nakuteha_nakereba(aligned)), [])
        self.assertEqual(len(list(match_obligation(aligned))), 1)

    def test_final_particle_does_not_make_conditional_ba(self):
        verb = ('動詞', '一般', '*', '*', '五段', '未然形-一般')
        negative = ('助動詞', '*', '*', '*', '助動詞-ナイ', '仮定形-一般')
        parts = [tok('行か', '行く', verb), tok('なけれ', 'ない', negative),
                 tok('ば', 'ば', ('助詞', '接続助詞')), tok('ね', 'ね', ('助詞', '終助詞'))]
        aligned, error = aligned_tokens('行かなければね', parts)
        self.assertIsNone(error)
        self.assertEqual(len(list(match_nakuteha_nakereba(aligned))), 1)
        self.assertEqual(list(match_ba(aligned)), [])
