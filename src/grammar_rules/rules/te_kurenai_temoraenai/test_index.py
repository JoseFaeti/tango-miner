import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.te_kurenai_temoraenai.index import RULE, match_te_kurenai_temoraenai


class RequestTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '手伝ってくれない？': 'てくれない',
            '読んでくれないか。': 'でくれないか',
            '見てもらえない？': 'てもらえない',
            '読んでもらえないか。': 'でもらえないか',
            '本をもらえない？': None, '雨でくれない？': None,
            '読んでくれる？': None, '読んでもらわない？': None,
            '読んでくれなかった。': None, '読んでくれない人。': None,
            '読んで くれない？': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_kurenai_temoraenai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('これを食べてくれない？', [
            tok('これを', 'これ', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('くれ', 'くれる', ('動詞', '非自立可能', '*', '*', '下一段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞',)), tok('？', '？', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'くれ', 'ない'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てくれない')


if __name__ == '__main__':
    unittest.main()
