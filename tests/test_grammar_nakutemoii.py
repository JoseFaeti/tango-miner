import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.nakutemoii import RULE, match_nakutemoii
from src.grammar_rules.rules.nai import match_nai
from src.grammar_rules.rules.te_mo import match_te_mo
from src.grammar_rules.rules.te_mo_ii import match_te_mo_ii
from tests.test_grammar_te_iru import sentence, tok


class NakutemoiiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行かなくてもいい。': 'なくてもいい',
                 '食べなくても良いです。': 'なくても良い',
                 '行かなくてもよかった。': 'なくてもよかった',
                 '行かなくても大丈夫。': None, '行かなくても困る。': None,
                 '行ってもいい。': None, '行かなくてもよくない。': None,
                 '行かなくても いい。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nakutemoii(aligned)],
                                 [] if expected is None else [expected])

    def test_overlap_reservation_and_identity(self):
        source = sentence('今日は行かなくてもいい。', [
            tok('今日は', '今日', ('名詞',)),
            tok('行か', '行く', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('なく', 'ない', ('助動詞', '*', '*', '*', '*', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('も', 'も', ('助詞', '係助詞')),
            tok('いい', 'いい', ('形容詞', '*')),
            tok('。', '。', ('補助記号',))])
        aligned, error = aligned_tokens(source.text, source.tokens)
        self.assertIsNone(error)
        for matcher in (match_nai, match_te_mo, match_te_mo_ii):
            self.assertEqual(list(matcher(aligned)), [])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['なく', 'て', 'も', 'いい'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なくてもいい')


if __name__ == '__main__':
    unittest.main()
