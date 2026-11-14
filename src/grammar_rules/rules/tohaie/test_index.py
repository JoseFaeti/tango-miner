import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tohaie.index import RULE, match_tohaie
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TohaieTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '安いとはいえ、高い。': 'とはいえ',
            '春とは言え寒い。': 'とは言え',
            'とはいえ、行く。': 'とはいえ',
            'そうとは言えない。': None,
            '彼にそうとは言える。': None,
            '安いとはいえ。': None,
            '安いと はいえ、高い。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tohaie(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('春とは言え今日は寒いですね。', [
            tok('春', '春', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('は', 'は', ('助詞', '係助詞')),
            tok('言え', '言ふ', ('動詞', '一般', '*', '*', '文語四段-ハ行', '已然形-一般')),
            tok('今日は寒いですね。', '今日', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'は', '言え'])
        words = {'春': object()}
        stage.add_cards(words)
        self.assertIn('春', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とは言え')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)


if __name__ == '__main__':
    unittest.main()
