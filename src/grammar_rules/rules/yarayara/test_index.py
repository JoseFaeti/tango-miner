import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.yarayara.index import RULE, match_yarayara


class YarayaraTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本やら雑誌やらを買った。': 2, '嬉しいやら悲しいやら複雑だ。': 2,
                 '泣くやら笑うやら忙しい。': 2, '行ったやら来たやら。': 2,
                 '本やら雑誌やら新聞やら。': 3, '何やら怪しい。': 0,
                 'やらないやらない。': 0, '本やら。犬やら。': 0,
                 '本 やら犬やら。': 0}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_yarayara(tokens)], ['やら'] * expected)

    def test_reservation_and_identity(self):
        source = sentence('本やら雑誌やらを買った。', [tok('本', '本', ('名詞',)),
                          tok('やら', 'やら', ('助詞', '副助詞')),
                          tok('雑誌', '雑誌', ('名詞',)),
                          tok('やら', 'やら', ('助詞', '副助詞')),
                          tok('を', 'を', ('助詞', '格助詞')),
                          tok('買っ', '買う', ('動詞',)), tok('た', 'た', ('助動詞',)),
                          tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['やら', 'やら'])
        words = {'本': object(), '雑誌': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'やら')
        self.assertIn('本', words)
        self.assertIn('雑誌', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
