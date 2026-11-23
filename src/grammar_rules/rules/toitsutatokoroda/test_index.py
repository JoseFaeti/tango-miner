import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toitsutatokoroda.index import RULE, match_toitsutatokoroda
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class ToitsutatokorodaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '参加者は百人といったところだ。': 'といったところだ',
            'さすが勇者の血を継ぐものといったところかしら。': 'といったところ',
            '百人といったところかな。': 'といったところ',
            '百人といったところ？': 'といったところ',
            '百人といったところ': 'といったところ',
            '特殊な合金といった方が近いでしょうか。': None,
            '犬などといった動物が好きだ。': None,
            '百人といったところ、次へ進む。': None,
            '費用は一万円と言ったところです。': 'と言ったところです',
            '時間は一時間といったところだった。': 'といったところだった',
            '人数は十人といった所でした。': 'といった所でした',
            '百人といったところに行く。': None,
            '百人といったところで終わる。': None,
            '百人と言った。': None,
            '百人と行ったところだ。': None,
            '百人といったところだったらよい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsutatokoroda(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_reported_sentences_through_grammar_stage(self):
        from sudachipy import dictionary, tokenizer

        from src.grammar_rules.rules.toitsuta.index import RULE as EXAMPLES

        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = (
            ('さすが勇者の血を継ぐものといったところかしら。', True),
            ('特殊な合金といった方が近いでしょうか。', False),
        )
        for text, assessment in cases:
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                stage = GrammarStep()
                data = stage.process(Artifact([sentence(text, raw)]))
                ExcludeGrammarTokensStep(stage).process(data)
                words = {}
                stage.add_cards(words)
                self.assertNotIn(EXAMPLES.expression, words)
                self.assertEqual(RULE.expression in words, assessment)
                reserved = [t['surface'] for t in data.data[0].tokens
                            if t.get('grammar_reserved')]
                if assessment:
                    self.assertEqual(words[RULE.expression].sentences[0].surface_form,
                                     'といったところ')
                    self.assertTrue(all(s in reserved for s in ('と', 'いっ', 'た', 'ところ')))
                    self.assertNotIn('もの', reserved)
                else:
                    self.assertFalse(any(t.get('grammar_reserved') for t in data.data[0].tokens
                                         if t['surface'] in ('いっ', '合金')))

    def test_card_identity_and_reservation(self):
        tokens = [tok('百人', '百人', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
                  tok('言っ', '言う', ('動詞',)), tok('た', 'た', ('助動詞',)),
                  tok('ところ', 'ところ', ('名詞',)), tok('だ', 'だ', ('助動詞',)),
                  tok('。', '。', ('補助記号',))]
        source = sentence('百人と言ったところだ。', tokens)
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in data.data[0].tokens[1:6]))
        words = {'百人': object()}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言ったところだ')
        self.assertIn('百人', words)


if __name__ == '__main__':
    unittest.main()
