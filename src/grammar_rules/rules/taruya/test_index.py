import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.taruya.index import RULE, match_taruya
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class TaruyaTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'王たるや威厳がある。': ['たるや'],
                 '彼の怒りたるや、すさまじい。': ['たるや'],
                 'その美しさたるや言葉にできない。': ['たるや'],
                 '足るや否や。': [], '樽や箱。': [], '教師たる者。': [],
                 '堂々たる態度。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_taruya(tokens)], expected)

    def test_boundaries_and_identity(self):
        parts = [tok('怒り', '怒り', ('名詞',)), tok('たる', 'たり', ('助動詞',)),
                 tok('や', 'や', ('助詞',)), tok('、本当にすさまじかった。', 'すさまじい', ('形容詞',))]
        source = sentence('彼の怒りたるや、本当にすさまじかった。',
                          [tok('彼の', '彼', ('名詞',))] + parts)
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['たる', 'や'])
        words = {'怒り': object()}
        stage.add_cards(words)
        self.assertIn('怒り', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'たるや')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        aligned, error = aligned_tokens('怒りたる や', parts[:2] + [parts[2]])
        self.assertIsNone(error)
        self.assertEqual(list(match_taruya(aligned)), [])
