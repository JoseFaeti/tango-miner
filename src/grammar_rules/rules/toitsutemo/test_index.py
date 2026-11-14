import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toitsutemo.index import RULE, match_toitsutemo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToitsutemoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '旅行といっても近所だ。': 'といっても',
            '料理と言っても簡単だ。': 'と言っても',
            'といっても、少しだ。': 'といっても',
            'そう言っても聞かない。': None,
            '学校と行っても楽しくない。': None,
            '旅行といっても。': None,
            '旅行と いっても近所だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsutemo(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('料理と言っても簡単だ。', [
            tok('料理', '料理', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('言っ', '言う', ('動詞', '一般', '*', '*', '五段-ワア行', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')), tok('も', 'も', ('助詞', '係助詞')),
            tok('簡単だ。', '簡単', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '言っ', 'て', 'も'])
        words = {'料理': object()}
        stage.add_cards(words)
        self.assertIn('料理', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言っても')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
