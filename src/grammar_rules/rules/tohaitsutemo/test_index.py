import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tohaitsutemo.index import RULE, match_tohaitsutemo
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TohaitsutemoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '旅行とはいっても近所だ。': 'とはいっても',
            '料理とは言っても簡単だ。': 'とは言っても',
            'とはいっても、少しだ。': 'とはいっても',
            'そう言っても聞かない。': None,
            '旅行といっても近所だ。': None,
            '学校とは行っても楽しくない。': None,
            '旅行とはいっても。': None,
            '旅行と はいっても近所だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tohaitsutemo(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('料理とは言っても簡単だ。', [
            tok('料理', '料理', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('は', 'は', ('助詞', '係助詞')),
            tok('言っ', '言う', ('動詞', '一般', '*', '*', '五段-ワア行', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')), tok('も', 'も', ('助詞', '係助詞')),
            tok('簡単だ。', '簡単', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'は', '言っ', 'て', 'も'])
        words = {'料理': object()}
        stage.add_cards(words)
        self.assertIn('料理', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とは言っても')
        self.assertEqual(stage.occurrences['toitsutemo'].count, 0)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
