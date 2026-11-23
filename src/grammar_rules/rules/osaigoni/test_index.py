import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.osaigoni.index import RULE, match_osaigoni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OsaigoniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今日を最後に引退する。': 'を最後に',
            'これを最後に会っていない。': 'を最後に',
            '公演を最後にして活動を終えた。': 'を最後に',
            '今日をさいごにやめる。': 'をさいごに',
            '最後に紹介する。': None,
            '最後の公演を見た。': None,
            '今日を 最後に辞める。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_osaigoni(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('今日を最後に引退する。', [
            tok('今日', '今日', ('名詞',)), tok('を', 'を', ('助詞', '格助詞')),
            tok('最後', '最後', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('引退する。', '引退', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '最後', 'に'])
        words = {'今日': object()}
        stage.add_cards(words)
        self.assertIn('今日', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を最後に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
