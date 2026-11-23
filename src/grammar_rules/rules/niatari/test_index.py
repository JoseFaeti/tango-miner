import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niatari.index import RULE, match_niatari
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NiatariTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '開会に当たり、挨拶する。': 'に当たり',
            '入学にあたり準備する。': 'にあたり',
            '契約するに当たり確認する。': 'に当たり',
            '開会に当たりまして挨拶する。': None,
            '壁に当たり跳ね返る。': None,
            '仕事に当たります。': None,
            '開会に当たって挨拶する。': None,
            '開会に 当たり挨拶する。': None,
            '開会を当たり挨拶する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niatari(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('開会に当たり、挨拶する。', [
            tok('開会', '開会', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('当たり', '当たる', ('動詞',)), tok('、', '、', ('補助記号',)),
            tok('挨拶する。', '挨拶', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '当たり'])
        words = {'開会': object()}
        stage.add_cards(words)
        self.assertIn('開会', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に当たり')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
