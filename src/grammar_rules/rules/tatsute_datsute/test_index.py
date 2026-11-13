import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tatsute_datsute.index import RULE, match_tatsute_datsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TatsuteDatsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行ったって無駄だ。': ['たって'], '読んだって分からない。': ['だって'],
            '高くたって買う。': ['たって'], '行かなくたっていい。': ['たって'],
            '私だって知ってる。': [], 'だって嫌だもん。': [],
            '読んだって言った。': [], '行ったって。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tatsute_datsute(aligned)], expected)

    def test_reservation_and_identity(self):
        source = sentence('明日は学校に行ったって無駄だから休もう。', [
            tok('明日は学校に', '学校', ('名詞',)),
            tok('行っ', '行く', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('たって', 'たって', ('助詞', '接続助詞')),
            tok('無駄だから休もう', '無駄', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['たって'])
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
