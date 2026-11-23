import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tokoroomiruto.index import RULE, match_tokoroomiruto
from src.grammar_rules.rules.tokoroo.index import RULE as TOKOROO_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TokoroomirutoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼が笑っているところを見ると元気そうだ。': 'ところを見ると',
            '誰もいないところを見ると休みらしい。': 'ところを見ると',
            '静かなところをみると安心する。': 'ところをみると',
            '昨日行った所を見ると懐かしい。': '所を見ると',
            'ところを見ると驚いた。': None,
            '彼が帰るところを見た。': None,
            '彼が帰るところを見る。': None,
            '彼が帰るところを 見ると驚く。': None,
            '彼が帰るところを見る と驚く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tokoroomiruto(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_identity_and_overlap(self):
        source = sentence('彼が帰るところを見ると安心する。', [
            tok('彼が', '彼', ('名詞',)),
            tok('帰る', '帰る', ('動詞', '一般', '*', '*', '五段-ラ行', '連体形-一般')),
            tok('ところ', 'ところ', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('見る', '見る', ('動詞',)), tok('と', 'と', ('助詞',)),
            tok('安心する。', '安心', ('名詞',))])
        stage = GrammarStep(rules=(TOKOROO_RULE, RULE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'を', '見る', 'と'])
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertIn('帰る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ところを見ると')
        self.assertNotIn(TOKOROO_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
