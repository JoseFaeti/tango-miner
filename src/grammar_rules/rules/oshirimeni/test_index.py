import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.oshirimeni.index import RULE, match_oshirimeni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OShirimeniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer
        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '反対を尻目に計画を進めた。': 'を尻目に',
            '心配をしりめに出かけた。': 'をしりめに',
            '彼を尻目に笑った。': 'を尻目に',
            '太郎を尻目に走り続けた。': 'を尻目に',
            '人々の不安を尻目に、事業は拡大した。': 'を尻目に',
            '反対を尻目にして進めた。': 'を尻目に',
            '尻目にかける。': None,
            'を尻目に進めた。': None,
            '彼の尻目に映った。': None,
            '彼を尻目で見た。': None,
            '彼を尻目線で見た。': None,
            '彼を尻に敷いた。': None,
            '彼を尻目。に見た。': None,
            '彼 を尻目に進めた。': None,
            '彼を 尻目に進めた。': None,
            '彼を尻目 に進めた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_oshirimeni(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_attached_word(self):
        source = sentence('心配を尻目に出かけた。', [
            tok('心配', '心配', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('尻目', '尻目', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('出かけた。', '出かける', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '尻目', 'に'])
        words = {'心配': object()}
        stage.add_cards(words)
        self.assertIn('心配', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を尻目に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
