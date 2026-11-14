import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimokakawarazu.index import RULE, match_nimokakawarazu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NimokakawarazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨にもかかわらず出かけた。': 'にもかかわらず',
            '雨にも関わらず出かけた。': 'にも関わらず',
            '忙しいにも拘わらず来た。': 'にも拘わらず',
            'それにもかかわらず帰った。': 'にもかかわらず',
            '彼にも関わらずに生きる権利がある。': None,
            '天候にかかわらず開催する。': None,
            '仕事にも関わった。': None,
            '雨にも かかわらず出かけた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimokakawarazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('雨にもかかわらず出かけた。', [
            tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('も', 'も', ('助詞',)), tok('かかわら', 'かかわる', ('動詞',)),
            tok('ず', 'ず', ('助動詞',)), tok('出かけた。', '出かける', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'も', 'かかわら', 'ず'])
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にもかかわらず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
