import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.haoroka.index import RULE, match_haoroka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class HaorokaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '漢字はおろかひらがなも読めない。': 'はおろか',
            '漢字は愚かひらがなも読めない。': 'は愚か',
            '名前はおろか、顔も知らない。': 'はおろか',
            '読むことはおろか話すこともできない。': 'はおろか',
            '彼は愚かだ。': None, '彼はおろかな人だ。': None,
            '愚かな人だ。': None, '彼は、おろかだ。': None,
            '彼は おろかだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_haoroka(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_topic_overlap(self):
        source = sentence('漢字はおろかひらがなも読めない。', [
            tok('漢字', '漢字', ('名詞',)), tok('は', 'は', ('助詞', '係助詞')),
            tok('おろか', 'おろか', ('名詞',)), tok('ひらがなも読めない。', 'ひらがな', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['は', 'おろか'])
        words = {'漢字': object()}
        stage.add_cards(words)
        self.assertIn('漢字', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'はおろか')
        self.assertIn('〜は', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
