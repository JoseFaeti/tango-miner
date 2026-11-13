import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nioite_niokeru.index import RULE, match_nioite_niokeru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NioiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'会議において発表する。': 'において', '現代においては重要だ。': 'において',
                 '学校における教育。': 'における', 'それにおいての問題。': 'において',
                 '机に本をおいてください。': None,
                 '机に置いてください。': None, '机に置ける本。': None,
                 '会議に おいて発表する。': None, 'おいて行く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nioite_niokeru(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('会議において発表する。', [
            tok('会議', '会議', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('おい', 'おく', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('発表する。', '発表', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'おい', 'て'])
        words = {'会議': object()}
        stage.add_cards(words)
        self.assertIn('会議', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'において')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
