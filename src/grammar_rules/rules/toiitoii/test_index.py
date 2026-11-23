import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiitoii.index import RULE
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class ToiitoiiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '味といい香りといい素晴らしい。': ['といい'],
            '顔と言い、性格といい美しい。': ['と言い'],
            '味といい香りといい色といい素晴らしい。': ['といい', 'といい'],
            '彼といい友達になった。': [], '晴れるといいな。': [],
            '味といい。香りといい素晴らしい。': [],
            '味と いい香りといい素晴らしい。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected)

    def test_reservation_and_identity(self):
        text = '味といい香りといい素晴らしい。'
        raw = [tok('味', '味', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
               tok('いい', 'いい', ('形容詞',)), tok('香り', '香り', ('名詞',)),
               tok('と', 'と', ('助詞', '格助詞')), tok('いい', 'いい', ('形容詞',)),
               tok('素晴らしい', '素晴らしい', ('形容詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep()
        artifact = stage.process(Artifact([sentence(text, raw)]))
        artifact = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[3].get('grammar_reserved'))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {'香り': object()}
        stage.add_cards(words)
        self.assertIn('香り', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'といい')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
