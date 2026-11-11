import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nado_nanka_nante.index import RULE, match_nado_nanka_nante
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NadoNankaNanteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本などを読む。': ['など'], '私なんかには無理だ。': ['なんか'],
                 '行くなんて信じられない。': ['なんて'], '猫なんて嫌いだ。': ['なんて'],
                 'なんか食べたい。': [], 'なんて美しい。': [],
                 '何か食べたい。': [], '何て言った？': [], '本 などを読む。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nado_nanka_nante(tokens)], expected)

    def test_reservation_identity_and_lexical_evidence(self):
        source = sentence('私は本などを読みます。', [tok('私は', '私', ('代名詞',)),
            tok('本', '本', ('名詞',)), tok('など', 'など', ('助詞', '副助詞')),
            tok('を読みます。', '読む', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['など'])
        lexical = object()
        words = {'本': object(), 'など': lexical}
        stage.add_cards(words)
        self.assertIs(words['など'], lexical)
        self.assertIn('本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'など')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
