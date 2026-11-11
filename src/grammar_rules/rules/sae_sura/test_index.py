import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.sae_sura.index import RULE, match_sae_sura
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SaeSuraTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'子供さえ知っている。': ['さえ'], '名前すら知らない。': ['すら'],
                 '水さえあればいい。': ['さえ'], '食べさえすればいい。': ['さえ'],
                 '彼にさえ言わない。': ['さえ'], 'さえずる鳥': [],
                 '冴えない顔': [], 'すらすら読む': [], '本 さえ読む。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_sae_sura(tokens)], expected)

    def test_reservation_identity_and_lexical_evidence(self):
        source = sentence('私は本さえ読みます。', [
            tok('私は', '私', ('代名詞',)), tok('本', '本', ('名詞',)),
            tok('さえ', 'さえ', ('助詞', '副助詞')), tok('読みます。', '読む', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['さえ'])
        lexical = object()
        words = {'本': object(), 'さえ': lexical}
        stage.add_cards(words)
        self.assertIs(words['さえ'], lexical)
        self.assertIn('本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'さえ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
