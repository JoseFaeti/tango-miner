import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nuku.index import RULE, match_nuku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NukuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'最後まで走り抜く。': '走り抜く', '最後まで走り抜いた。': '走り抜いた',
                 '困難を生き抜いて。': '生き抜いて', '最後まで考え抜きます。': '考え抜きます',
                 '最後まで戦い抜かない。': '戦い抜かない', '最後まで戦い抜けば。': '戦い抜けば',
                 '歯を抜く。': None, '手を引く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nuku(aligned)],
                                 [] if expected is None else [expected], repr(raw))

    def test_boundaries_and_card_evidence(self):
        verb = tok('走り', '走る', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('ぬく', 'ぬく', ('動詞', '非自立可能'))
        for text, raw, expected in [('走りぬく', [verb, suffix], ['走りぬく']),
                                    ('走り ぬく', [verb, suffix], []),
                                    ('ぬく', [suffix], [])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_nuku(aligned)], expected)
        self.assertIn(RULE, RULES)
        source = sentence('私は最後まで走りぬくと思います。', [
            tok('私は最後まで', '最後', ('名詞',)), verb, suffix,
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        stage.process(Artifact([source, source]))
        words = {'走る': object()}
        stage.add_cards(words)
        self.assertIn('走る', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '走りぬく')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
