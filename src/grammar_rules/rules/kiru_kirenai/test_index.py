import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kiru_kirenai.index import RULE, match_kiru_kirenai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KiruKirenaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本を読みきる。': '読みきる', '本を読み切った。': '読み切った',
                 '食べきれない。': '食べきれない', '食べ切れなかった。': '食べ切れなかった',
                 '使い切ります。': '使い切ります', '走りきれません。': '走りきれません',
                 '読みきって。': '読みきって', '読みきれば。': '読みきれば',
                 '紙を切る。': None, '長く生きる。': None, '売り切れる。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kiru_kirenai(aligned)],
                                 [] if expected is None else [expected], repr(raw))

    def test_boundaries_and_card_evidence(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('きれ', 'きれる', ('動詞', '非自立可能'))
        negative = tok('ない', 'ない', ('助動詞',))
        for text, raw, expected in [('読みきれない', [verb, suffix, negative], ['読みきれない']),
                                    ('読み きれない', [verb, suffix, negative], []),
                                    ('読みきれ', [verb, suffix], [])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_kiru_kirenai(aligned)], expected)
        self.assertIn(RULE, RULES)
        source = sentence('私は本を読みきれないと思います。', [
            tok('私は本を', '本', ('名詞',)), verb, suffix, negative,
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '読みきれない')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
