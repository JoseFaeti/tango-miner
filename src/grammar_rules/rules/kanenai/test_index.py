import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kanenai.index import RULE, match_kanenai
from src.grammar_rules.rules.kaneru.index import match_kaneru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KanenaiTests(unittest.TestCase):
    def test_boundaries_and_inflections(self):
        verb = tok('起こり', '起こる', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('かね', 'かねる', ('接尾辞', '動詞的'))
        negative = lambda surface: tok(surface, 'ない', ('助動詞',))
        cases = [
            ([verb, suffix, negative('ない')], ['起こりかねない']),
            ([verb, suffix, negative('なかっ'), tok('た', 'た', ('助動詞',))], ['起こりかねなかった']),
            ([verb, suffix, negative('なく')], ['起こりかねなく']),
            ([verb, suffix, negative('なけれ'), tok('ば', 'ば', ('助詞',))], ['起こりかねなければ']),
            ([tok('起こりかね', '起こりかねる', ('動詞',)), negative('ない')], ['起こりかねない']),
            ([suffix, negative('ない')], []),
            ([tok('金', '金', ('名詞',)), suffix, negative('ない')], []),
            ([verb, suffix, negative('なかっ')], []),
            ([verb, tok('かねる', 'かねる', ('動詞',))], []),
        ]
        for raw, expected in cases:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_kanenai(aligned)], expected)
        aligned, _ = aligned_tokens('起こり かねない', [verb, suffix, negative('ない')])
        self.assertEqual(list(match_kanenai(aligned)), [])

    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in [('事故が起こりかねない。', '起こりかねない'),
                               ('誤解を招きかねなかった。', '招きかねなかった'),
                               ('彼は仕事を兼ねない。', None),
                               ('返事を決めかねる。', None)]:
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kanenai(aligned)],
                                 [] if expected is None else [expected], repr(raw))
                if expected:
                    self.assertEqual(list(match_kaneru(aligned)), [])

    def test_card_and_lexical_evidence(self):
        source = sentence('事故が起こりかねない。', [tok('事故が', '事故', ('名詞',)),
            tok('起こり', '起こる', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('かね', 'かねる', ('接尾辞', '動詞的')), tok('ない', 'ない', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[0].tokens))
        words = {'起こる': object()}
        stage.add_cards(words)
        self.assertIn('起こる', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '起こりかねない')
