import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import KAKERU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kakeru.index import match_kakeru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KakeruTests(unittest.TestCase):
    def test_split_forms_and_boundaries(self):
        verb = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        suffix = lambda surface: tok(surface, 'かける', ('動詞', '非自立可能'))
        cases = [
            ([verb, suffix('かける')], ['食べかける']),
            ([verb, suffix('かけ'), tok('た', 'た', ('助動詞',))], ['食べかけた']),
            ([verb, suffix('かけ'), tok('て', 'て', ('助詞',))], ['食べかけて']),
            ([verb, suffix('かけ'), tok('ます', 'ます', ('助動詞',))], ['食べかけます']),
            ([verb, suffix('かけ'), tok('ない', 'ない', ('助動詞',))], ['食べかけない']),
            ([verb, suffix('かけれ'), tok('ば', 'ば', ('助詞',))], ['食べかければ']),
            ([verb, suffix('かけ')], []),
            ([tok('本', '本', ('名詞',)), suffix('かける')], []),
            ([tok('話しかける', '話しかける', ('動詞',))], ['話しかける']),
            ([suffix('かける')], []),
        ]
        for raw, expected in cases:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_kakeru(aligned)], expected)
        aligned, error = aligned_tokens('食べ かける', [verb, suffix('かける')])
        self.assertIsNone(error)
        self.assertEqual(list(match_kakeru(aligned)), [])

    def test_sudachi_examples(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in [('本を読みかけた。', '読みかけた'),
                               ('死にかける。', None),
                               ('椅子に掛ける。', None),
                               ('彼に話しかける。', '話しかける')]:
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kakeru(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_lexical_evidence(self):
        source = sentence('私は本を読みかけたと思います。', [tok('私は本を', '本', ('名詞',)),
            tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('かけ', 'かける', ('動詞', '非自立可能')), tok('た', 'た', ('助動詞',)),
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[KAKERU_EXPRESSION].frequency, 2)
        self.assertEqual(words[KAKERU_EXPRESSION].sentences[0].surface_form, '読みかけた')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))

    def test_nominal_attributive_and_boundaries(self):
        verb = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        no = tok('の', 'の', ('助詞',))
        cases = [([tok('食べかけ', '食べかけ', ('名詞',)), no], ['食べかけの']),
                 ([verb, tok('かけ', 'かけ', ('名詞',)), no], ['食べかけの']),
                 ([tok('仕掛け', '仕掛け', ('名詞',)), no], []),
                 ([tok('食べかけ', '食べかけ', ('名詞',))], [])]
        for raw, expected in cases:
            text = ''.join(t['surface'] for t in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_kakeru(tokens)], expected)
        tokens, error = aligned_tokens('食べかけ の', [cases[0][0][0], no])
        self.assertIsNone(error)
        self.assertEqual(list(match_kakeru(tokens)), [])

    def test_sudachi_nominal(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in [('食べかけのパン。', '食べかけの'),
                               ('仕事の仕掛けの話。', None)]:
            raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                   for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_kakeru(tokens)],
                             [] if expected is None else [expected])

    def test_nominal_card_keeps_word(self):
        noun = tok('食べかけ', '食べかけ', ('名詞',))
        source = sentence('私は食べかけのパンを見ました。', [tok('私は', '私', ('名詞',)), noun,
                          tok('の', 'の', ('助詞',)), tok('パンを見ました。', 'パン', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'食べかけ': object()}
        stage.add_cards(words)
        self.assertIn('食べかけ', words)
        self.assertEqual(words[KAKERU_EXPRESSION].sentences[0].surface_form, '食べかけの')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
