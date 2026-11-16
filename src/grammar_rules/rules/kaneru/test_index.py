import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, KANERU_EXPRESSION
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kaneru.index import RULE, match_kaneru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaneruTests(unittest.TestCase):
    def test_split_merged_and_boundaries(self):
        verb = tok('決め', '決める', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        suffix = lambda surface: tok(surface, 'かねる', ('動詞', '非自立可能'))
        cases = [
            ([verb, suffix('かねる')], ['決めかねる']),
            ([verb, suffix('かね'), tok('た', 'た', ('助動詞',))], ['決めかねた']),
            ([verb, suffix('かね'), tok('て', 'て', ('助詞',))], ['決めかねて']),
            ([verb, suffix('かね'), tok('ます', 'ます', ('助動詞',))], ['決めかねます']),
            ([verb, suffix('かねれ'), tok('ば', 'ば', ('助詞',))], ['決めかねれば']),
            ([verb, suffix('かね'), tok('ない', 'ない', ('助動詞',))], []),
            ([verb, suffix('かね'), tok('ませ', 'ます', ('助動詞',)), tok('ん', 'ぬ', ('助動詞',))], []),
            ([verb, suffix('かね')], []),
            ([suffix('かねる')], []),
            ([tok('金', '金', ('名詞',)), suffix('かねる')], []),
            ([tok('決めかねる', '決めかねる', ('動詞',))], ['決めかねる']),
        ]
        for raw, expected in cases:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_kaneru(aligned)], expected)
        aligned, error = aligned_tokens('決め かねる', [verb, suffix('かねる')])
        self.assertIsNone(error)
        self.assertEqual(list(match_kaneru(aligned)), [])

    def test_sudachi_examples(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = [('返事を決めかねる。', '決めかねる'),
                 ('それには同意しかねます。', 'しかねます'),
                 ('判断しかねた。', 'しかねた'),
                 ('事故が起こりかねない。', None),
                 ('仕事を兼ねる。', None)]
        for text, expected in cases:
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kaneru(aligned)],
                                 [] if expected is None else [expected], repr(raw))

    def test_card_keeps_lexical_evidence(self):
        self.assertIn(RULE, RULES)
        self.assertIn(KANERU_EXPRESSION, GRAMMAR_EXPRESSIONS)
        source = sentence('私は返事を決めかねると思います。', [tok('私は返事を', '返事', ('名詞',)),
            tok('決め', '決める', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('かねる', 'かねる', ('動詞', '非自立可能')), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'決める': object()}
        stage.add_cards(words)
        self.assertIn('決める', words)
        self.assertEqual(words[KANERU_EXPRESSION].frequency, 2)
        self.assertEqual(words[KANERU_EXPRESSION].sentences[0].surface_form, '決めかねる')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))

    def test_dictionary_compounds_take_precedence(self):
        from pathlib import Path
        from src.steps.TokenizeStep.index import tokenize_raw_sentences
        from src.steps.MergeTokensStep.index import combine_tokens_jmdict, get_jmdict

        try:
            dictionary = get_jmdict()
            texts = ['そんな俺を見かねたのか工場長が力を貸してくれてな。',
                     '知らせを受けそなたらの帰りを待ちかねていたのじゃ。',
                     '知らせを受けそなたの帰りを待ちかねていたのじゃ。',
                     '返事を決めかねる。']
            sources = tokenize_raw_sentences(texts, tag='test', source_path=Path('test.txt'))
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'dictionary or Sudachi unavailable: {exc}')
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        stage.process(Artifact(sources))
        for index, lemma in enumerate(('見かねる', '待ちかねる', '待ちかねる')):
            with self.subTest(text=texts[index]):
                self.assertFalse(stage.spans.get(index))
                merged = combine_tokens_jmdict(sources[index].tokens, dictionary)
                compound = next(t for t in merged if t['lemma'] == lemma)
                self.assertEqual(compound['analysis_rule'], 'jmdict_kaneru_verb')
                self.assertTrue(compound['components'])
        self.assertTrue(stage.spans.get(3))

    def test_compound_lookup_requires_a_dictionary_verb(self):
        from src.steps.MergeTokensStep.merger import _merge_kaneru_verb

        first = tok('持ち', '持つ', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('かね', 'かねる', ('接尾辞', '動詞的', '*', '*', '下一段', '連用形-一般'))
        for token in (first, suffix):
            token.update(base_form=token['lemma'], reading='ヨミ')
        class Dictionary:
            def __init__(self, pos):
                self.pos = pos
            def lookup_word(self, word):
                return [{'senses': [{'pos': [self.pos]}]}] if word == '持ちかねる' else []
            def get_reading(self, entry, word):
                return 'もちかねる'

        self.assertIsNone(_merge_kaneru_verb([first, suffix], Dictionary('noun')))
        compound = _merge_kaneru_verb([first, suffix], Dictionary('Ichidan verb'))
        self.assertEqual(compound['lemma'], '持ちかねる')
        self.assertEqual(compound['surface'], '持ちかね')
        self.assertEqual(compound['pos'][0], '動詞')
