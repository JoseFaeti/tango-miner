import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toosu.index import RULE, match_toosu


class ToosuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'最後まで走り通す。': '走り通す', '最後まで走り通した。': '走り通した',
                 '困難を生き通して。': '生き通して', '最後まで考え通します。': '考え通します',
                 '最後まで戦い通さない。': '戦い通さない', '最後まで戦い通せば。': '戦い通せば',
                 '道を通す。': None, '手を引く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toosu(aligned)],
                                 [] if expected is None else [expected], repr(raw))

    def test_boundaries_and_card_evidence(self):
        verb = tok('走り', '走る', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('とおす', 'とおす', ('動詞', '非自立可能'))
        for text, raw, expected in [('走りとおす', [verb, suffix], ['走りとおす']),
                                    ('走り とおす', [verb, suffix], []),
                                    ('とおす', [suffix], [])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_toosu(aligned)], expected)
        self.assertIn(RULE, RULES)
        source = sentence('私は最後まで走りとおすと思います。', [
            tok('私は最後まで', '最後', ('名詞',)), verb, suffix,
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        stage.process(Artifact([source, source]))
        words = {'走る': object()}
        stage.add_cards(words)
        self.assertIn('走る', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '走りとおす')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))

    def test_dictionary_compounds_take_precedence(self):
        from pathlib import Path

        from src.steps.MergeTokensStep.index import combine_tokens_jmdict, get_jmdict
        from src.steps.TokenizeStep.index import tokenize_raw_sentences

        try:
            dictionary = get_jmdict()
            texts = ['いつまでも隠し通すことはできません。',
                     '今もちょっと痛ぇけど隠し通すぜこ、根性で!',
                     '最後まで押し通した。', '最後まで走り通す。']
            sources = tokenize_raw_sentences(texts, tag='test', source_path=Path('test.txt'))
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'dictionary or Sudachi unavailable: {exc}')
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        stage.process(Artifact(sources))
        for index, lemma in enumerate(('隠し通す', '隠し通す', '押し通す')):
            with self.subTest(text=texts[index]):
                self.assertFalse(stage.spans.get(index))
                merged = combine_tokens_jmdict(sources[index].tokens, dictionary)
                compound = next(t for t in merged if t['lemma'] == lemma)
                if compound.get('analysis_rule') == 'jmdict_toosu_verb':
                    self.assertTrue(compound.get('components'))
        self.assertTrue(stage.spans.get(3))

    def test_split_compound_requires_dictionary_verb(self):
        from src.steps.MergeTokensStep.merger import _merge_toosu_verb

        first = tok('隠し', '隠す', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('通し', '通す', ('動詞', '非自立可能', '*', '*', '五段', '連用形-一般'))
        for token in (first, suffix):
            token.update(base_form=token['lemma'], reading='ヨミ')

        class Dictionary:
            def __init__(self, pos):
                self.pos = pos
            def lookup_word(self, word):
                return [{'senses': [{'pos': [self.pos]}]}] if word == '隠し通す' else []
            def get_reading(self, entry, word):
                return 'かくしとおす'

        self.assertIsNone(_merge_toosu_verb([first, suffix], Dictionary('noun')))
        compound = _merge_toosu_verb([first, suffix], Dictionary('Godan verb'))
        self.assertEqual(compound['lemma'], '隠し通す')
        self.assertEqual(compound['surface'], '隠し通し')
        self.assertEqual(len(compound['components']), 2)
