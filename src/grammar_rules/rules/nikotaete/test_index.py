import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikotaete.index import RULE, match_nikotaete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikotaeteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'需要に応えて変える。': 'に応えて',
                 'それに応えて決める。': 'に応えて',
                 '状況にこたえて変える。': 'にこたえて',
                 '要望にこたえて開催する。': 'にこたえて',
                 '期待にこたえまして努力する。': 'にこたえまして',
                 'それにこたえて働く。': 'にこたえて',
                 '期待にこたえる。': None,
                 '期待にこたえた成果。': None,
                 '期待に こたえて働く。': None,
                 '状況に応えまして変える。': 'に応えまして',
                 '声に応えて答える。': 'に応えて',
                 '要求に応える。': None, '状況に応えた対応。': None,
                 '応えて答える。': None, '状況に 応えて変える。': None,
                 '状況に応じて変える。': None,
                 '質問に答えて説明する。': None,
                 '暑さが身にこたえて眠れない。': 'にこたえて',
                 '期待を応えて働く。': None,
                 '期待に、応えて働く。': None,
                 '期待に応え。て働く。': None,
                 '期待に応えても働く。': 'に応えて',
                 '期待に応えている。': 'に応えて'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikotaete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('需要に応えて価格を調整します。', [
            tok('需要', '需要', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('応え', '応える', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('価格を調整します。', '価格', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '応え', 'て'])
        words = {'需要': object()}
        stage.add_cards(words)
        self.assertIn('需要', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に応えて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_kana_forms_share_one_card_and_preserve_nouns(self):
        stage = GrammarStep()
        sources = []
        for surface, verb, tail in [('にこたえて', 'こたえ', ['て']),
                                    ('にこたえまして', 'こたえ', ['まし', 'て']),
                                    ('に応えて', '応え', ['て'])]:
            source = sentence('期待' + surface + '価格を調整します。', [
                tok('期待', '期待', ('名詞',)), tok('に', 'に', ('助詞',)),
                tok(verb, 'こたえる' if verb == 'こたえ' else '応える', ('動詞',)),
                *[tok(t, 'ます' if t == 'まし' else t,
                      ('助動詞',) if t == 'まし' else ('助詞',)) for t in tail],
                tok('価格を調整します。', '価格', ('名詞',))])
            sources.append(source)
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact(sources)))
        for source, surface in zip(data.data, ['にこたえて', 'にこたえまして', 'に応えて']):
            self.assertEqual(''.join(t['surface'] for t in source.tokens
                                     if t.get('grammar_reserved')), surface)
            self.assertFalse(source.tokens[0].get('grammar_reserved'))
        words = {'期待': object()}
        stage.add_cards(words)
        self.assertEqual(set(words), {'期待', RULE.expression})
        self.assertEqual({s.surface_form for s in words[RULE.expression].sentences},
                         {'にこたえて', 'にこたえまして', 'に応えて'})
