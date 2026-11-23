import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.saserareru.index import RULE as PASSIVE
from src.grammar_rules.rules.seru_saseru.index import RULE, match_seru_saseru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class CausativeTests(unittest.TestCase):
    def test_analyzer_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'子供を行かせる。': 'せる', '子供に食べさせた。': 'させ',
                 '勉強させない。': 'せ', '来させます。': 'せ',
                 '泳がせればいい。': 'せれ', '見せる。': None,
                 '話せる。': None, '食べられる。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_seru_saseru(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_reservation(self):
        verb = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '未然形-一般'))
        suffix = tok('させ', 'させる', ('助動詞',))
        for text, raw in [('食べ させ', [verb, suffix]),
                          ('食べさせ', [verb, tok('させ', 'させる', ('動詞',))])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_seru_saseru(aligned)), [])
        source = sentence('私は子供に野菜を食べさせた。', [tok('私は子供に野菜を', '野菜', ('名詞',)), verb,
                          suffix, tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['させ'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'させ')

        passive = sentence('私は子供に野菜を食べさせられる。', [tok('私は子供に野菜を', '野菜', ('名詞',)), verb,
                           suffix, tok('られる', 'られる', ('助動詞',)),
                           tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([passive]))
        words = {}
        stage.add_cards(words)
        self.assertIn(PASSIVE.expression, words)
        self.assertNotIn(RULE.expression, words)
