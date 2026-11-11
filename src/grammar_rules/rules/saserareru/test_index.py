import unittest
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.saserareru.index import RULE, match_saserareru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SaserareruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べさせられる。': 'させられる', '食べさせられた。': 'させられ',
                 '行かせられない。': 'せられ', '話させられる。': 'せられる',
                 'させられる。': 'せられる', '見せられる。': None,
                 '食べられる。': None, '行かせる。': None, '読まされた。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_saserareru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('私は野菜を食べさせられる。', [
            tok('私は野菜を', '野菜', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '*', '*', '*', '一段', '連用形-一般')),
            tok('させ', 'させる', ('助動詞',)), tok('られる', 'られる', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['させ', 'られる'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'させられる')

    def test_boundaries_and_parts_of_speech(self):
        verb = tok('食べ', '食べる', ('動詞', '*', '*', '*', '一段', '連用形-一般'))
        for text, parts in [
            ('食べさせ られる', [verb, tok('させ', 'させる', ('助動詞',)), tok('られる', 'られる', ('助動詞',))]),
            ('食べさせられる', [verb, tok('させ', 'させる', ('動詞',)), tok('られる', 'られる', ('助動詞',))]),
        ]:
            aligned, error = aligned_tokens(text, parts)
            self.assertIsNone(error)
            self.assertEqual(list(match_saserareru(aligned)), [])
