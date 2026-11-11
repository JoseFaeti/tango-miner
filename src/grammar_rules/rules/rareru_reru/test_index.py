import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.rareru_reru.index import RULE, match_rareru_reru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class RareruReruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べられる。': 'られる', '見られた。': 'られ',
                 '行かれる。': 'れる', '来られない。': 'られ',
                 '食べさせられる。': None, '見せられる。': 'られる',
                 '食べれる。': None, '読める。': None, 'できる。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_rareru_reru(aligned)],
                                 [] if expected is None else [expected])

    def test_card_reserves_suffix_and_preserves_verb(self):
        source = sentence('私は野菜を食べられる。', [
            tok('私は野菜を', '野菜', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '*', '*', '*', '一段', '未然形-一般')),
            tok('られる', 'られる', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['られる'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'られる')

    def test_requires_adjacent_auxiliary(self):
        verb = tok('食べ', '食べる', ('動詞', '*', '*', '*', '一段', '未然形-一般'))
        for text, suffix in [('食べ られる', tok('られる', 'られる', ('助動詞',))),
                             ('食べられる', tok('られる', 'られる', ('動詞',)))]:
            aligned, error = aligned_tokens(text, [verb, suffix])
            self.assertIsNone(error)
            self.assertEqual(list(match_rareru_reru(aligned)), [])
