import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import HAJIMERU_DASU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.hajimeru_dasu.index import match_hajimeru_dasu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HajimeruDasuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読み始める。': '読み始める', '雨が降り始めた。': '降り始めた',
            '笑い始めます。': '笑い始めます', '食べ始めなかった。': '食べ始めなかった',
            '歩きはじめて。': '歩きはじめて', '読み始めれば。': '読み始めれば',
            '雨が降り出す。': '降り出す', '泣き出した。': '泣き出した',
            '走り出します。': '走り出します', '泣き出さない。': '泣き出さない',
            '泣きだして。': '泣きだして', '走り出せば。': '走り出せば',
            '箱から取り出す。': '取り出す',
            '仕事を始める。': None, '手紙を出す。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hajimeru_dasu(aligned)],
                                 [] if expected is None else [expected])

    def test_split_tokens_and_incomplete_forms(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('始め', '始める', ('動詞', '非自立可能'))
        past = tok('た', 'た', ('助動詞',))
        for raw, expected in [([verb, suffix, past], ['読み始めた']),
                              ([verb, suffix], []),
                              ([tok('本', '本', ('名詞',)), suffix, past], [])]:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_hajimeru_dasu(aligned)], expected)
        aligned, error = aligned_tokens('読み 始めた', [verb, suffix, past])
        self.assertIsNone(error)
        self.assertEqual(list(match_hajimeru_dasu(aligned)), [])

    def test_card_preserves_lexical_candidate(self):
        source = sentence('私は本を読み始めると思います。', [tok('私は本を', '本', ('名詞',)),
                          tok('読み始める', '読み始める', ('動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読み始める': object()}
        stage.add_cards(words)
        self.assertIn('読み始める', words)
        self.assertEqual(words[HAJIMERU_DASU_EXPRESSION].frequency, 2)
        self.assertEqual(words[HAJIMERU_DASU_EXPRESSION].sentences[0].surface_form, '読み始める')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
