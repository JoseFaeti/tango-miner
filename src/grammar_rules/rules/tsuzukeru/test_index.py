import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TSUZUKERU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tsuzukeru.index import match_tsuzukeru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TsuzukeruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読み続ける。': '読み続ける', '雨が降り続けた。': '降り続けた',
            '走り続けます。': '走り続けます', '食べ続けなかった。': '食べ続けなかった',
            '歩きつづけて。': '歩きつづけて', '読み続ければ。': '読み続ければ',
            '仕事を続ける。': None, '雨が続く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsuzukeru(aligned)],
                                 [] if expected is None else [expected])

    def test_split_tokens_and_incomplete_forms(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('続け', '続ける', ('動詞', '非自立可能'))
        past = tok('た', 'た', ('助動詞',))
        for raw, expected in [([verb, suffix, past], ['読み続けた']),
                              ([verb, suffix], []),
                              ([tok('本', '本', ('名詞',)), suffix, past], [])]:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_tsuzukeru(aligned)], expected)
        aligned, error = aligned_tokens('読み 続けた', [verb, suffix, past])
        self.assertIsNone(error)
        self.assertEqual(list(match_tsuzukeru(aligned)), [])

    def test_card_preserves_lexical_candidate(self):
        source = sentence('私は本を読み続けると思います。', [tok('私は本を', '本', ('名詞',)),
                          tok('読み続ける', '読み続ける', ('動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読み続ける': object()}
        stage.add_cards(words)
        self.assertIn('読み続ける', words)
        self.assertEqual(words[TSUZUKERU_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUZUKERU_EXPRESSION].sentences[0].surface_form, '読み続ける')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
