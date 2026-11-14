import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ge.index import GE_EXPRESSION, match_ge
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'悲しげな顔': 'げ', '楽しげに笑う': 'げ', '寂しげだ': 'げ',
                 '不安げな人': 'げ', '得意げに話す': 'げ', '意味ありげな目': 'げ',
                 '言いたげな顔': None, '投げる': None, '髭': None, '大げさ': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ge(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        host = tok('悲し', '悲しい', ('形容詞', '一般', '*', '*', '形容詞', '語幹-一般'))
        suffix = tok('げ', 'げ', ('接尾辞', '形状詞的'))
        for text, raw in [('悲し げ', [host, suffix]),
                          ('悲しげ', [host, tok('げ', 'げ', ('名詞',))]),
                          ('猫げ', [tok('猫', '猫', ('名詞', '普通名詞', '一般')), suffix]),
                          ('げ', [suffix])]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_ge(tokens)), [])
        source = sentence('彼女は悲しげな顔で歩いていた。', [
            tok('彼女は', '彼女', ('名詞',)), host, suffix,
            tok('な顔で歩いていた。', '顔', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        result = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['げ'])
        words = {'悲しい': object()}
        stage.add_cards(words)
        self.assertIn('悲しい', words)
        self.assertEqual(words[GE_EXPRESSION].frequency, 2)
        self.assertEqual(words[GE_EXPRESSION].sentences[0].surface_form, 'げ')
