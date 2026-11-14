import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kimi.index import KIMI_EXPRESSION, match_kimi
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KimiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'風邪気味です': '気味', '疲れ気味だ': '気味',
                 '太り気味': '気味', '押され気味': '気味',
                 '寝不足気味': '気味', '気味が悪い': None,
                 '不気味な顔': None, '君が好き': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kimi(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        host = tok('風邪', '風邪', ('名詞', '普通名詞'))
        suffix = tok('気味', '気味', ('名詞', '普通名詞'))
        for text, raw in [
            ('風邪 気味', [host, suffix]),
            ('風邪気味', [host, tok('気味', '気味', ('動詞',))]),
            ('気味', [suffix]),
            ('食べる気味', [tok('食べる', '食べる', ('動詞', '一般', '*', '*', '下一段', '終止形')), suffix]),
        ]:
            with self.subTest(text=text):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_kimi(tokens)), [])
        source = sentence('今日は少し風邪気味です。', [
            tok('今日は少し', '今日', ('名詞',)), host, suffix,
            tok('です。', 'です', ('助動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'風邪': object()}
        stage.add_cards(words)
        self.assertIn('風邪', words)
        self.assertEqual(words[KIMI_EXPRESSION].frequency, 2)
        self.assertEqual(words[KIMI_EXPRESSION].sentences[0].surface_form, '気味')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source, source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(artifact.data[0].tokens[2]['grammar_reserved'])
