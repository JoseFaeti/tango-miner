import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import MASENKA_NAIKA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.masenka_naika.index import match_masenka_naika
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MasenkaNaikaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '一緒に行きませんか。': 'ませんか', '一緒に行かないか。': 'ないか',
            '食べませんか？': 'ませんか', '来ないか。': 'ないか',
            '行かないかな。': None, '行かないか確認する。': None,
            '時間がないか。': None, '行きません。': None, '行かない。': None,
            '猫じゃないか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_masenka_naika(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_token_boundaries(self):
        verb = tok('行か', '行く', ('動詞', '一般', '*', '*', '五段', '未然形-一般'))
        negative = tok('ない', 'ない', ('助動詞',))
        question = tok('か', 'か', ('助詞', '終助詞'))
        for text, expected in [('行かないか', ['ないか']), ('行か ないか', []),
                               ('行かない か', [])]:
            tokens, error = aligned_tokens(text, [verb, negative, question])
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_masenka_naika(tokens)], expected)
        source = sentence('明日一緒に公園へ行かないか。', [
            tok('明日一緒に公園へ', '公園', ('名詞',)), verb, negative, question,
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[MASENKA_NAIKA_EXPRESSION].sentences[0].surface_form, 'ないか')
        artifact = Artifact([source])
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(artifact.data[0].tokens[2]['grammar_reserved'])
        self.assertTrue(artifact.data[0].tokens[3]['grammar_reserved'])
