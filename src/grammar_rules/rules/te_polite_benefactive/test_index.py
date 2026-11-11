import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import TE_POLITE_BENEFACTIVE_EXPRESSION
from src.TokenizedSentence import TokenizedSentence
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_polite_benefactive.index import match_te_polite_benefactive


class PoliteBenefactiveTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読んでいただく。': 'でいただく', '本を読んで頂いた。': 'で頂いた',
            '本を読んでいただいて。': 'でいただいて', '本を読んでいただきます。': 'でいただきます', '本を読んでいただかない。': 'でいただかない',
            '本を読んでくださる。': 'でくださる', '本を読んで下さった。': 'で下さった',
            '本を読んでくださいます。': 'でくださいます', '本を読んでくださらない。': 'でくださらない',
            '本を読んで差し上げる。': 'で差し上げる', '本を読んでさしあげました。': 'でさしあげました',
            '本を読んで差し上げて。': 'で差し上げて', '本を見ていただく。': 'ていただく',
            '本をいただく。': None, '本を差し上げる。': None, '本を読んでください。': None,
            '本を読んでいただけませんか。': None, '本を読んでくれる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [{'surface': m.surface(), 'lemma': m.dictionary_form(), 'pos': m.part_of_speech()}
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_polite_benefactive(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        def token(surface, lemma, pos):
            return {'surface': surface, 'lemma': lemma, 'pos': pos}
        source = TokenizedSentence('本を読んでくださった。', 'source', Path('source.txt'), [
            token('本を', '本', ('名詞',)),
            token('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            token('で', 'で', ('助詞', '接続助詞')),
            token('くださっ', 'くださる', ('動詞', '非自立可能', '*', '*', '五段', '連用形')),
            token('た', 'た', ('助動詞',)), token('。', '。', ('補助記号',))])
        collection = GrammarStep()
        artifact = Artifact([source])
        collection.process(artifact)
        collection.add_cards(words := {})
        self.assertEqual(words[TE_POLITE_BENEFACTIVE_EXPRESSION].sentences[0].surface_form, 'でくださった')
        ExcludeGrammarTokensStep(collection).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(artifact.data[0].tokens[3].get('grammar_reserved', False))


if __name__ == '__main__':
    unittest.main()
