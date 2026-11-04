import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import TE_BENEFACTIVE_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.TokenizedSentence import TokenizedSentence
from src.grammar_rules.rules.te_benefactive.index import match_te_benefactive


class BenefactiveTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読んであげる。': 'であげる', '本を読んであげた。': 'であげた',
            '本を読んであげます。': 'であげます', '本を読んであげない。': 'であげない',
            '本を読んでくれる。': 'でくれる', '本を読んでくれた。': 'でくれた',
            '本を読んでくれました。': 'でくれました', '本を読んでくれない。': 'でくれない',
            '本を読んでもらう。': 'でもらう', '本を読んでもらった。': 'でもらった',
            '本を読んでもらいます。': 'でもらいます', '本を読んでもらわない。': 'でもらわない',
            '本を見てくれる。': 'てくれる', '本をもらう。': None,
            '手を上げる。': None, '本を読んでくれ。': None, '本を読んでいる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [{'surface': m.surface(), 'lemma': m.dictionary_form(), 'pos': m.part_of_speech()}
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_benefactive(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation_keep_attached_verb(self):
        def token(surface, lemma, pos):
            return {'surface': surface, 'lemma': lemma, 'pos': pos}
        source = TokenizedSentence('今日は本を読んでくれたよ。', 'source', Path('source.txt'), [
            token('今日は本を', '本', ('名詞',)),
            token('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            token('で', 'で', ('助詞', '接続助詞')),
            token('くれ', 'くれる', ('動詞', '非自立可能', '*', '*', '下一段', '連用形')),
            token('た', 'た', ('助動詞',)), token('よ。', 'よ', ('助詞',))])
        collection = GrammarStep()
        artifact = Artifact([source])
        collection.process(artifact)
        collection.add_cards(words := {})
        self.assertEqual(words[TE_BENEFACTIVE_EXPRESSION].sentences[0].surface_form, 'でくれた')
        ExcludeGrammarTokensStep(collection).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(artifact.data[0].tokens[3].get('grammar_reserved', False))


if __name__ == '__main__':
    unittest.main()
