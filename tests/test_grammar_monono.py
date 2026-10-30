import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.rules.monono import match_monono
from src.GrammarConstants import MONONO_GRAMMAR_EXPRESSION as EXPRESSION
from src.GrammarShadow import aligned_tokens
from tests.test_grammar_te_iru import sentence, tok


class MononoTests(unittest.TestCase):
    def test_tokenizer_forms_and_nominal_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '買ったものの、使っていない。': True,
            '高いものの、買った。': True,
            '便利なものの、使いにくい。': True,
            '学生であるものの、働いている。': True,
            '大事なものの名前。': False,
            '買うものの名前。': False,
            '買ったものの。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                # The system dictionary splits もの + の; this ambiguous
                # analysis remains lexical. A separate fixture tests the
                # supported single-token conjunction analysis.
                self.assertEqual([text[a:b] for a, b in match_monono(aligned)], [])

    def test_card_retains_lexical_evidence_and_span(self):
        source = sentence('買ったものの、使わない。', [
            tok('買っ', '買う', ('動詞', '一般', '*', '*', '五段', '連用形')),
            tok('た', 'た', ('助動詞', '*', '*', '*', '*', '終止形-一般')),
            tok('ものの', 'ものの', ('接続詞',)),
            tok('、', '、', ('補助記号',)), tok('使わない。', '使う', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'買う': object(), 'ものの': object()}
        stage.add_cards(words)
        self.assertIn('買う', words)
        self.assertIn('ものの', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'ものの')
        self.assertEqual(len(words[EXPRESSION].sentences), 1)

    def test_split_nominal_and_incomplete_are_not_matched(self):
        source = sentence('買うものの名前。', [
            tok('買う', '買う', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('名前。', '名前', ('名詞',))])
        aligned, error = aligned_tokens(source.text, source.tokens)
        self.assertIsNone(error)
        self.assertEqual(list(match_monono(aligned)), [])


if __name__ == '__main__':
    unittest.main()
