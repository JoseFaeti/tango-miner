import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.hazuganai.index import RULE
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HazuganaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼が来るはずがない。': 'はずがない',
            '彼が来たはずがなかった。': 'はずがなかった',
            '寒いはずがなくて驚いた。': 'はずがなくて',
            '静かなはずがなければ困る。': 'はずがなければ',
            '行かないはずがない。': 'はずがない',
            '来るはずがないです。': 'はずがない',
            '矢のはずがない。': None,
            '学生のはずがない。': None,
            '来るはずだ。': None,
            '来るはず がない。': None,
            '来るはずがなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼が来るはずがない。', [
            tok('彼が', '彼', ('代名詞',)),
            tok('来る', '来る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('はず', 'はず', ('名詞', '普通名詞', '一般')),
            tok('が', 'が', ('助詞', '格助詞')),
            tok('ない', 'ない', ('形容詞', '非自立可能')),
            tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE, NAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['はず', 'が', 'ない'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'来る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'はずがない')
        self.assertIn('来る', words)
