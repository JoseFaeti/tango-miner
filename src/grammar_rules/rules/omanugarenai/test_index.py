import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omanugarenai.index import RULE
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OmanugarenaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '責任を免れない。': 'を免れない',
            '批判を免れなかった。': 'を免れなかった',
            '破滅を免れなくて困った。': 'を免れなく',
            '批判を免れなければならない。': 'を免れなければ',
            '責任をまぬがれない。': 'をまぬがれない',
            '責任を免れません。': 'を免れません',
            'を免れない。': None,
            '責任を免れる。': None,
            '責任を免れた。': None,
            '責任を免れなかっ。': None,
            '責任を取らない。': None,
            '責任を 免れない。': None,
            '責任は免れない。': None,
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
        source = sentence('彼は責任を免れないだろう。', [
            tok('彼は', '彼', ('代名詞',)),
            tok('責任', '責任', ('名詞', '普通名詞')),
            tok('を', 'を', ('助詞', '格助詞')),
            tok('免れ', '免れる', ('動詞', '一般', '*', '*', '下一段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞',)), tok('だろう。', 'だ', ('助動詞',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE, NAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '免れ', 'ない'])
        words = {'責任': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を免れない')
        self.assertIn(NAI.expression, words)
        self.assertIn('責任', words)
