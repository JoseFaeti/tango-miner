import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zuni.index import RULE as SHORT_RULE
from src.grammar_rules.rules.zunihaokanai.index import RULE, match_zunihaokanai


class ZunihaokanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'その話は人を笑わせずにはおかない。': 'ずにはおかない',
            '彼を感動させずにはおかなかった。': 'ずにはおかなかった',
            '彼を驚かせずにはおかなくて。': 'ずにはおかなく',
            '彼を驚かせずにはおかなければ。': 'ずにはおかなければ',
            '彼を感動させずにはおきません。': 'ずにはおきません',
            '彼を感動させずには置かない。': 'ずには置かない',
            '笑わずにはおいた。': None, 'ずにはおかない。': None,
            '笑わずに はおかない。': None, '笑わずにはおかなかっ。': None,
            '本を置かない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zunihaokanai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_overlap_and_example_keep_verb(self):
        parts = [tok('笑わ', '笑う', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('に', 'に', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('おか', '置く', ('動詞',)),
                 tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))]
        source = sentence('笑わずにはおかない。', parts)
        stage = GrammarStep(rules=(RULE, SHORT_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'に', 'は', 'おか', 'ない'])
        words = {'笑う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずにはおかない')
        self.assertIn(SHORT_RULE.expression, words)
        self.assertIn('笑う', words)
