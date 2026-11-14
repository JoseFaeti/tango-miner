import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.zunihairarenai.index import RULE, match_zunihairarenai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ZunihairarenaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '笑わずにはいられない。': 'ずにはいられない',
            '食べずにはいられなかった。': 'ずにはいられなかった',
            '笑わずにはいられなくて。': 'ずにはいられなく',
            '笑わずにはいられなければ。': 'ずにはいられなければ',
            'せずにはいられません。': 'ずにはいられません',
            '来ずには居られない。': 'ずには居られない',
            '言わずにいられない。': 'ずにいられない',
            '笑わずにはいられた。': None,
            'ずにはいられない。': None,
            '笑わず にはいられない。': None,
            '笑わずにはいられなかっ。': None,
            '笑わずに帰った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zunihairarenai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_example_keep_verb(self):
        parts = [tok('笑わ', '笑う', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('に', 'に', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('い', 'いる', ('動詞',)),
                 tok('られ', 'られる', ('助動詞',)), tok('ない', 'ない', ('助動詞',))]
        source = sentence('笑わずにはいられない', parts)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'に', 'は', 'い', 'られ', 'ない'])
        words = {'笑う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずにはいられない')
        self.assertIn('笑う', words)
