import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naidehaokanai.index import RULE, match_naidehaokanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaidehaokanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'その話は人を笑わせないではおかない。': 'ないではおかない',
            '彼を感動させないではおかなかった。': 'ないではおかなかった',
            '彼を驚かせないではおかなくて。': 'ないではおかなく',
            '彼を驚かせないではおかなければ。': 'ないではおかなければ',
            '彼を感動させないではおきません。': 'ないではおきません',
            '彼を感動させないでは置かない。': 'ないでは置かない',
            '彼を感動させないではおいた。': None,
            'ないではおかない。': None,
            '笑わないで はおかない。': None,
            '笑わないではおかなかっ。': None,
            '危ないではおかない。': None,
            '本を置かない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naidehaokanai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_example_keep_verb(self):
        parts = [tok('笑わ', '笑う', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ない', 'ない', ('助動詞',)), tok('で', 'で', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('おか', '置く', ('動詞',)),
                 tok('ない', 'ない', ('助動詞',))]
        source = sentence('笑わないではおかない', parts)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'で', 'は', 'おか', 'ない'])
        words = {'笑う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないではおかない')
        self.assertIn('笑う', words)
