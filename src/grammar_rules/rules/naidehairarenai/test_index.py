import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naidehairarenai.index import RULE, match_naidehairarenai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaidehairarenaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '笑わないではいられない。': 'ないではいられない',
            '食べないではいられなかった。': 'ないではいられなかった',
            '笑わないではいられなくて。': 'ないではいられなく',
            '笑わないではいられなければ。': 'ないではいられなければ',
            'しないではいられません。': 'ないではいられません',
            '来ないでは居られない。': 'ないでは居られない',
            '言わないでいられない。': 'ないでいられない',
            '笑わないではいられた。': None,
            'ないではいられない。': None,
            '笑わないで はいられない。': None,
            '笑わないではいられなかっ。': None,
            '笑わないで帰った。': None,
            '危ないではいられない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naidehairarenai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_example_keep_verb(self):
        parts = [tok('笑わ', '笑う', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ない', 'ない', ('助動詞',)), tok('で', 'で', ('助詞', '接続助詞')),
                 tok('は', 'は', ('助詞',)), tok('い', 'いる', ('動詞',)),
                 tok('られ', 'られる', ('助動詞',)), tok('ない', 'ない', ('助動詞',))]
        source = sentence('笑わないではいられない', parts)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'で', 'は', 'い', 'られ', 'ない'])
        words = {'笑う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないではいられない')
        self.assertIn('笑う', words)
