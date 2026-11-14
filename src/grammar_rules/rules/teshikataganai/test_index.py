import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.teshikataganai.index import RULE, match_teshikataganai


class TeshikataganaiTests(unittest.TestCase):
    def test_observed_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '会いたくて仕方がない。': 'て仕方がない',
            '嬉しくてしかたない。': 'てしかたない',
            '心配でしょうがない。': 'でしょうがない',
            '眠くてしょうがなかった。': 'てしょうがなかった',
            '暑くて仕方ありません。': 'て仕方ありません',
            '悲しくてしょうがなくて泣いた。': 'てしょうがなくて',
            '嬉しくて仕様がない。': 'て仕様がない',
            '嬉しくてしようがない。': 'てしようがない',
            '仕方がない。': None, '嬉しくて 仕方がない。': None,
            '作り方がない。': None, '嬉しくて仕方がある。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_teshikataganai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_incomplete_suffix(self):
        adjective = tok('嬉しく', '嬉しい', ('形容詞', '一般', '*', '*', '形容詞', '連用形-一般'))
        te = tok('て', 'て', ('助詞', '接続助詞'))
        noun = tok('仕方', '仕方', ('名詞',))
        ga = tok('が', 'が', ('助詞',))
        incomplete = [adjective, te, noun, ga, tok('なかっ', 'ない', ('形容詞',))]
        aligned, error = aligned_tokens('嬉しくて仕方がなかっ', incomplete)
        self.assertIsNone(error)
        self.assertEqual(list(match_teshikataganai(aligned)), [])
        source = sentence('嬉しくて仕方がない。', [adjective, te, noun, ga,
                          tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['て', '仕方', 'が', 'ない'])
        words = {'嬉しい': object()}
        stage.add_cards(words)
        self.assertIn('嬉しい', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'て仕方がない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
