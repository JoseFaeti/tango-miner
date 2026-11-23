import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tekanawanai.index import RULE, match_tekanawanai
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TekanawanaiTests(unittest.TestCase):
    def test_observed_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '暑くてかなわない。': 'てかなわない',
            'うるさくてかなわなかった。': 'てかなわなかった',
            '迷惑でかなわない。': 'でかなわない',
            '暑くて敵わない。': 'て敵わない',
            '暑くてかないません。': 'てかないません',
            '暑くてかなわなくて困る。': 'てかなわなくて',
            '暑くてかなわなければ休む。': 'てかなわなければ',
            '騒いでかなわない。': 'でかなわない',
            '夢がかなわない。': None,
            '努力して夢が叶わない。': None,
            '条件に合って適わない。': None,
            '暑くてかなう。': None,
            '暑くて かなわない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tekanawanai(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_card_integration(self):
        adjective = tok('暑く', '暑い', ('形容詞', '一般', '*', '*', '形容詞', '連用形-一般'))
        te = tok('て', 'て', ('助詞', '接続助詞'))
        verb = tok('かなわ', 'かなう', ('動詞',))
        incomplete = [adjective, te, verb, tok('なかっ', 'ない', ('助動詞',))]
        aligned, error = aligned_tokens('暑くてかなわなかっ', incomplete)
        self.assertIsNone(error)
        self.assertEqual(list(match_tekanawanai(aligned)), [])
        source = sentence('暑くてかなわないので困る。', [adjective, te, verb,
                          tok('ない', 'ない', ('助動詞',)), tok('ので', 'ので', ('助詞',)),
                          tok('困る', '困る', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'かなわ', 'ない'])
        words = {'暑い': object()}
        stage.add_cards(words)
        self.assertIn('暑い', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てかなわない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)


if __name__ == '__main__':
    unittest.main()
