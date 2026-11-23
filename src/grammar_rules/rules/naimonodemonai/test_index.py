import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naimonodemonai.index import RULE, match_naimonodemonai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaimonodemonaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行けないものでもない。': 'ないものでもない',
            '食べられないものでもなかった。': 'ないものでもなかった',
            '安くないものでもありません。': 'ないものでもありません',
            '行けない物でもない。': None,
            '行けないものではない。': None,
            '行けないものでもある。': None,
            '行けないものでも ない。': None,
            '行けない、ものでもない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naimonodemonai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('行けないものでもない。', [
            tok('行け', '行ける', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('もの', 'もの', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('も', 'も', ('助詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'もの', 'で', 'も', 'ない'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないものでもない')
        from src.grammar_rules.registry import RULES
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
