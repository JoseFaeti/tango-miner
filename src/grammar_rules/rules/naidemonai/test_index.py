import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naidemonai.index import RULE, match_naidemonai
from src.grammar_rules.rules.nai.index import RULE as NAI_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaidemonaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行けないでもない。': 'ないでもない',
            '食べられないでもなかった。': 'ないでもなかった',
            '安くないでもありません。': 'ないでもありません',
            '元気でないでもありませんでした。': 'ないでもありませんでした',
            'お金がないでもない。': 'ないでもない',
            '行かないでください。': None,
            '行かないでもいい。': None,
            '行かないでもある。': None,
            '行かないでも ない。': None,
            '行かない、でもない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naidemonai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlapping_negative(self):
        source = sentence('そこへ行けないでもない。', [
            tok('そこ', 'そこ', ('代名詞',)), tok('へ', 'へ', ('助詞',)),
            tok('行け', '行ける', ('動詞', '非自立可能', '*', '*', '下一段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*', '*', '*', '助動詞-ナイ', '終止形-一般')),
            tok('で', 'で', ('助詞', '接続助詞')), tok('も', 'も', ('助詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, NAI_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'で', 'も', 'ない'])
        self.assertFalse(result.data[0].tokens[2].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないでもない')
        self.assertEqual(words[NAI_RULE.expression].frequency, 1)
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        from src.grammar_rules.registry import RULES
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
