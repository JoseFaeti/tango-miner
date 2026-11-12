import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.you_ni.index import RULE as YOU_NI
from src.grammar_rules.rules.you_ni_iu_tanomu.index import RULE, match_you_ni_iu_tanomu


class YouNiIuTanomuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '早く来るように言う。': 'ように言う',
            '遅れないように言った。': 'ように言った',
            '早く来るように言いました。': 'ように言いました',
            '早く来るように頼んだ。': 'ように頼んだ',
            '忘れないように頼みました。': 'ように頼みました',
            '早く来るように頼んでいる。': 'ように頼んで',
            '早く来るように言わない。': 'ように言わない',
            '早く来るように頼みませんでした。': 'ように頼みませんでした',
            '鳥のように言った。': None,
            '静かなように言った。': None,
            '来るように考えた。': None,
            '来るように 言った。': None,
            '来るように言っ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_you_ni_iu_tanomu(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_overlap(self):
        source = sentence('早く来るように言ったよ。', [
            tok('早く', '早い', ('形容詞',)),
            tok('来る', '来る', ('動詞', '一般', '*', '*', 'カ行変格', '連体形-一般')),
            tok('よう', 'よう', ('形状詞',)), tok('に', 'だ', ('助動詞',)),
            tok('言っ', '言う', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('よ。', 'よ', ('助詞',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE, YOU_NI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[YOU_NI.id].count, 0)
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['よう', 'に', '言っ', 'た'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ように言った')
