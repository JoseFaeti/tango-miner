import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.you_ni.index import RULE as YOU_NI
from src.grammar_rules.rules.you_ni_mieru.index import RULE, match_you_ni_mieru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class YouNiMieruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は疲れているように見える。': 'ように見える',
            '彼は若いように見えた。': 'ように見えた',
            '彼は元気なように見えます。': 'ように見えます',
            '彼は医者のように見えました。': 'ように見えました',
            '彼は疲れているように見えない。': 'ように見えない',
            '彼は疲れているように見えなかった。': 'ように見えなかった',
            '彼は疲れているように見えません。': 'ように見えません',
            '彼は疲れているように見えませんでした。': 'ように見えませんでした',
            '彼は疲れているように見えている。': 'ように見えて',
            '彼は疲れているように思える。': None,
            '彼は疲れているように 見える。': None,
            '彼は疲れているように見え。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_you_ni_mieru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼はとても若いように見えるよ。', [
            tok('彼はとても', '彼', ('名詞',)),
            tok('若い', '若い', ('形容詞', '一般', '*', '*', '形容詞', '連体形-一般')),
            tok('よう', 'よう', ('形状詞',)), tok('に', 'だ', ('助動詞',)),
            tok('見える', '見える', ('動詞',)), tok('よ。', 'よ', ('助詞',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE, YOU_NI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[YOU_NI.id].count, 0)
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['よう', 'に', '見える'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ように見える')
