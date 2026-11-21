import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.karatoitsute.index import RULE, match_karatoitsute
from src.grammar_rules.rules.kara_node.index import RULE as REASON
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaratoitsuteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '安いからといって買わない。': 'からといって',
            '学生だからと言って暇ではない。': 'からと言って',
            '雨が降ったからといって中止しない。': 'からといって',
            '駅からと言っていた。': None,
            '彼からと言って本を渡した。': None,
            '食べてからといっていた。': None,
            '安いから といって買わない。': None,
            '安いからと言った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karatoitsute(aligned)],
                                 [expected] if expected else [])

    def test_identity_reservation_and_reason_overlap(self):
        source = sentence('安いからといって買わない。', [
            tok('安い', '安い', ('形容詞', '一般', '*', '*', '形容詞', '終止形-一般')),
            tok('から', 'から', ('助詞', '接続助詞')),
            tok('と', 'と', ('助詞', '格助詞')),
            tok('いっ', 'いう', ('動詞', '一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('買わない。', '買う', ('動詞', '一般'))])
        stage = GrammarStep(rules=(RULE, REASON))
        artifact = stage.process(Artifact([source]))
        words = {'安い': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'からといって')
        self.assertNotIn(REASON.expression, words)
        self.assertIn('安い', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['から', 'と', 'いっ', 'て'])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
