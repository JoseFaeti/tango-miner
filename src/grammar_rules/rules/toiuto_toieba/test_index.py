import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.toiuto_toieba.index import RULE, match_toiuto_toieba
from src.grammar_rules.rules.toitsutara_toieba.index import RULE as EXISTING_RULE
from src.grammar_rules.rules.karayuuto_karaieba.index import RULE as STANDPOINT_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToiutoToiebaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本というと桜を思い出す。': 'というと',
            '夏と言うと海だ。': 'と言うと',
            '日本といえば桜だ。': None,
            '価格から言うと安い。': None,
            '友達と行くと楽しい。': None,
            '日本と いうと桜だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiuto_toieba(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_existing_card(self):
        source = sentence('日本というと桜がとても有名だ。', [
            tok('日本', '日本', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('いう', '言う', ('動詞',)), tok('と', 'と', ('助詞', '接続助詞')),
            tok('桜がとても有名だ', '桜', ('名詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, EXISTING_RULE, STANDPOINT_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いう', 'と'])
        words = {'日本': object()}
        stage.add_cards(words)
        self.assertIn('日本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'というと')
        self.assertNotIn(EXISTING_RULE.expression, words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
