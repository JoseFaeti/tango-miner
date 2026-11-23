import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.katoomouto_katoomottara.index import RULE as KA_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toomottara.index import EXPRESSION, RULE, match_toomottara


class ToomottaraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '終わったと思ったら、まだ続いていた。': 'と思ったら',
            '猫だとおもったら犬だった。': 'とおもったら',
            '何かと思ったら猫だった。': 'と思ったら',
            '帰ったかと思ったら雨が降った。': None,
            '帰ると思った。': None,
            '帰ると、思ったら違った。': None,
            '本を持ったら帰る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toomottara(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_attached_word(self):
        source = sentence('猫だと思ったら犬だった。', [
            tok('猫', '猫', ('名詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('と', 'と', ('助詞', '格助詞')), tok('思っ', '思う', ('動詞',)),
            tok('たら', 'た', ('助動詞',)), tok('犬だった。', '犬', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '思っ', 'たら'])
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertIn('猫', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'と思ったら')

    def test_related_rule_owns_past_ka_sequence(self):
        source = sentence('帰ったかと思ったら雨だ。', [
            tok('帰っ', '帰る', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('か', 'か', ('助詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('思っ', '思う', ('動詞',)), tok('たら', 'た', ('助動詞',)),
            tok('雨だ。', '雨', ('名詞',))])
        stage = GrammarStep(rules=(RULE, KA_RULE))
        stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertEqual(stage.occurrences[KA_RULE.id].count, 1)
