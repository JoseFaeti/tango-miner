import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.to_omou.index import match_to_omou
from src.grammar_rules.rules.you_to_omou.index import RULE, match_you_to_omou


class YouToOmouTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'読もうと思う。': 'と思う', '食べようと思った。': 'と思った',
                 '来ようと思います。': 'と思います', '勉強しようと思わない。': 'と思わない',
                 '帰ろうと思っている。': 'と思って', '行くと思う。': None,
                 '鳥のようと思う。': None, '読もうとする。': None,
                 '読もうと 思う。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_you_to_omou(aligned)],
                                 [] if expected is None else [expected], raw)
                if expected is not None:
                    self.assertEqual(list(match_to_omou(aligned)), [])

    def test_reservation_and_identity(self):
        source = sentence('本を読もうと思った。', [
            tok('本を', '本', ('名詞',)),
            tok('読もう', '読む', ('動詞', '一般', '*', '*', '五段', '意志推量形')),
            tok('と', 'と', ('助詞',)), tok('思っ', '思う', ('動詞',)),
            tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '思っ', 'た'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と思った')
