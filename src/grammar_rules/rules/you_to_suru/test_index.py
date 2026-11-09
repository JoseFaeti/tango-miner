import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import tok
from src.grammar_rules.rules.you_to_suru.index import match_you_to_suru
from src.grammar_rules.rules.you_to_suru.index import RULE
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.Artifact import Artifact
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class YouToSuruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読もうとする。': 'とする', '家を出ようとした。': 'とした',
            '帰ろうとしています。': 'として', '食べようとしない。': 'としない',
            '来ようとしました。': 'としました', '勉強しようとする。': 'とする',
            '雨が降ろうとしている。': 'として',
            '本を読むとする。': None, '鳥のようとする。': None,
            '食べようと思う。': None, '食べようと する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_you_to_suru(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_and_identity(self):
        source = sentence('私は面白い本を読もうとした。', [
            tok('私は面白い本を', '本', ('名詞',)),
            tok('読もう', '読む', ('動詞', '一般', '*', '*', '五段', '意志推量形')),
            tok('と', 'と', ('助詞',)), tok('し', 'する', ('動詞',)),
            tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'し', 'た'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とした')

    def test_incomplete_and_nonadjacent_forms(self):
        verb = tok('読もう', '読む', ('動詞', '一般', '*', '*', '五段', '意志推量形'))
        for parts in ([verb, tok('と', 'と', ('助詞',)), tok('し', 'する', ('動詞',))],
                      [verb, tok('と', 'と', ('助詞',)), tok('し', 'する', ('動詞',)),
                       tok('まし', 'ます', ('助動詞',))]):
            text = ''.join(t['surface'] for t in parts)
            tokens, error = aligned_tokens(text, parts)
            self.assertIsNone(error)
            self.assertEqual(list(match_you_to_suru(tokens)), [])
