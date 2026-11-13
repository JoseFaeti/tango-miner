import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dakedenaku.index import RULE, match_dakedenaku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DakedenakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本だけでなく雑誌も読む。': 'だけでなく',
                 '読むだけではなく書く。': 'だけではなく',
                 '安いだけじゃなく便利だ。': 'だけじゃなく',
                 '静かなだけでなく便利だ。': 'だけでなく',
                 '本だけではない。': None, '本だけ読む。': None,
                 '本だけで なく雑誌も読む。': None, '本 だけでなく雑誌も読む。': None,
                 'できるだけでなくなる。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dakedenaku(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity_with_competing_rules(self):
        source = sentence('本だけでなく雑誌も読む。', [
            tok('本', '本', ('名詞',)), tok('だけ', 'だけ', ('助詞', '副助詞')),
            tok('で', 'だ', ('助動詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('雑誌も読む。', '雑誌', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['だけ', 'で', 'なく'])
        words = {'本': object()}
        stage.add_cards(words)
        self.assertIn('本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'だけでなく')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
