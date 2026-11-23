import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omochimashite.index import RULE, match_omochimashite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OmochimashiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本日をもちまして終了します。': 'をもちまして',
                 '本日を持ちまして閉店いたします。': 'を持ちまして',
                 '誠意をもちまして対応いたします。': 'をもちまして',
                 '鞄を持ちまして出かけます。': 'を持ちまして',
                 '本日をもって終了します。': None,
                 '本日を もちまして終了します。': None,
                 '本日にもちまして終了します。': None,
                 '本を持ちました。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omochimashite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('本日をもちまして終了します。', [
            tok('本日', '本日', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('もち', 'もつ', ('動詞',)), tok('まし', 'ます', ('助動詞',)),
            tok('て', 'て', ('助詞',)), tok('終了します。', '終了', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'もち', 'まし', 'て'])
        words = {'本日': object()}
        stage.add_cards(words)
        self.assertIn('本日', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をもちまして')
        self.assertIn('〜をもって', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
