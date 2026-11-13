import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.hamochiron.index import RULE, match_hamochiron
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HamochironTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本語はもちろん英語も話せる。': 'はもちろん',
            '子供は勿論大人も楽しめる。': 'は勿論',
            '読むことはもちろん書くこともできる。': 'はもちろん',
            'もちろん行きます。': None,
            '彼は、もちろん行く。': None,
            '彼は もちろん行く。': None,
            'それは当然だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hamochiron(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_topic_overlap(self):
        source = sentence('日本語はもちろん英語も話せる。', [
            tok('日本語', '日本語', ('名詞',)), tok('は', 'は', ('助詞', '係助詞')),
            tok('もちろん', '勿論', ('副詞',)), tok('英語も話せる。', '英語', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['は', 'もちろん'])
        words = {'日本語': object()}
        stage.add_cards(words)
        self.assertIn('日本語', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'はもちろん')
        self.assertIn('〜は', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
