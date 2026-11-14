import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.numama.index import RULE, match_numama
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NumamaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かぬまま終わった。': ['ぬまま'],
            '食べぬまま寝た。': ['ぬまま'],
            '読まれぬまま終わった。': ['ぬまま'],
            'せぬまま帰った。': ['ぬまま'],
            '来ぬまま終わった。': ['ぬまま'],
            '行かぬ儘終わった。': ['ぬ儘'],
            '死ぬまま終わった。': [],
            '行かんまま終わった。': [],
            '行かないまま終わった。': [],
            '行かぬ まま終わった。': [],
            'そのまま帰る。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_numama(tokens)], expected, raw)

    def test_reservation_and_overlapping_cards(self):
        source = sentence('本を読まぬまま帰った。', [
            tok('本を', '本', ('名詞',)),
            tok('読ま', '読む', ('動詞', '*', '*', '*', '五段-マ行', '未然形-一般')),
            tok('ぬ', 'ぬ', ('助動詞',)), tok('まま', 'まま', ('名詞', '普通名詞')),
            tok('帰った。', '帰る', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ぬ', 'まま'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ぬまま')
        self.assertIn('〜まま', words)
        self.assertIn('〜ぬ / 〜ん', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
