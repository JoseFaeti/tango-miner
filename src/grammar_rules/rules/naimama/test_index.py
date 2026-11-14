import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.naimama.index import RULE, match_naimama
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaimamaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '何も食べないまま出かけた。': ['ないまま'],
            '読まれないまま終わった。': ['ないまま'],
            '高くないままだ。': ['ないまま'],
            '時間がないまま終わった。': ['ないまま'],
            '食べない儘寝た。': ['ない儘'],
            '窓を開けたまま寝た。': [],
            '行かぬまま終わった。': [],
            '危ないまま進む。': [],
            '食べない まま寝た。': [],
            'そのまま帰る。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naimama(tokens)], expected, raw)

    def test_reservation_and_overlapping_cards(self):
        source = sentence('何も食べないまま出かけた。', [
            tok('何も', '何', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞',)), tok('まま', 'まま', ('名詞', '普通名詞')),
            tok('出かけた。', '出かける', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'まま'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないまま')
        self.assertIn('〜まま', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
