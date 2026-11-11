import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kara_made.index import RULE, match_kara_made
from src.grammar_rules.rules.kara_node.index import RULE as REASON
from src.grammar_rules.rules.madeni.index import RULE as DEADLINE
from src.grammar_rules.rules.te_kara.index import RULE as AFTER
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaraMadeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '駅から学校まで歩く。': ['から', 'まで'],
            '明日から働く。': ['から'], '夜まで待つ。': ['まで'],
            '帰るまで待つ。': ['まで'], '雨が降るから帰る。': [],
            '食べてから行く。': [], '明日までに帰る。': [],
            'ここまで美しい。': [], 'それほどまで驚く。': [],
            '体が空っぽだ。': [], '駅 から歩く。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kara_made(tokens)], expected)

    def test_reservation_examples_and_identity(self):
        source = sentence('駅から学校まで歩く。', [
            tok('駅', '駅', ('名詞',)), tok('から', 'から', ('助詞', '格助詞')),
            tok('学校', '学校', ('名詞',)), tok('まで', 'まで', ('助詞', '副助詞')),
            tok('歩く', '歩く', ('動詞',)), tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['から', 'まで'])
        words = {'駅': object(), '学校': object()}
        stage.add_cards(words)
        self.assertIn('駅', words)
        self.assertIn('学校', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'から')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_existing_rules_own_their_markers(self):
        cases = [
            ('雨が降るから帰る。', [('雨', '雨', ('名詞',)), ('が', 'が', ('助詞', '格助詞')),
             ('降る', '降る', ('動詞', '一般', '*', '*', '*', '終止形-一般')),
             ('から', 'から', ('助詞', '接続助詞')), ('帰る。', '帰る', ('動詞',))], REASON),
            ('食べてから行く。', [('食べ', '食べる', ('動詞', '一般', '*', '*', '*', '連用形-一般')), ('て', 'て', ('助詞', '接続助詞')),
             ('から', 'から', ('助詞', '格助詞')), ('行く。', '行く', ('動詞',))], AFTER),
            ('明日までに帰る。', [('明日', '明日', ('名詞',)), ('まで', 'まで', ('助詞', '副助詞')),
             ('に', 'に', ('助詞', '格助詞')), ('帰る。', '帰る', ('動詞',))], DEADLINE),
        ]
        for text, parts, owner in cases:
            with self.subTest(text=text):
                stage = GrammarStep(rules=(RULE, REASON, AFTER, DEADLINE))
                stage.process(Artifact([sentence(text, [tok(*part) for part in parts])]))
                self.assertEqual(stage.occurrences[RULE.id].count, 0)
                self.assertEqual(stage.occurrences[owner.id].count, 1)
