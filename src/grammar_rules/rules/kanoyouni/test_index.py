import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kanoyouni.index import RULE, match_kanoyouni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KanoyouniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は全て知っているかのように話す。': True,
            '夢を見たかのように笑った。': True,
            '嬉しいかのように振る舞う。': True,
            '彼は知っているかのようだ。': False,
            '彼は知っているかのような顔だ。': False,
            '先生が言ったように書く。': False,
            '鹿のように走る。': False,
            '彼は知っているかのよう に話す。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kanoyouni(aligned)],
                                 ['かのように'] if expected else [])

    def test_reservation_example_and_registry(self):
        source = sentence('彼は知っているかのように話す。', [
            tok('彼は', '彼', ('名詞',)),
            tok('知っている', 'いる', ('動詞', '一般', '*', '*', '*', '連体形-一般')),
            tok('か', 'か', ('助詞', '副助詞')),
            tok('の', 'の', ('助詞', '格助詞')),
            tok('よう', 'よう', ('形状詞', '助動詞語幹')),
            tok('に', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '連用形-ニ')),
            tok('話す', '話す', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=RULES)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['か', 'の', 'よう', 'に'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'知る': object(), '話す': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'かのように')
        self.assertIn('知る', words)
        self.assertIn('話す', words)

