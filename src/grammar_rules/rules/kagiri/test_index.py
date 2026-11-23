import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kagiri.index import RULE, match_kagiri
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class KagiriTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '生きている限り希望はある。': '限り',
            'できる限り頑張る。': None,  # Sudachi keeps the lexical adverb whole.
            '若いかぎり挑戦する。': 'かぎり',
            '命の限り戦う。': None,
            '怒ると破壊の限りを尽くすとても凶暴な魔物ザル。': None,
            '完全な姿を見ることがかなわず本当に残念な限りじゃよ。': None,
            'はい、82さんの言うとおり、力の限り適当に行うであります。': None,
            '残念な限り。': None,
            '嬉しい限りです。': None,
            '今日限りで閉店する。': None,
            '今回かぎり許す。': None,
            '知る限り本当だ。': '限り',
            '調べた限り問題はない。': '限り',
            '学生である限り勉強する。': '限り',
            '学生であった限り責任がある。': '限り',
            '元気な限り働く。': None,
            '行かない限り分からない。': None,
            '知る限りでは本当だ。': None,
            '嬉しい限りだ。': None,
            'できる限りのことをする。': None,
            '知る 限り分からない。': None,
            '限りがある。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kagiri(tokens)],
                                 [] if expected is None else [expected], tokens)
                if expected is None:
                    source = sentence(text, raw)
                    stage = GrammarStep(rules=(RULE,))
                    data = ExcludeGrammarTokensStep(stage).process(
                        stage.process(Artifact([source])))
                    self.assertFalse(any(t.get('grammar_reserved')
                                         for t in data.data[0].tokens))
                    words = {'限り': object()}
                    retained = words.copy()
                    stage.add_cards(words)
                    self.assertEqual(words, retained)

    def test_reservation_and_identity(self):
        source = sentence('生きる限り希望を持って進み続ける。', [
            tok('生きる', '生きる', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('限り', '限り', ('名詞',)), tok('希望を持って進み続ける。', '希望', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['限り'])
        words = {'生きる': object()}
        stage.add_cards(words)
        self.assertIn('生きる', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '限り')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
