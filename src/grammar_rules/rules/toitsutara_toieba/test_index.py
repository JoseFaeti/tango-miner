import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toitsutara_toieba.index import RULE, match_toitsutara_toieba
from src.grammar_rules.rules.toitsutaranai.index import RULE as EXTREME_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToitsutaraToiebaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本といえば桜だ。': 'といえば',
            '夏と言えば海だ。': 'と言えば',
            '彼といったら、いつも遅刻だ。': 'といったら',
            'あの暑さと言ったらすごかった。': 'と言ったら',
            '嬉しいといったらない。': None,
            '嬉しいといったらなかった。': None,
            '友達と行ったら楽しかった。': None,
            '日本と いえば桜だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsutara_toieba(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_longer_rule(self):
        source = sentence('日本といえば桜がとても有名だ。', [
            tok('日本', '日本', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('いえ', 'いう', ('動詞',)), tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('桜がとても有名だ', '桜', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, EXTREME_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いえ', 'ば'])
        words = {'日本': object()}
        stage.add_cards(words)
        self.assertIn('日本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'といえば')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
