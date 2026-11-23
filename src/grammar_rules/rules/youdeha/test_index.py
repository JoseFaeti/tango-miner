import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.youdeha.index import RULE, match_youdeha


class YoudehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'こんなに雨が降るようでは試合はできない。': 'ようでは',
            '毎日遅刻するようじゃ困る。': 'ようじゃ',
            '仕事が忙しいようでは休めない。': 'ようでは',
            'そんなに元気なようでは心配だ。': 'ようでは',
            '雨が降るようではない。': None,
            '雨が降るようではありません。': None,
            '雨が降るようで困る。': None,
            '用では困る。': None,
            '雨が降るようでは。': None,
            '雨が降るよう では困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_youdeha(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_component_evidence(self):
        source = sentence('雨が降るようでは困る。', [
            tok('雨が', '雨', ('名詞',)),
            tok('降る', '降る', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('よう', 'よう', ('形状詞', '助動詞語幹')),
            tok('で', 'だ', ('助動詞',)), tok('は', 'は', ('助詞',)),
            tok('困る。', '困る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'降る': object()}
        stage.add_cards(words)
        self.assertIn('降る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ようでは')
