import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.youdehanaika.index import RULE, match_youdehanaika
from src.GrammarConstants import YOUDEHANAIKA_EXPRESSION


class YoudehanaikaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行こうではないか。': 'ではないか',
            '食べようじゃないか。': 'じゃないか',
            '一緒にしようではありませんか。': 'ではありませんか',
            '行こうではないかと誘った。': 'ではないか',
            '彼が来るようではないか。': None,
            '雨が降るようでは困る。': None,
            'そうではないか。': None,
            '行こうではない。': None,
            '行こう ではないか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_youdehanaika(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_lexical_evidence(self):
        source = sentence('みんなで行こうではないか。', [
            tok('みんなで', 'みんな', ('名詞',)),
            tok('行こう', '行く', ('動詞', '*', '*', '*', '*', '意志推量形')),
            tok('で', 'だ', ('助動詞',)), tok('は', 'は', ('助詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('か', 'か', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'は', 'ない', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(RULE.expression, YOUDEHANAIKA_EXPRESSION)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ではないか')
