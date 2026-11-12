import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import KOTOKA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kotoka.index import match_kotoka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotokaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'どんなに嬉しいことか。': 'ことか',
            '何度後悔したことか。': 'ことか',
            'どれほど大変なことか。': 'ことか',
            'どれだけ待ったことか': 'ことか',
            'なんと美しい事か！': '事か',
            'どんなことか知っている。': None,
            '彼が来ることかと思った。': None,
            '大切なことだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kotoka(tokens)],
                                 [] if expected is None else [expected])

    def test_card_identity_and_source(self):
        source = sentence('あなたに会えてどんなに嬉しいことか。', [
            tok('あなたに会えてどんなに', 'あなた', ('名詞',)),
            tok('嬉しい', '嬉しい', ('形容詞', '一般', '*', '*', '形容詞', '連体形-一般')),
            tok('こと', 'こと', ('名詞', '普通名詞')),
            tok('か', 'か', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'嬉しい': object()}
        stage.add_cards(words)
        self.assertIn('嬉しい', words)
        self.assertEqual(words[KOTOKA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KOTOKA_EXPRESSION].sentences[0].surface_form, 'ことか')
