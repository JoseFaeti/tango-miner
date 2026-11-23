import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kagiri.index import match_kagiri
from src.grammar_rules.rules.kagirideha.index import match_kagirideha
from src.grammar_rules.rules.naikagiri.index import RULE, match_naikagiri
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NaikagiriTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かない限り分からない。': 'ない限り',
            '雨が降らないかぎり行く。': 'ないかぎり',
            'お金がない限り買えない。': 'ない限り',
            '高くない限り買う。': 'ない限り',
            '静かではない限り眠れない。': 'ない限り',
            '知らない限りでは本当だ。': None,
            '終わらない限りの苦しみ。': None,
            '限りない夢。': None,
            '行かない 限り分からない。': None,
            '行く限り続く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naikagiri(tokens)],
                                 [] if expected is None else [expected], tokens)
                if expected:
                    self.assertEqual(list(match_kagiri(tokens)), [])
                    self.assertEqual(list(match_kagirideha(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('行かない限り真実は分からない。', [
            tok('行か', '行く', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*', '*', '*', '助動詞-ナイ', '連体形-一般')),
            tok('限り', '限り', ('名詞',)), tok('真実は分からない。', '真実', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', '限り'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ない限り')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
