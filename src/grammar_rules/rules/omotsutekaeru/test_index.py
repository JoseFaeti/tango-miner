import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.omotsutekaeru.index import RULE, match_omotsutekaeru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class OmotsutekaeruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '書面をもって挨拶に代える。': 'をもって挨拶に代える',
            'お礼をもってご挨拶に代えます。': 'をもってご挨拶に代えます',
            '感謝を以て挨拶に代えた。': 'を以て挨拶に代えた',
            '書面をもちまして挨拶に代えます。': 'をもちまして挨拶に代えます',
            '言葉をもって代えさせていただきます。': 'をもって代えさせて',
            '書面をもって挨拶に代えて終わる。': 'をもって挨拶に代えて',
            '書面をもって終了する。': None,
            '書面をもって挨拶を代える。': None,
            '書面をもって挨拶に変える。': None,
            '書面をもって 挨拶に代える。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omotsutekaeru(tokens)],
                                 [] if expected is None else [expected])

    def test_card_identity_overlap_and_lexical_preservation(self):
        source = sentence('書面をもって挨拶に代える。', [
            tok('書面', '書面', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('もっ', 'もつ', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('挨拶', '挨拶', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('代える', '代える', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        by_surface = {t['surface']: t for t in data.data[0].tokens}
        self.assertFalse(by_surface['挨拶'].get('grammar_reserved', False))
        self.assertFalse(by_surface['代える'].get('grammar_reserved', False))
        words = {'挨拶': object()}
        stage.add_cards(words)
        self.assertIn('挨拶', words)
        self.assertIn('〜をもって', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をもって挨拶に代える')
