import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dokoroka.index import RULE, match_dokoroka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DokorokaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨どころか雪が降った。': 'どころか',
            '読むどころか書くこともできない。': 'どころか',
            '静かどころかうるさい。': 'どころか',
            '寝るどころではない。': None,
            '所かどうか。': None,
            'どころか、逆だ。': None,
            '雨、どころか雪だ。': None,
            '雨どころ か雪だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dokoroka(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_evidence(self):
        source = sentence('雨どころか雪まで降ってきました。', [
            tok('雨', '雨', ('名詞',)), tok('どころ', 'どころ', ('助詞', '副助詞')),
            tok('か', 'か', ('助詞', '副助詞')), tok('雪まで降ってきました。', '雪', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['どころ', 'か'])
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'どころか')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
