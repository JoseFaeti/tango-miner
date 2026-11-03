import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kamoshirenai import RULE, match_kamoshirenai
from tests.test_grammar_te_iru import sentence, tok


class KamoshirenaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くかもしれない。': 'かもしれない',
            '雨かもしれません。': 'かもしれません',
            '静かかもしれなかった。': 'かもしれなかった',
            '寒かったかもしれませんでした。': 'かもしれませんでした',
            '素性のしれない者。': None, '行くかも。': None,
            '行くかもしれん。': None, '行くかも しれない。': None,
            '行くかもしれなかっ': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kamoshirenai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_example(self):
        source = sentence('明日は学校へ行くかもしれないよ。', [
            tok('明日は学校へ', '学校', ('名詞',)),
            tok('行く', '行く', ('動詞',)),
            tok('か', 'か', ('助詞', '副助詞')),
            tok('も', 'も', ('助詞', '係助詞')),
            tok('しれ', 'しれる', ('動詞',)),
            tok('ない', 'ない', ('助動詞',)),
            tok('よ。', 'よ', ('助詞',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['か', 'も', 'しれ', 'ない'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'かもしれない')
        self.assertIn('行く', words)
