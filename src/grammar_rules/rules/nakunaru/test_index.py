import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nakunaru.index import RULE, match_nakunaru
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NakunaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '酒を飲まなくなる。': 'なくなる',
            'テレビを見なくなった。': 'なくなった',
            '学校に行かなくなりました。': 'なくなりました',
            '話さなくなっている。': 'なくなって',
            '食べられなくなる。': 'なくなる',
            '行かなくならない。': 'なくならない',
            'お金がなくなった。': None,
            '痛くなくなった。': None,
            '行かなく なる。': None,
            '行かなくなっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nakunaru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('酒を飲まなくなった。', [
            tok('酒を', '酒', ('名詞',)),
            tok('飲ま', '飲む', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('なく', 'ない', ('助動詞',)), tok('なっ', 'なる', ('動詞',)),
            tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE, NAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[NAI.id].count, 0)
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['なく', 'なっ', 'た'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なくなった')
