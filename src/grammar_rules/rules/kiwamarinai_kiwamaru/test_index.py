import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kiwamarinai_kiwamaru.index import RULE, match_kiwamarinai_kiwamaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KiwamarinaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = [('失礼極まりない。', '極まりない'), ('危険極まりなかった。', '極まりなかった'),
                 ('危険極まりなく、困る。', '極まりなく'), ('失礼きわまりない。', 'きわまりない'),
                 ('不愉快極まる行為。', '極まる'), ('議論が極まる。', None),
                 ('感激極まって泣く。', None), ('失礼 極まりない。', None)]
        for text, expected in cases:
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kiwamarinai_kiwamaru(aligned)],
                                 [] if expected is None else [expected])

    def test_registry(self):
        self.assertIn(RULE, RULES)

    def test_card_retains_stem(self):
        from src.Artifact import Artifact
        from src.Grammar import ExcludeGrammarTokensStep, GrammarStep

        source = sentence('その態度は失礼極まりないと思います。', [
            tok('その態度は', '態度', ('名詞',)),
            tok('失礼', '失礼', ('名詞', '普通名詞', 'サ変形状詞可能')),
            tok('極まりない', '極まりない', ('形容詞', '一般')),
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        stage.process(Artifact([source, source]))
        words = {'失礼': object()}
        stage.add_cards(words)
        self.assertIn('失礼', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '極まりない')
        result = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved', False))
