import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.de.index import RULE, match_de
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'学校で勉強する。': 1, '電車で行く。': 1, '病気で休む。': 1,
                 '家で一人で食べる。': 2, '「学校」で勉強する。': 1,
                 '学校 で勉強する。': 0, 'で行く。': 0, '読んでいる。': 0,
                 '静かで綺麗だ。': 0, 'これです。': 0, '腕を出す。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_de(tokens)], ['で'] * count)

    def test_reservation_and_identity(self):
        source = sentence('学校で毎日勉強する。', [
            tok('学校', '学校', ('名詞',)), tok('で', 'で', ('助詞', '格助詞')),
            tok('毎日', '毎日', ('名詞',)), tok('勉強', '勉強', ('名詞',)), tok('する', 'する', ('動詞',)),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['で'])
        words = {'学校': object(), '勉強': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'で')
        self.assertTrue(all(word in words for word in ('学校', '勉強')))
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_non_particle_and_punctuation_controls(self):
        for previous, particle in [
            (tok('学校', '学校', ('名詞',)), tok('で', 'だ', ('助動詞',))),
            (tok('、', '、', ('補助記号', '読点')), tok('で', 'で', ('助詞', '格助詞'))),
            (tok('学校', '学校', ('名詞',)), tok('で', '違う', ('助詞', '格助詞'))),
        ]:
            text = previous['surface'] + particle['surface']
            tokens, error = aligned_tokens(text, [previous, particle])
            self.assertIsNone(error)
            self.assertEqual(list(match_de(tokens)), [])
