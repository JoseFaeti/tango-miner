import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.youtomaito.index import RULE, match_youtomaito


class YoutomaitoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '降ろうと降るまいと行く。': '降ろうと降るまいと',
            '食べようと食べまいと自由だ。': '食べようと食べまいと',
            'しようとしまいと同じだ。': 'しようとしまいと',
            '来ようと来まいと構わない。': '来ようと来まいと',
            '行こうと帰るまいと構わない。': None,
            '行こうと行くまいと思う。': '行こうと行くまいと',
            '行こうと 行くまいと構わない。': None,
            '行こうが行くまいが構わない。': None,
            '行こうと行くまいが構わない。': None,
            '行こうと思う。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_youtomaito(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('行こうと行くまいと関係ない。', [
            tok('行こう', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '意志推量形')),
            tok('と', 'と', ('助詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('まい', 'まい', ('助動詞',)), tok('と', 'と', ('助詞',)),
            tok('関係', '関係', ('名詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['行こう', 'と', '行く', 'まい', 'と'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '行こうと行くまいと')
