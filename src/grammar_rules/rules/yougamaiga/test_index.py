import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.yougamaiga.index import RULE, match_yougamaiga


class YougamaigaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行こうが行くまいが関係ない。': '行こうが行くまいが',
            '食べようが食べまいが自由だ。': '食べようが食べまいが',
            'しようがしまいが同じだ。': 'しようがしまいが',
            '来ようが来まいが構わない。': '来ようが来まいが',
            '行こうが帰るまいが構わない。': None,
            '行こうが行くまいと思う。': None,
            '行こうが 行くまいが構わない。': None,
            '降ろうと降るまいと行く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_yougamaiga(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('行こうが行くまいが関係ない。', [
            tok('行こう', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '意志推量形')),
            tok('が', 'が', ('助詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('まい', 'まい', ('助動詞',)), tok('が', 'が', ('助詞',)),
            tok('関係', '関係', ('名詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['行こう', 'が', '行く', 'まい', 'が'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '行こうが行くまいが')
