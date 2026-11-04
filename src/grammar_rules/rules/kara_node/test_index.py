import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kara_node.index import RULE, match_kara_node
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaraNodeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るから行かない。': 'から',
            '雨が降ったから。': 'から',
            '学生だから休む。': 'から',
            '静かだから休む。': 'から',
            '雨ですから行かない。': 'から',
            '雨が降るので休む。': 'ので',
            '学生なので休む。': 'ので',
            '静かなので休む。': 'ので',
            '雨ですので休む。': 'ので',
            '駅から歩く。': None,
            '食べてから行く。': None,
            '彼の電話。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kara_node(aligned)],
                                 [expected] if expected else [])

    def test_card_and_reservation(self):
        source = sentence('読むので今日は休みます。', [
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('の', 'の', ('助詞', '準体助詞')),
            tok('で', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '連用形-一般')),
            tok('今日は休みます。', '今日', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['の', 'で'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ので')
        self.assertIn('読む', words)

    def test_split_and_noncausal_tokens_do_not_match(self):
        text = '読む の で'
        raw = [tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
               tok('の', 'の', ('助詞', '準体助詞')),
               tok('で', 'だ', ('助動詞',))]
        aligned, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        self.assertEqual(list(match_kara_node(aligned)), [])
