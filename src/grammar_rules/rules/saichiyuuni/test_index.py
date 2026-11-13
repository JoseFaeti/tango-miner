import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.saichiyuuni.index import RULE, match_saichiyuuni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SaichiyuuniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べている最中に電話が鳴った。': '最中に',
            '会議の最中に電話が鳴った。': '最中に',
            '食べてる最中に電話が鳴った。': '最中に',
            '会議のさいちゅうに電話が鳴った。': 'さいちゅうに',
            '最中にあんこを詰める。': None,
            '最中を食べる。': None,
            '食べる最中に電話が鳴った。': None,
            '会議の最中は静かだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_saichiyuuni(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_attached_word_and_evidence(self):
        source = sentence('会議の最中に電話が鳴った。', [
            tok('会議', '会議', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('最中', '最中', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('電話が鳴った。', '電話', ('名詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['最中', 'に'])
        words = {'会議': object()}
        stage.add_cards(words)
        self.assertIn('会議', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '最中に')
