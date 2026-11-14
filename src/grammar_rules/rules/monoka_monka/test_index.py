import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import MONOKA_MONKA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.monoka_monka.index import RULE, match_monoka_monka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonokaMonkaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '二度と行くものか。': 'ものか', '負けるもんか。': 'もんか',
            'そんなことがあるものか。': 'ものか', '静かなものか。': 'ものか',
            '買ったものか？': 'ものか',  # Also a literal question about a thing.
            'そんなものか。': None, '買うものの名前。': None,
            '行くものかどうか。': None,
            'あるいは、全く別の、何者かの思惑で動いているものか？': None,
            'フーンだ、誰がアンタらみたいなザコと何度もたたかうもんか！': 'もんか',
            '私の槍一本でどこまでブルメシアを守れるものか…': 'ものか',
            'あるいは私が負けるものか？': 'ものか',
            '誰がそんなことをしているものか？': 'ものか',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monoka_monka(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        mono = tok('もの', 'もの', ('名詞',))
        ka = tok('か', 'か', ('助詞', '終助詞'))
        source = sentence('二度とあの場所へ行くものか。', [
            tok('二度とあの場所へ', '場所', ('名詞',)), verb, mono, ka,
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in reserved.data[0].tokens))
        words = {'行く': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn('もの', words)
        self.assertEqual(words[MONOKA_MONKA_EXPRESSION].frequency, 2)
        self.assertEqual(words[MONOKA_MONKA_EXPRESSION].sentences[0].surface_form, 'ものか')
        aligned, error = aligned_tokens('行く ものか', [verb, mono, ka])
        self.assertIsNone(error)
        self.assertEqual(list(match_monoka_monka(aligned)), [])
