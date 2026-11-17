import unittest
from dataclasses import replace
from pathlib import Path

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.dispatch import RuleDispatch, SURFACE_TRIGGERS
from src.grammar_rules.registry import RULES
from src.steps.TokenizeStep.index import tokenize_raw_sentences
from src.grammar_rules.rules.te_iru.test_index import tok


class GrammarDispatchTests(unittest.TestCase):
    def test_triggered_positives_and_boundary_controls(self):
        cases = {
            '猫ってかわいい': 'tte', '猫っていう動物': 'toiu', '猫って言う名前': 'toiu',
            '雨が降るとしても出かける。': 'toshitemo_nishitemo',
            '行ったにしても遅すぎる。': 'toshitemo_nishitemo',
            '彼は犯人に決まっている。': 'nikimatsuteiru',
            '彼が勝つにきまっている。': 'nikimatsuteiru',
            '彼は犯人に決まってた。': 'nikimatsuteru',
            '彼は犯人に相違ない。': 'nisouinai',
            '静かに相違なかった。': 'nisouinai',
            '彼は犯人に違いない。': 'nichigainai',
            '予想にちがいない。': 'nichigainai',
            '雨が降らないかな。': 'naikana',
            '仕事に向いてた。': 'nimuiteiru_muki',
            '初心者向きの本。': 'nimuiteiru_muki',
            '猫と いう動物': None, '彼に 決まっている。': None,
            '彼に 相違ない。': None, '子供に しても無理だ。': None,
        }
        sentences = tokenize_raw_sentences(list(cases), tag='source', source_path=Path('source.txt'))
        dispatch = RuleDispatch(RULES)
        for sentence, expected in zip(sentences, cases.values()):
            with self.subTest(text=sentence.text):
                tokens, error = aligned_tokens(sentence.text, sentence.tokens)
                self.assertIsNone(error)
                candidates = dispatch.candidates(tokens)
                original = [(r.id, a, b) for r in RULES for a, b in r.match(tokens)]
                indexed = [(r.id, a, b) for r in candidates for a, b in r.match(tokens)]
                self.assertEqual(original, indexed)
                if expected:
                    self.assertIn(expected, {rule_id for rule_id, _, _ in original})

    def test_absent_triggers_skip_only_canonical_rules_and_preserve_order(self):
        selected = next(rule for rule in RULES if rule.id == 'tte')
        other = next(rule for rule in RULES if rule.id not in SURFACE_TRIGGERS)
        replacement = replace(selected, match=lambda tokens: iter(((0, 1),)))
        tokens = [{'surface': '猫'}]
        self.assertEqual(RuleDispatch((selected, other)).candidates(tokens), (other,))
        self.assertEqual(RuleDispatch((replacement, other)).candidates(tokens), (replacement, other))
        self.assertEqual(RuleDispatch((other, selected)).candidates([{'surface': 'って'}]),
                         (other, selected))

    def test_kana_boundary_variants_remain_candidates(self):
        rule = next(rule for rule in RULES if rule.id == 'nimuiteiru_muki')
        dispatch = RuleDispatch((rule,))
        noun = tok('仕事', '仕事', ('名詞', '普通名詞'))
        particle = tok('に', 'に', ('助詞', '格助詞'))
        connective = tok('て', 'て', ('助詞', '接続助詞'))
        iru = tok('いる', 'いる', ('動詞', '非自立可能'))
        for surface, lemma in (('向い', '向く'), ('むい', 'むく')):
            verb = tok(surface, lemma, ('動詞', '一般', '*', '*', '五段', '連用形-イ音便'))
            text = '仕事に' + surface + 'ている'
            tokens, error = aligned_tokens(text, [noun, particle, verb, connective, iru])
            with self.subTest(surface=surface):
                self.assertIsNone(error)
                self.assertTrue(list(rule.match(tokens)))
                self.assertEqual(dispatch.candidates(tokens), (rule,))
        for surface in ('向き', 'むき'):
            suffix = tok(surface, surface, ('接尾辞', '名詞的'))
            tokens, error = aligned_tokens('仕事' + surface, [noun, suffix])
            with self.subTest(surface=surface):
                self.assertIsNone(error)
                self.assertTrue(list(rule.match(tokens)))
                self.assertEqual(dispatch.candidates(tokens), (rule,))

    def test_stage_equivalence_including_precedence_and_reservation(self):
        texts = ['猫って本当にかわいいですね。', '猫という動物が好きです。',
                 '彼は犯人に決まってる。', '彼は犯人に相違なかった。',
                 '雨にしても雪にしても出かける。',
                 '劫火と戦ってからというもの自分の力に自信がついた気がする。',
                 '食べてから出かけます。', '猫って本当にかわいいですね。']
        sentences = tokenize_raw_sentences(texts, tag='source', source_path=Path('source.txt'))
        stages, outputs = [], []
        for indexed in (False, True):
            stage = GrammarStep(prefer_expressions=True, indexed_dispatch=indexed)
            artifact = stage.process(Artifact(sentences))
            output = ExcludeGrammarTokensStep(stage).process(artifact)
            stages.append(stage)
            outputs.append(output.data)
        self.assertEqual(stages[0].spans, stages[1].spans)
        self.assertEqual(stages[0].matches, stages[1].matches)
        self.assertEqual(stages[0].occurrences, stages[1].occurrences)
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(stages[0].occurrences['te_kara'].count, 1)
        self.assertTrue(any(token.get('grammar_reserved') for s in outputs[0] for token in s.tokens))


if __name__ == '__main__':
    unittest.main()
