import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.Grammar import AddGrammarCardsStep, GrammarOccurrences, GrammarStep
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.dictionary_equivalents import recover_dictionary_candidates
from src.grammar_rules.rules.niwatatsute.index import RULE
from src.Pipeline import Pipeline
from src.SegmentedSentence import SegmentedSentence
from src.steps.AddDefinitionsStep.index import AddDefinitionsStep
from src.WordStats import Sentence
from tests.helpers import make_stats


class DictionaryGrammarRecoveryTests(unittest.TestCase):
    def test_configured_dictionary_definition_recovers_example(self):
        artifact = Artifact({'にわたって': make_stats()})
        artifact = Pipeline([AddDefinitionsStep()]).run(artifact)
        self.assertTrue(artifact.data['にわたって'].jmdict_definition)
        recover_dictionary_candidates(artifact.data, [], (RULE,))
        self.assertIn(RULE.expression, artifact.data)

    def candidate(self, definition=RULE.meaning):
        stats = make_stats(definition=definition, frequency=4, index=0.2, index_count=1)
        stats.jmdict_definition = True
        return stats

    def test_recovery_combines_counts_and_remaps_examples_before_attachment(self):
        grammar = GrammarStep(rules=(RULE,))
        grammar.occurrences = {RULE.id: GrammarOccurrences(
            count=2, tags={'other'}, index=0.6, index_count=1,
            examples=[Sentence('長期間にわたって続いた。', 'other', 'file', 'にわたって')])}
        grammar.spans = {}
        sentence = SegmentedSentence('長期間にわたって続いた。', 'source', Path('file'),
                                     {'にわたって': 'にわたって'},
                                     {'にわたって': (3, 8)})
        sentence.lemma_occurrences['にわたって'] = [('にわたって', (3, 8), None)]
        artifact = Artifact({'にわたって': self.candidate()}, sentences=[sentence])
        AddGrammarCardsStep(grammar).process(artifact)
        self.assertEqual(set(artifact.data), {RULE.expression})
        stats = artifact.data[RULE.expression]
        self.assertEqual(stats.frequency, 6)
        self.assertAlmostEqual(stats.index, 0.4)
        self.assertEqual(stats.tags, {'source', 'other'})
        self.assertIn(RULE.expression, sentence.lemma_occurrences)
        self.assertNotIn('にわたって', sentence.lemma_surfaces)

    def test_recovers_without_any_structural_match(self):
        stats = self.candidate('1. throughout<br>2. over a span of ...; over a period of ...')
        words = {'にわたって': stats}
        recover_dictionary_candidates(words, [], (RULE,))
        self.assertEqual(words[RULE.expression].pos, ('grammar',))
        self.assertEqual(stats.frequency, 4)

    def test_lexical_controls_remain(self):
        for word, definition, dictionary in (
            ('にわたって', 'crossing', True),
            ('にわたって', RULE.meaning + '; crossing', True),
            ('別の語', RULE.meaning, True),
            ('にわたって', RULE.meaning, False),
        ):
            with self.subTest(word=word, definition=definition, dictionary=dictionary):
                stats = self.candidate(definition)
                stats.jmdict_definition = dictionary
                words = {word: stats}
                recover_dictionary_candidates(words, [], (RULE,))
                self.assertEqual(list(words), [word])

    def test_ambiguous_rule_mapping_is_retained(self):
        other = GrammarRule('other', 'other', RULE.reading, RULE.meaning, lambda _: ())
        words = {'にわたって': self.candidate()}
        recover_dictionary_candidates(words, [], (RULE, other))
        self.assertEqual(list(words), ['にわたって'])
