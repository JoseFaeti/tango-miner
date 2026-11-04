import unittest
from unittest.mock import patch

from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.steps.AddWordsToAnkiStep import export_words_to_anki
from tests.helpers import make_stats
from src.steps.tests.test_add_words_to_anki_step import note


class GrammarIdentityMigrationTests(unittest.TestCase):
    def run_export(self, existing, words, *, apply=False):
        calls, plans = [], []
        def invoke(action, params=None):
            calls.append((action, params))
            if action == 'modelNames': return ['Custom']
            if action == 'findNotes': return [n['noteId'] for n in existing]
            if action == 'notesInfo': return existing
            if action == 'multi': return []
            raise AssertionError(action)
        with patch('builtins.input', return_value='y'):
            result = export_words_to_anki('Deck', words, 'Custom', invoke=invoke,
                                          prune=True, output_only=not apply,
                                          details_handler=plans.append)
        return plans[0], calls, result

    def test_each_legacy_note_is_renamed_and_refreshes_meaning_and_metrics(self):
        for expression in GRAMMAR_EXPRESSIONS:
            with self.subTest(expression=expression):
                old = expression + ' (grammar)'
                existing = note(123, old, tags=['custom'], fields={'Meaning': 'user edit'})
                plan, calls, result = self.run_export([existing], {expression: make_stats()}, apply=True)
                self.assertEqual(result, (0, 1, 0))
                self.assertEqual(plan['modify'][0]['fields'], {
                    'Expression': {'before': old, 'after': expression},
                    'Meaning': {'before': 'user edit', 'after': 'definition'},
                    'Frequency': {'before': '2', 'after': '1'},
                    'Score': {'before': '123.45', 'after': '0.0'},
                })
                actions = next(params['actions'] for action, params in calls if action == 'multi')
                self.assertEqual(actions, [{'action': 'updateNoteFields', 'params': {'note': {
                    'id': 123, 'fields': {'Expression': expression, 'Meaning': 'definition',
                                          'Frequency': '1', 'Score': '0.0'}}}}])
                self.assertEqual(plan['add'], [])
                self.assertEqual(plan['delete'], [])

    def test_legacy_notes_are_protected_when_grammar_absent(self):
        old = next(iter(GRAMMAR_EXPRESSIONS)) + ' (grammar)'
        plan, _, _ = self.run_export([note(1, old)], {})
        self.assertEqual(plan['delete'], [])
        self.assertEqual(plan['retained_existing_grammar'][0]['expression'], old)

    def test_collision_or_duplicate_legacy_notes_are_reported_and_retained(self):
        expression = next(iter(GRAMMAR_EXPRESSIONS))
        old = expression + ' (grammar)'
        for existing in ([note(1, old), note(2, expression)], [note(1, old), note(2, old)]):
            with self.subTest(existing=existing):
                plan, _, _ = self.run_export(existing, {expression: make_stats()})
                self.assertEqual(plan['add'], [])
                self.assertEqual(plan['modify'], [])
                self.assertEqual(plan['delete'], [])
                self.assertEqual(len(plan['grammar_migration_conflicts']), 1)

    def test_migration_removes_only_obsolete_synthetic_tag(self):
        expression = next(iter(GRAMMAR_EXPRESSIONS))
        old = expression + ' (grammar)'
        for tags, expected in [({'source'}, ['grammar']), ({'grammar'}, [])]:
            plan, _, _ = self.run_export([note(1, old, tags=['grammar', 'custom'])],
                                         {expression: make_stats(tags=tags)})
            self.assertEqual(plan['modify'][0]['tags_removed'], expected)

    def test_existing_grammar_refreshes_only_selected_fields(self):
        expression = next(iter(GRAMMAR_EXPRESSIONS))
        existing = note(123, expression, tags=['custom'], fields={
            'Meaning': 'old', 'Reading': 'custom reading', 'Index': '99',
            'Frequency': '4', 'Score': '5', 'Sentence': 'custom example'})
        plan, calls, _ = self.run_export([existing], {
            expression: make_stats(definition='new', frequency=7, score=8)}, apply=True)
        self.assertEqual(plan['modify'][0]['fields'], {
            'Meaning': {'before': 'old', 'after': 'new'},
            'Frequency': {'before': '4', 'after': '7'},
            'Score': {'before': '5', 'after': '8'},
        })
        actions = next(params['actions'] for action, params in calls if action == 'multi')
        self.assertEqual(actions, [{'action': 'updateNoteFields', 'params': {'note': {
            'id': 123, 'fields': {'Meaning': 'new', 'Frequency': '7', 'Score': '8'}}}}])
        self.assertEqual(plan['delete'], [])

    def test_duplicate_current_grammar_notes_are_reported_not_updated_or_deleted(self):
        expression = next(iter(GRAMMAR_EXPRESSIONS))
        plan, calls, _ = self.run_export([note(1, expression), note(2, expression)],
                                         {expression: make_stats()}, apply=True)
        self.assertEqual(plan['modify'], [])
        self.assertEqual(plan['delete'], [])
        self.assertEqual(plan['grammar_migration_conflicts'][0]['current_note_ids'], [1, 2])
        self.assertNotIn('multi', [action for action, _ in calls])
