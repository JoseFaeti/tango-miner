import unittest
from unittest.mock import patch

from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.steps.AddWordsToAnkiStep import export_words_to_anki
from tests.helpers import make_stats
from tests.test_add_words_to_anki_step import note


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

    def test_each_legacy_note_is_renamed_in_place_preserving_other_fields(self):
        for expression in GRAMMAR_EXPRESSIONS:
            with self.subTest(expression=expression):
                old = expression + ' (grammar)'
                existing = note(123, old, tags=['custom'], fields={'Meaning': 'user edit'})
                plan, calls, result = self.run_export([existing], {expression: make_stats()}, apply=True)
                self.assertEqual(result, (0, 1, 0))
                self.assertEqual(plan['modify'][0]['fields'], {'Expression': {'before': old, 'after': expression}})
                actions = next(params['actions'] for action, params in calls if action == 'multi')
                self.assertEqual(actions, [{'action': 'updateNoteFields', 'params': {'note': {
                    'id': 123, 'fields': {'Expression': expression}}}}])
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
