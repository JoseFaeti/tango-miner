"""Compatibility exports must survive unused-import cleanup."""
import unittest
from importlib import import_module


class CompatibilityExportTests(unittest.TestCase):
    def test_legacy_exports_refer_to_current_implementations(self):
        exports = {
            'src.GrammarConstants': {
                'src.grammar_rules.rules.te_iru.index': ('TE_IRU_GRAMMAR_EXPRESSION',),
                'src.grammar_rules.rules.tai.index': ('TAI_GRAMMAR_EXPRESSION',),
            },
            'src.Grammar': {
                'src.grammar_rules.base': ('GrammarRule',),
                **{f'src.grammar_rules.rules.{name}.index': (f'match_{name}',)
                   for name in ('te_iru', 'nda', 'tai', 'te_shimau', 'monono')},
            },
            'src.steps.MergeTokensStep.index': {
                'src.steps.MergeTokensStep.helpers': (
                    '_is_split_kana_noun', '_is_contracted_kana_verb', 'has_kanji'),
                'src.steps.MergeTokensStep.grammar_guards': ('_is_inflection_misread_as_noun',),
                'src.steps.MergeTokensStep.recovery': (
                    '_repair_kigahen_boundaries', '_repair_chigee_boundary',
                    '_repair_warui_koto_boundary'),
            },
            'src.steps.TokenizeStep.index': {
                'src.steps.MergeTokensStep.index': ('combine_tokens_jmdict', 'get_jmdict'),
            },
        }
        for legacy_name, sources in exports.items():
            legacy = import_module(legacy_name)
            for source_name, names in sources.items():
                source = import_module(source_name)
                for name in names:
                    with self.subTest(module=legacy_name, name=name):
                        self.assertIs(getattr(legacy, name), getattr(source, name))
