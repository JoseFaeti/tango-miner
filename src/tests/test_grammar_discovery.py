import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.grammar_rules import registry
from src.grammar_rules.base import GrammarRule
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


def rule(identifier, expression, order=1000000):
    return GrammarRule(identifier, expression, '', '', lambda tokens: (), order)


class GrammarDiscoveryTests(unittest.TestCase):
    def discover(self, modules):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in modules:
                path = root / 'rules' / name
                path.mkdir(parents=True)
                (path / 'index.py').touch()
            with patch.object(registry, 'Path', return_value=root / 'registry.py'), patch.object(
                    registry, 'import_module', side_effect=lambda name: modules[name.split('.')[-2]]):
                return registry.discover_rules()

    def test_discovers_multiple_rules_in_deterministic_order(self):
        later = rule('later', 'later', 4)
        earlier = rule('earlier', 'earlier', 1)
        default = rule('default', 'default')
        result = self.discover({'z': SimpleNamespace(RULES=(later, earlier)),
                                'a': SimpleNamespace(RULES=(default,))})
        self.assertEqual(result, (earlier, later, default))

    def test_rejects_missing_invalid_and_duplicate_rules(self):
        first = rule('a', 'A')
        for rules in (None, (), [first], ('bad',), (first, rule('a', 'B')),
                      (first, rule('b', 'A'))):
            with self.subTest(rules=rules), self.assertRaises(ValueError):
                self.discover({'point': SimpleNamespace(__name__='point', RULES=rules)})

    def test_protection_sets_cover_discovered_rules(self):
        self.assertEqual(GRAMMAR_EXPRESSIONS, frozenset(r.expression for r in registry.RULES))
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS,
                         {s + ' (grammar)': s for s in GRAMMAR_EXPRESSIONS})
