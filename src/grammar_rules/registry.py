"""Discover grammar rules and keep their execution order deterministic."""
from importlib import import_module
from pathlib import Path

from src.grammar_rules.base import GrammarRule


def discover_rules():
    root = Path(__file__).parent / 'rules'
    rules = []
    for path in sorted(root.glob('*/index.py')):
        module = import_module(f'src.grammar_rules.rules.{path.parent.name}.index')
        declared = getattr(module, 'RULES', None)
        if not isinstance(declared, tuple) or not declared:
            raise ValueError(f'{module.__name__} must declare a nonempty RULES tuple')
        if any(not isinstance(rule, GrammarRule) for rule in declared):
            raise ValueError(f'{module.__name__} contains an invalid grammar rule')
        rules.extend(declared)
    if len({rule.id for rule in rules}) != len(rules):
        raise ValueError('Duplicate grammar rule id')
    if len({rule.expression for rule in rules}) != len(rules):
        raise ValueError('Duplicate grammar expression')
    return tuple(sorted(rules, key=lambda rule: (rule.order, rule.id)))


RULES = discover_rules()
