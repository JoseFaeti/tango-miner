"""Conservative surface triggers for selected registered grammar rules.

Each trigger is a necessary token surface in the current matcher. Rules not
listed here, and replacement rules with the same ID, are always invoked.
"""
from src.grammar_rules.registry import RULES

SURFACE_TRIGGERS = {
    'tte': ('って',),
    'toiu': ('いう', '言う'),
    'toshitemo_nishitemo': ('し',),
    'nikimatsuteiru': ('決まっ', 'きまっ'),
    'nikimatsuteru': ('決まっ', 'きまっ'),
    'nisouinai': ('相違',),
    'nichigainai': ('違い', 'ちがい'),
    'naikana': ('ない',),
    'nimuiteiru_muki': ('向き', 'むき', '向い', 'むい'),
}
_CANONICAL = {rule.id: rule for rule in RULES}


class RuleDispatch:
    """Collect candidate rules once per sentence in their original order."""
    def __init__(self, rules):
        self.rules = tuple(rules)
        self._surface_masks = {}
        self._rule_masks = []
        for rule in self.rules:
            surfaces = SURFACE_TRIGGERS.get(rule.id) if _CANONICAL.get(rule.id) is rule else None
            mask = 1 << len(self._rule_masks) if surfaces else 0
            self._rule_masks.append(mask)
            if surfaces:
                for surface in surfaces:
                    self._surface_masks[surface] = self._surface_masks.get(surface, 0) | mask
        self._candidates = {}

    def candidates(self, tokens):
        mask = 0
        get_mask = self._surface_masks.get
        for token in tokens:
            mask |= get_mask(token['surface'], 0)
        candidates = self._candidates.get(mask)
        if candidates is None:
            candidates = tuple(rule for rule, required in zip(self.rules, self._rule_masks)
                               if not required or mask & required)
            self._candidates[mask] = candidates
        return candidates
