"""Conditional ば after an aligned hypothetical predicate."""
from src.GrammarConstants import BA_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.obligation import match_obligation


def match_ba(tokens):
    obligation_spans = None
    for i in range(1, len(tokens) - 1):
        previous, particle = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (particle['surface'] != 'ば' or particle['lemma'] != 'ば'
                or particle['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(previous, particle)
                or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith('仮定形')):
            continue
        if not any(t['pos'][:1] != ['補助記号'] for t in tokens[i + 1:]):
            continue
        # Complete なければならない / いけない belongs to the obligation card.
        if previous['lemma'] == 'ない':
            if obligation_spans is None:
                obligation_spans = tuple(match_obligation(tokens))
            if any(start <= particle['span'][0] and particle['span'][1] <= end
                   for start, end in obligation_spans):
                continue
        yield tuple(particle['span'])


RULE = GrammarRule(id='conditional_ba', expression=BA_GRAMMAR_EXPRESSION,
                   reading='ば', meaning='if; when; provided that', match=match_ba)
