"""Noun + によって / による, with bounded lexical controls."""
from src.GrammarConstants import NIYOTTE_NIYORU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_niyotte_niyoru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] != 'よる' or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        following = tokens[i + 2] if i + 2 < len(tokens) else None
        if verb['surface'] == 'よっ':
            if (following and adjacent(verb, following)
                    and following['surface'] == 'て' and following['pos'][:1] == ['助詞']):
                yield ni['span'][0], following['span'][1]
        elif verb['surface'] == 'よる':
            # によると belongs to the separately scheduled hearsay point.
            if (following and adjacent(verb, following)
                    and following['surface'] == 'と' and following['pos'][:1] == ['助詞']):
                continue
            yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(id='niyotte_niyoru', expression=NIYOTTE_NIYORU_EXPRESSION,
                   reading='によって / による', meaning='by; depending on; due to',
                   match=match_niyotte_niyoru)
