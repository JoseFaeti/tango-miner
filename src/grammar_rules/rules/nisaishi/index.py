"""Formal occasion expressed by the continuative に際し."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nisaishi(tokens):
    for i in range(1, len(tokens) - 1):
        before, ni, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or (before['pos'][:1] == ['動詞'] and before['surface'] != before['lemma'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) != ('際し', '際する')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(before, ni) or not adjacent(ni, verb)):
            continue
        following = tokens[i + 2] if i + 2 < len(tokens) else None
        if (following and adjacent(verb, following)
                and (following['surface'] == 'て' or following['pos'][:1] == ['助動詞'])):
            continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(id='nisaishi', expression='〜に際し', reading='にさいし',
                   meaning='on the occasion of; when', match=match_nisaishi, order=11)
RULES = (RULE,)
