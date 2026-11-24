"""Formal noun + に対し: toward or in contrast to."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitaishi(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        nominal = (noun['pos'][:1] in (['名詞'], ['代名詞'])
                   or (noun['surface'] == 'の'
                       and noun['pos'][:2] == ['助詞', '準体助詞']))
        if (not nominal
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] != '対し' or verb['lemma'] != '対する'
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        # Leave inflected verbs and longer particle constructions to their rules.
        if i + 2 < len(tokens):
            tail = tokens[i + 2]
            if adjacent(verb, tail) and tail['pos'][:1] in (['助詞'], ['助動詞']):
                continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=7, id='nitaishi', expression='〜に対し',
                   reading='にたいし', meaning='toward; in contrast to; whereas (formal)',
                   match=match_nitaishi)
RULES = (RULE,)
