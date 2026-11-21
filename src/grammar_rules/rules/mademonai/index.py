"""Verb + までもない: there is no need to go so far as to do it."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_mademonai(tokens):
    for i in range(1, len(tokens) - 2):
        verb, made = tokens[i - 1:i + 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or not adjacent(verb, made)
                or made['surface'] != 'まで' or made['lemma'] != 'まで'
                or made['pos'][:1] != ['助詞']
                or not next_matches(tokens, i, 'も', '助詞', 'も')
                or not adjacent(tokens[i + 1], tokens[i + 2])):
            continue
        end = negative_predicate_end(tokens, i + 2, polite_lemma='ある',
                                     polite_stems={'あり'})
        if end is not None:
            yield made['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='mademonai', expression='〜までもない', reading='までもない',
                   meaning='there is no need to; it does not warrant doing',
                   match=match_mademonai, order=79)
RULES = (RULE,)
