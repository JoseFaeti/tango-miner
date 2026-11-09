"""Ability expressed by a verb followed by ことができる."""
from src.GrammarConstants import KOTO_GA_DEKIRU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end


def match_koto_ga_dekiru(tokens):
    for i in range(1, len(tokens)):
        verb, koto = tokens[i - 1:i + 1]
        if koto['surface'] not in ('こと', '事'):
            continue
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or not adjacent(verb, koto)
                or koto['surface'] not in ('こと', '事')
                or koto['lemma'] not in ('こと', '事') or koto['pos'][:1] != ['名詞']
                or not next_matches(tokens, i, 'が', '助詞', 'が')):
            continue
        j = i + 2
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        dekiru = tokens[j]
        if dekiru['lemma'] not in ('できる', '出来る') or dekiru['pos'][:1] != ['動詞']:
            continue
        surface = dekiru['surface']
        if surface in ('できる', '出来る'):
            end = j
        elif surface in ('でき', '出来'):
            if next_matches(tokens, j, 'た', '助動詞', 'た'):
                end = j + 1
            elif next_matches(tokens, j, 'ない', '助動詞'):
                end = negative_suffix_end(tokens, j + 1, pos='助動詞')
            elif next_matches(tokens, j, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, j + 1, forms=('positive', 'negative'))
            else:
                end = None
        else:
            end = None
        if end is not None:
            yield koto['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='koto_ga_dekiru', expression=KOTO_GA_DEKIRU_EXPRESSION,
                   reading='ことができる', meaning='can do; be able to do',
                   match=match_koto_ga_dekiru)
