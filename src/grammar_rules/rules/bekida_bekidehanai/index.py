"""Verb-attached obligation and prohibition with finite copulas."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_bekida(tokens):
    for i in range(1, len(tokens) - 1):
        verb, beki, copula = tokens[i - 1:i + 2]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or (beki['surface'], beki['lemma'], beki['pos'][:1]) != ('べき', 'べし', ['助動詞'])
                or not adjacent(verb, beki) or not adjacent(beki, copula)
                or copula['pos'][:1] != ['助動詞'] or copula['lemma'] not in ('だ', 'です')):
            continue
        surface = copula['surface']
        if (copula['lemma'], surface) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif (copula['lemma'], surface) in (('だ', 'だっ'), ('です', 'でし')):
            if not next_matches(tokens, i + 1, 'た', '助動詞', 'た'):
                continue
            end = i + 2
        elif copula['lemma'] == 'だ' and surface in ('で', 'じゃ'):
            start = i + 2
            if surface == 'で':
                if not next_matches(tokens, i + 1, 'は', '助詞', 'は'):
                    continue
                start += 1
            if start >= len(tokens) or not adjacent(tokens[start - 1], tokens[start]):
                continue
            end = negative_predicate_end(tokens, start, polite_lemma='ある', polite_stems=('あり',))
            if end is None:
                continue
            if tokens[start]['lemma'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
        else:
            continue
        yield beki['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=132, id='bekida_bekidehanai', expression='〜べきだ / 〜べきではない',
                   reading='べきだ / べきではない', meaning='should; ought to / should not; ought not to',
                   match=match_bekida)
RULES = (RULE,)
