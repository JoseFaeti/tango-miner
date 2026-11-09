"""Apparent desire expressed by a verb stem and the auxiliary たがる."""
from src.GrammarConstants import TAGARU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end


def match_tagaru(tokens):
    for i in range(1, len(tokens)):
        verb, desire = tokens[i - 1:i + 1]
        if desire['lemma'] != 'たがる':
            continue
        if not (adjacent(verb, desire) and verb['pos'][:1] == ['動詞']
                and len(verb['pos']) >= 6 and verb['pos'][5].startswith('連用形')
                and desire['pos'][:1] == ['助動詞']):
            continue
        surface = desire['surface']
        end = None
        if surface == 'たがる':
            end = i
        elif surface == 'たがっ':
            if next_matches(tokens, i, 'た', '助動詞', 'た') or next_matches(tokens, i, 'て', '助詞', 'て'):
                end = i + 1
        elif surface == 'たがら' and next_matches(tokens, i, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
        elif surface == 'たがり' and next_matches(tokens, i, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 1, forms=('positive', 'negative'))
        if end is not None:
            yield desire['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tagaru', expression=TAGARU_GRAMMAR_EXPRESSION,
                   reading='たがる', meaning='show signs of wanting to do',
                   match=match_tagaru)
