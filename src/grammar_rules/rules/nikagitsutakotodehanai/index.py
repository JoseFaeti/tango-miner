"""Noun + に限ったことではない: not restricted to that example."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end


def match_nikagitsutakotodehanai(tokens):
    for i in range(1, len(tokens) - 5):
        noun, ni, verb, past, koto = tokens[i - 1:i + 4]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('限っ', 'かぎっ')
                or verb['lemma'] not in ('限る', 'かぎる') or verb['pos'][:1] != ['動詞']
                or past['surface'] != 'た' or past['lemma'] != 'た' or past['pos'][:1] != ['助動詞']
                or koto['surface'] not in ('こと', '事') or koto['pos'][:1] != ['名詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, ni, verb, past), (ni, verb, past, koto)))):
            continue
        j = i + 4
        connector = tokens[j]
        if not adjacent(koto, connector):
            continue
        if connector['surface'] == 'じゃ' and connector['lemma'] == 'だ' and connector['pos'][:1] == ['助動詞']:
            j += 1
        elif (connector['surface'] == 'で' and connector['pos'][:1] == ['助詞']
              and j + 1 < len(tokens) and tokens[j + 1]['surface'] == 'は'
              and tokens[j + 1]['pos'][:1] == ['助詞'] and adjacent(connector, tokens[j + 1])):
            j += 2
        else:
            continue
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        end = negative_predicate_end(tokens, j, polite_lemma='ある', polite_stems=('あり',))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=1, id='nikagitsutakotodehanai',
                   expression='〜に限ったことではない', reading='にかぎったことではない',
                   meaning='not limited to; not unique to', match=match_nikagitsutakotodehanai)
RULES = (RULE,)
