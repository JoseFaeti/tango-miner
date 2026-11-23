"""It is fair to characterize something as the quoted claim."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import (
    adjacent,
    negative_predicate_end,
    negative_suffix_end,
    next_matches,
    polite_suffix_end,
)


def match_toitsutesashitsukaenai(tokens):
    for i in range(1, len(tokens) - 4):
        previous = tokens[i - 1]
        quote, verb, te = tokens[i:i + 3]
        if (previous['pos'][:1] == ['補助記号']
                or not all(adjacent(a, b) for a, b in zip(
                    [previous, quote, verb], [quote, verb, te]))
                or (quote['surface'], quote['lemma'], quote['pos'][:1]) != ('と', 'と', ['助詞'])
                or verb['surface'] not in ('言っ', 'いっ')
                or verb['lemma'] not in ('言う', 'いう') or verb['pos'][:1] != ['動詞']
                or (te['surface'], te['lemma'], te['pos'][:1]) != ('て', 'て', ['助詞'])):
            continue
        j = i + 3
        if next_matches(tokens, i + 2, 'も', '助詞', 'も'):
            j += 1
        if j + 1 >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        word = tokens[j]
        if word['surface'] not in ('差し支え', '差しつかえ', 'さしつかえ'):
            continue
        if not adjacent(word, tokens[j + 1]):
            continue
        if word['pos'][:1] == ['動詞'] and word['lemma'] in ('差し支える', '差しつかえる', 'さしつかえる'):
            suffix = tokens[j + 1]
            if suffix['lemma'] == 'ない':
                end = negative_suffix_end(tokens, j + 1, pos='助動詞')
                if end is not None and suffix['surface'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
                    end += 1
            else:
                end = polite_suffix_end(tokens, j + 1, forms=('negative',))
        elif word['pos'][:1] == ['名詞'] and word['lemma'] in ('差し支え', '差しつかえ', 'さしつかえ'):
            end = negative_predicate_end(tokens, j + 1, polite_lemma='ある', polite_stems=('あり',))
        else:
            continue
        if end is not None:
            yield quote['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=70, id='toitsutesashitsukaenai', expression='〜といって差し支えない',
                   reading='といってさしつかえない', meaning='it is fair to say',
                   match=match_toitsutesashitsukaenai)
RULES = (RULE,)
