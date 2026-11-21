"""No exaggeration in characterizing something as the quoted claim."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_toitsutemokagondehanai(tokens):
    for i in range(1, len(tokens) - 7):
        previous = tokens[i - 1]
        part = tokens[i:i + 5]
        expected = (('と', 'と', '助詞'), ('言っ', '言う', '動詞'),
                    ('て', 'て', '助詞'), ('も', 'も', '助詞'),
                    ('過言', '過言', '名詞'))
        if (previous['pos'][:1] == ['補助記号']
                or not all(adjacent(a, b) for a, b in zip([previous] + part, part))):
            continue
        if any((t['surface'], t['lemma'], t['pos'][:1]) != (surface, lemma, [pos])
               for t, (surface, lemma, pos) in zip(part, expected)
               if lemma != '言う'):
            continue
        verb = part[1]
        if (verb['surface'] not in ('言っ', 'いっ') or verb['lemma'] not in ('言う', 'いう')
                or verb['pos'][:1] != ['動詞']):
            continue
        copula = tokens[i + 5]
        if not adjacent(part[-1], copula):
            continue
        if copula['surface'] == 'じゃ' and copula['lemma'] == 'だ' and copula['pos'][:1] == ['助動詞']:
            start = i + 6
        elif (copula['surface'] == 'で' and copula['pos'][:1] == ['助詞']
              and next_matches(tokens, i + 5, 'は', '助詞', 'は')):
            start = i + 7
        else:
            continue
        if start >= len(tokens) or not adjacent(tokens[start - 1], tokens[start]):
            continue
        end = negative_predicate_end(tokens, start, polite_lemma='ある', polite_stems=('あり',))
        if end is None:
            continue
        if tokens[start]['lemma'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
            end += 1
        yield part[0]['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=70, id='toitsutemokagondehanai', expression='〜と言っても過言ではない',
                   reading='といってもかごんではない', meaning='it is no exaggeration to say',
                   match=match_toitsutemokagondehanai)
RULES = (RULE,)
