"""Rule for nda."""
from src.GrammarConstants import NDA_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, negative_predicate_end


RULE = GrammarRule(
    id='explanatory_nda', expression=NDA_GRAMMAR_EXPRESSION, reading='んだ',
    meaning='it is that; the reason is; (adds explanation or emphasis)',
    match=lambda tokens: match_nda(tokens),
)


def match_nda(tokens):
    def predicate_before(i):
        # A bare noun or pronoun + のだ may be possessive (僕のだ).
        return (i > 0 and adjacent(tokens[i - 1], tokens[i])
                and tokens[i - 1]['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞']))

    for i, particle in enumerate(tokens):
        # Sudachi sometimes treats the whole written form as a noun.
        if (particle['surface'] in ('のだ', 'のです') and predicate_before(i)):
            yield tuple(particle['span'])
            continue
        # Some analyses split の + だった but label both as non-copular.
        if (particle['surface'] == 'の' and predicate_before(i)
                and particle['pos'][:1] == ['助詞'] and i + 1 < len(tokens)
                and adjacent(particle, tokens[i + 1])
                and tokens[i + 1]['surface'] == 'だった'):
            yield particle['span'][0], tokens[i + 1]['span'][1]
            continue
        if not (particle['surface'] == particle['lemma']
                and particle['surface'] in ('ん', 'の')
                and particle['pos'][:2] == ['助詞', '準体助詞']
                and (particle['surface'] != 'の' or predicate_before(i))):
            continue
        if i + 1 >= len(tokens) or not adjacent(particle, tokens[i + 1]):
            continue
        copula = tokens[i + 1]
        if copula['pos'][:1] != ['助動詞'] or copula['lemma'] not in ('だ', 'です'):
            continue
        surface = copula['surface']
        end = i + 1
        if surface in ('だっ', 'でし'):
            if (end + 1 >= len(tokens) or not adjacent(tokens[end], tokens[end + 1])
                    or tokens[end + 1]['lemma'] != 'た'
                    or tokens[end + 1]['pos'][:1] != ['助動詞']):
                continue
            end += 1
        elif surface in ('じゃ', 'で'):
            # Include only complete negative or literary copula sequences.
            j = end + 1
            if j < len(tokens) and adjacent(tokens[j - 1], tokens[j]) and tokens[j]['surface'] == 'は' and tokens[j]['pos'][:1] == ['助詞']:
                j += 1
            if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
                continue
            word = tokens[j]
            if word['lemma'] == 'ない' and word['pos'][:1] == ['形容詞']:
                end = negative_suffix_end(tokens, j, pos='形容詞', continuative=True, extended=True)
                if end is None:
                    continue
            elif word['lemma'] == 'ある' and word['pos'][:1] == ['動詞']:
                end = j
                if word['surface'] != 'ある':
                    end = negative_predicate_end(tokens, j, polite_lemma='ある', polite_stems={'あり'})
                    if end is None:
                        continue
            else:
                continue
        elif surface not in ('だ', 'です', 'だろう', 'でしょう', 'でしょ'):
            continue
        yield particle['span'][0], tokens[end]['span'][1]
