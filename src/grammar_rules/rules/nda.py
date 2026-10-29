"""Rule for nda."""
from src.GrammarConstants import NDA_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


RULE = GrammarRule(
    id='explanatory_nda', expression=NDA_GRAMMAR_EXPRESSION, reading='んだ',
    meaning=('Grammar: explanatory んだ. Presents a statement as an explanation '
             'or emphasizes its relevance to the situation; nuance depends on context.'),
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
                end = j
                if word['surface'] == 'なかっ':
                    if (j + 1 >= len(tokens) or not adjacent(word, tokens[j + 1])
                            or tokens[j + 1]['lemma'] != 'た'
                            or tokens[j + 1]['pos'][:1] != ['助動詞']):
                        continue
                    end += 1
            elif word['lemma'] == 'ある' and word['pos'][:1] == ['動詞']:
                end = j
                if (j + 2 < len(tokens) and adjacent(word, tokens[j + 1])
                        and adjacent(tokens[j + 1], tokens[j + 2])
                        and tokens[j + 1]['lemma'] == 'ます' and tokens[j + 1]['surface'] == 'ませ'
                        and tokens[j + 2]['lemma'] == 'ぬ' and tokens[j + 2]['surface'] == 'ん'):
                    end = j + 2
                    if (end + 2 < len(tokens) and adjacent(tokens[end], tokens[end + 1])
                            and adjacent(tokens[end + 1], tokens[end + 2])
                            and tokens[end + 1]['lemma'] == 'です' and tokens[end + 1]['surface'] == 'でし'
                            and tokens[end + 2]['lemma'] == 'た'):
                        end += 2
                elif word['surface'] != 'ある':
                    continue
            else:
                continue
        elif surface not in ('だ', 'です', 'だろう', 'でしょう', 'でしょ'):
            continue
        yield particle['span'][0], tokens[end]['span'][1]
