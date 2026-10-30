"""Source-aligned verb て-form + くる construction."""
from src.GrammarConstants import TE_KURU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import polite_suffix_end, adjacent, next_matches, negative_predicate_end


RULE = GrammarRule(
    id='te_kuru', expression=TE_KURU_GRAMMAR_EXPRESSION, reading='てくる',
    meaning='come and do; do and come back; begin to; continue up to the present',
    match=lambda tokens: match_te_kuru(tokens),
)


def match_te_kuru(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, kuru = tokens[i - 1:i + 2]
        if (not adjacent(verb, connective) or not adjacent(connective, kuru)
                or verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or kuru['lemma'] not in ('くる', '来る')
                or kuru['pos'][:2] != ['動詞', '非自立可能']):
            continue
        end = i + 1
        surface = kuru['surface']
        if surface in ('くる', '来る', 'こよう', '来よう'):
            pass
        elif surface in ('き', '来') and not next_matches(tokens, end, 'ない', '助動詞'):
            if next_matches(tokens, end, 'た', '助動詞'):
                end += 1
            elif next_matches(tokens, end, 'て', '助詞', 'て'):
                end += 1
            elif next_matches(tokens, end, 'ます', '助動詞'):
                end += 1
                end = polite_suffix_end(tokens, end)
                if end is None:
                    continue
            else:
                continue
        elif surface in ('こ', '来'):
            end = negative_predicate_end(tokens, end, polite_lemma=kuru['lemma'],
                                         polite_stems=(), plain_lemma=kuru['lemma'],
                                         plain_stems={surface}, continuative=True, extended=True)
            if end is None:
                continue
        else:
            continue
        yield connective['span'][0], tokens[end]['span'][1]
