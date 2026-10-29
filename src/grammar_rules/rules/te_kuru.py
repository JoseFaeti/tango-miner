"""Source-aligned verb て-form + くる construction."""
from src.GrammarConstants import TE_KURU_GRAMMAR_EXPRESSION
from src.GrammarShadow import polite_suffix_end
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


RULE = GrammarRule(
    id='te_kuru', expression=TE_KURU_GRAMMAR_EXPRESSION, reading='てくる',
    meaning=('Grammar: verb て-form + くる can describe motion toward a point, '
             'an action followed by coming back, or a change emerging or continuing '
             'up to the present. Context determines the meaning.'),
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
                suffix = tokens[end]
                end = polite_suffix_end(tokens, end)
                if suffix['surface'] in ('まし', 'ませ') and tokens[end] is suffix:
                    continue
            else:
                continue
        elif surface in ('こ', '来'):
            if not next_matches(tokens, end, 'ない', '助動詞'):
                continue
            end += 1
            if tokens[end]['surface'] == 'なかっ':
                if not next_matches(tokens, end, 'た', '助動詞'):
                    continue
                end += 1
        else:
            continue
        yield connective['span'][0], tokens[end]['span'][1]
