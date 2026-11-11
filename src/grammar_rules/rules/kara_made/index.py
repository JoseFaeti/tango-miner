"""Source and endpoint particles with source-aligned attachment."""
from src.GrammarConstants import KARA_MADE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kara_made(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        surface = particle['surface']
        if (surface not in ('から', 'まで') or particle['lemma'] != surface
                or not adjacent(previous, particle)):
            continue
        if surface == 'から':
            if (particle['pos'][:2] != ['助詞', '格助詞']
                    or previous['pos'][:1] not in (['名詞'], ['代名詞'], ['数詞'])):
                continue
        else:
            if (particle['pos'][:2] != ['助詞', '副助詞']
                    or previous['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'], ['助動詞'])):
                continue
            if (i + 1 < len(tokens) and adjacent(particle, tokens[i + 1])
                    and tokens[i + 1]['surface'] == 'に'
                    and tokens[i + 1]['pos'][:2] == ['助詞', '格助詞']):
                continue
            # ここまで美しい and それほどまで express degree, not an endpoint.
            if previous['lemma'] in ('ほど', '程'):
                continue
            if (previous['lemma'] in ('ここ', 'そこ', 'あそこ', 'どこ')
                    and i + 1 < len(tokens) and adjacent(particle, tokens[i + 1])
                    and tokens[i + 1]['pos'][:1] == ['形容詞']):
                continue
        yield tuple(particle['span'])


RULE = GrammarRule(id='kara_made', expression=KARA_MADE_EXPRESSION,
                   reading='から / まで', meaning='from; until; up to', match=match_kara_made)
