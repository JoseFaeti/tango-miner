"""Local しか plus negative predicate; reserve only the limiting particle."""
from src.GrammarConstants import SHIKANAI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_shikanai(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'しか' or particle['lemma'] != 'しか'
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)
                or previous['pos'][:1] in (['空白'], ['補助記号'])):
            continue
        # Restrict the search to a local predicate, avoiding unrelated clauses.
        for j in range(i + 1, min(len(tokens), i + 4)):
            token = tokens[j]
            if not adjacent(tokens[j - 1], token):
                break
            if token['lemma'] == 'ない' and token['pos'][:1] in (['形容詞'], ['助動詞']):
                if negative_suffix_end(tokens, j, pos=token['pos'][0], conditional=True) is not None:
                    yield tuple(particle['span'])
                break
            if token['lemma'] == 'ます' and token['pos'][:1] == ['助動詞']:
                if polite_suffix_end(tokens, j, forms=('negative',)) is not None:
                    yield tuple(particle['span'])
                break
            if token['pos'][:1] not in (['動詞'], ['助動詞']):
                break


RULE = GrammarRule(id='shikanai', expression=SHIKANAI_EXPRESSION,
                   reading='しかない', meaning='only; nothing but', match=match_shikanai)
