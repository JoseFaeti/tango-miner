"""Explanatory clauses followed by a softening or contrastive connective."""
NDAKEDO_NDESUGA_EXPRESSION = '〜んだけど / 〜んですが'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nda.index import match_nda


def match_ndakedo_ndesuga(tokens):
    for start, end in match_nda(tokens):
        for i, copula in enumerate(tokens[:-1]):
            if copula['span'][1] != end:
                continue
            marker = tokens[i + 1]
            surface = ''.join(t['surface'] for t in tokens
                              if start <= t['span'][0] and t['span'][1] <= end)
            if (surface not in ('んだ', 'んです', 'のだ', 'のです')
                    or marker['surface'] not in ('けど', 'けれど', 'けれども', 'が')
                    or marker['pos'][:2] != ['助詞', '接続助詞']
                    or not adjacent(copula, marker)):
                continue
            marker_end = marker['span'][1]
            if marker['surface'] == 'けれど' and i + 2 < len(tokens):
                mo = tokens[i + 2]
                if (adjacent(marker, mo) and mo['surface'] == 'も'
                        and mo['lemma'] == 'も' and mo['pos'][:2] == ['助詞', '係助詞']):
                    marker_end = mo['span'][1]
            yield start, marker_end


RULE = GrammarRule(order=64, id='ndakedo_ndesuga', expression=NDAKEDO_NDESUGA_EXPRESSION,
                   reading='んだけど / んですが',
                   meaning='explanatory introduction; but; softening', match=match_ndakedo_ndesuga)

RULES = (RULE,)
