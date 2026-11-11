"""Source-aligned discourse-final particles ね, よ and よね."""
from src.GrammarConstants import NE_YO_YONE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def _particle(token, surface):
    return (token['surface'] == surface and token['lemma'] == surface
            and token['pos'][:2] == ['助詞', '終助詞'])


def match_ne_yo_yone(tokens):
    i = 1
    while i < len(tokens):
        previous, marker = tokens[i - 1:i + 1]
        if (not (_particle(marker, 'ね') or _particle(marker, 'よ'))
                or not adjacent(previous, marker)
                or previous['pos'][:1] in (['空白'], ['補助記号'])):
            i += 1
            continue
        end = marker['span'][1]
        if (marker['surface'] == 'よ' and i + 1 < len(tokens)
                and _particle(tokens[i + 1], 'ね') and adjacent(marker, tokens[i + 1])):
            i += 1
            end = tokens[i]['span'][1]
        yield marker['span'][0], end
        i += 1


RULE = GrammarRule(id='ne_yo_yone', expression=NE_YO_YONE_EXPRESSION,
                   reading='ね / よ / よね',
                   meaning='seeking agreement; informing or emphasizing', match=match_ne_yo_yone)
