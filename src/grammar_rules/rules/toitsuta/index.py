"""Examples introduced with attributive といった."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.toitsutatokoroda.index import match_toitsutatokoroda

TOITSUTA_EXPRESSION = '〜といった'


def match_toitsuta(tokens):
    longer_starts = {start for start, _ in match_toitsutatokoroda(tokens)}
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, past, noun = tokens[i - 1:i + 4]
        if (previous['pos'][:1] == ['補助記号']
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いっ', '言っ')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or past['surface'] != 'た' or past['lemma'] != 'た' or past['pos'][:1] != ['助動詞']
                or noun['pos'][:1] != ['名詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 3], tokens[i:i + 4]))
                or particle['span'][0] in longer_starts):
            continue
        yield particle['span'][0], past['span'][1]


RULE = GrammarRule(order=92, id='toitsuta', expression=TOITSUTA_EXPRESSION,
                   reading='といった', meaning='such as; like', match=match_toitsuta)
RULES = (RULE,)
