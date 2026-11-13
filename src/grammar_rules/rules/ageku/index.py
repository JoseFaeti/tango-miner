"""After a prolonged action or process, resulting in an outcome."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

AGEKU_EXPRESSION = '〜あげく'


def match_ageku(tokens):
    for i in range(2, len(tokens)):
        anchor, link, ageku = tokens[i - 2:i + 1]
        if (ageku['surface'] not in ('あげく', '挙句', '挙げ句')
                or ageku['lemma'] not in ('あげく', '挙句', '挙げ句')
                or ageku['pos'][:1] != ['名詞']
                or not adjacent(anchor, link) or not adjacent(link, ageku)):
            continue
        past = (anchor['pos'][:1] == ['動詞']
                and link['surface'] in ('た', 'だ') and link['lemma'] in ('た', 'だ')
                and link['pos'][:1] == ['助動詞'])
        nominal = (anchor['pos'][:1] == ['名詞'] and link['surface'] == 'の'
                   and link['lemma'] == 'の' and link['pos'][:1] == ['助詞'])
        if not (past or nominal):
            continue
        end = i
        if (i + 1 < len(tokens) and adjacent(ageku, tokens[i + 1])
                and tokens[i + 1]['surface'] == 'に'
                and tokens[i + 1]['lemma'] == 'に'
                and tokens[i + 1]['pos'][:1] == ['助詞']):
            end += 1
        yield ageku['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=86, id='ageku', expression=AGEKU_EXPRESSION,
                   reading='あげく', meaning='after a long process, ending up with',
                   match=match_ageku)
RULES = (RULE,)
