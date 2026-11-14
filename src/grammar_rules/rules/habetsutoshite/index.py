"""Source-aligned は別として setting-aside construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_habetsutoshite(tokens):
    for i in range(1, len(tokens) - 4):
        previous = tokens[i - 1]
        particle, betsu, to, shi, te = tokens[i:i + 5]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or particle['surface'] != 'は' or particle['lemma'] != 'は'
                or particle['pos'][:2] != ['助詞', '係助詞']
                or betsu['surface'] not in ('別', 'べつ')
                or betsu['lemma'] not in ('別', 'べつ') or betsu['pos'][:1] != ['名詞']
                or to['surface'] != 'と' or to['lemma'] != 'と' or to['pos'][:1] != ['助詞']
                or shi['surface'] != 'し' or shi['lemma'] != 'する' or shi['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['lemma'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        yield particle['span'][0], te['span'][1]


RULE = GrammarRule(order=2, id='habetsutoshite', expression='〜は別として',
                   reading='はべつとして', meaning='setting aside; apart from',
                   match=match_habetsutoshite)
RULES = (RULE,)
