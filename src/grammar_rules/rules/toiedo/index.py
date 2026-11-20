"""Literary concessive といえど."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toiedo(tokens):
    for i in range(len(tokens) - 2):
        particle, verb, do = tokens[i:i + 3]
        if not (particle['surface'] == 'と' and particle['pos'][:2] == ['助詞', '格助詞']
                and verb['surface'] in ('いえ', '言え')
                and verb['lemma'] in ('いう', '言う') and verb['pos'][:1] == ['動詞']
                and do['surface'] == 'ど' and do['pos'][:2] == ['助詞', '接続助詞']
                and adjacent(particle, verb) and adjacent(verb, do)):
            continue
        if (i + 3 < len(tokens) and tokens[i + 3]['surface'] == 'も'
                and tokens[i + 3]['pos'][:2] == ['助詞', '係助詞']
                and adjacent(do, tokens[i + 3])):
            continue
        yield particle['span'][0], do['span'][1]


RULE = GrammarRule(order=2, id='toiedo', expression='〜といえど',
                   reading='といえど', meaning='even; although (literary)', match=match_toiedo)
RULES = (RULE,)
