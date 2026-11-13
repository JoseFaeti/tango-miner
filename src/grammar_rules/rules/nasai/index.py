"""Commands formed with a verb's continuative and なさい."""
NASAI_EXPRESSION = '〜なさい'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nasai(tokens):
    for i in range(1, len(tokens)):
        verb, command = tokens[i - 1:i + 1]
        if (adjacent(verb, command) and verb['pos'][:1] == ['動詞']
                and len(verb['pos']) >= 6 and verb['pos'][5].startswith('連用形')
                and command['surface'] == 'なさい' and command['lemma'] == 'なさる'
                and command['pos'][:2] == ['動詞', '非自立可能']
                and len(command['pos']) >= 6 and command['pos'][5].startswith('命令形')):
            yield command['span'][0], command['span'][1]


RULE = GrammarRule(order=172, id='nasai', expression=NASAI_EXPRESSION, reading='なさい',
                   meaning='do (an instruction or command)', match=match_nasai)

RULES = (RULE,)
