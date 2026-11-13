"""Ever since an event expressed by a verb in the te-form."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TE_IRAI_EXPRESSION = '〜て以来'


def match_te_irai(tokens):
    for i in range(1, len(tokens) - 1):
        verb, te, irai = tokens[i - 1:i + 2]
        pos = verb['pos']
        if (pos[:1] in (['動詞'], ['助動詞']) and len(pos) >= 6
                and pos[5].startswith('連用形')
                and te['surface'] in ('て', 'で')
                and te['pos'][:2] == ['助詞', '接続助詞']
                and irai['surface'] == '以来' and irai['lemma'] == '以来'
                and irai['pos'][:1] == ['名詞']
                and adjacent(verb, te) and adjacent(te, irai)):
            yield te['span'][0], irai['span'][1]


RULE = GrammarRule(order=85, id='te_irai', expression=TE_IRAI_EXPRESSION,
                   reading='ていらい', meaning='ever since doing', match=match_te_irai)
RULES = (RULE,)
