"""Temporal て-form + から, retaining the preceding verb as lexical evidence."""
from src.GrammarConstants import TE_KARA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_te_kara(tokens):
    for i in range(1, len(tokens) - 1):
        verb, te, kara = tokens[i - 1:i + 2]
        pos = verb['pos']
        if (pos[:1] not in (['動詞'], ['助動詞']) or len(pos) < 6
                or not pos[5].startswith('連用形')
                or not adjacent(verb, te) or not adjacent(te, kara)):
            continue
        if (te['surface'] in ('て', 'で') and te['pos'][:2] == ['助詞', '接続助詞']
                and kara['surface'] == 'から' and kara['lemma'] == 'から'
                and kara['pos'][:2] == ['助詞', '格助詞']):
            yield te['span'][0], kara['span'][1]


RULE = GrammarRule(id='te_kara', expression=TE_KARA_EXPRESSION, reading='てから',
                   meaning='after doing; since doing', match=match_te_kara)
