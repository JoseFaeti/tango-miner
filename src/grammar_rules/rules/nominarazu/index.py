"""Additive のみならず following a source-aligned expression."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NOMINARAZU_EXPRESSION = '〜のみならず'


def match_nominarazu(tokens):
    for i in range(1, len(tokens) - 2):
        previous, nomi, nara, zu = tokens[i - 1:i + 3]
        if (previous['pos'][:1] == ['空白']
                or (previous['pos'][:1] == ['補助記号']
                    and previous['pos'][1:2] != ['括弧閉'])
                or nomi['surface'] != 'のみ' or nomi['lemma'] != 'のみ'
                or nomi['pos'][:2] != ['助詞', '副助詞']
                or nara['surface'] != 'なら' or nara['lemma'] != 'なり'
                or nara['pos'][:1] != ['助動詞']
                or zu['surface'] != 'ず' or zu['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in
                           zip((previous, nomi, nara), (nomi, nara, zu)))):
            continue
        yield nomi['span'][0], zu['span'][1]


RULE = GrammarRule(order=1, id='nominarazu', expression=NOMINARAZU_EXPRESSION,
                   reading='のみならず', meaning='not only; as well as', match=match_nominarazu)
RULES = (RULE,)
