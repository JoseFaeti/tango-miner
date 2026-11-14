"""Noun + を問わず: regardless of the named condition."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OTOWAZU_EXPRESSION = '〜を問わず'


def match_otowazu(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, verb, zu = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (('問わ', '問う'), ('とわ', 'とう'))
                or verb['pos'][:1] != ['動詞']
                or zu['surface'] != 'ず' or zu['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip((noun, wo, verb), (wo, verb, zu)))):
            continue
        if (i + 3 < len(tokens) and adjacent(zu, tokens[i + 3])
                and tokens[i + 3]['surface'] == 'に'
                and tokens[i + 3]['pos'][:1] == ['助詞']):
            continue
        yield wo['span'][0], zu['span'][1]


RULE = GrammarRule(order=1, id='otowazu', expression=OTOWAZU_EXPRESSION,
                   reading='をとわず', meaning='regardless of; irrespective of',
                   match=match_otowazu)
RULES = (RULE,)
