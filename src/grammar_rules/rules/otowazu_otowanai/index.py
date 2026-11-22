"""Regardless of a named condition, with 問わず or 問わない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.otowazu.index import match_otowazu


def match_otowazu_otowanai(tokens):
    yield from match_otowazu(tokens)
    for i in range(1, len(tokens) - 2):
        noun, wo, verb, negative = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and (verb['surface'], verb['lemma']) in (('問わ', '問う'), ('とわ', 'とう'))
                and verb['pos'][:1] == ['動詞']
                and negative['surface'] == 'ない' and negative['lemma'] == 'ない'
                and negative['pos'][:1] == ['助動詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, verb), (wo, verb, negative)))):
            yield wo['span'][0], negative['span'][1]


RULE = GrammarRule(order=1, id='otowazu_otowanai', expression='〜を問わず / 〜を問わない',
                   reading='をとわず / をとわない', meaning='regardless of; irrespective of',
                   match=match_otowazu_otowanai)
RULES = (RULE,)
