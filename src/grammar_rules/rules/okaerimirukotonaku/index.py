"""Noun + を顧みることなく: without regard for."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OKAERIMIRUKOTONAKU_EXPRESSION = '〜を顧みることなく'


def match_okaerimirukotonaku(tokens):
    for i in range(1, len(tokens) - 3):
        noun, wo, verb, koto, naku = tokens[i - 1:i + 4]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (('顧みる', '顧みる'), ('かえりみる', 'かえりみる'))
                or verb['pos'][:1] != ['動詞']
                or koto['surface'] != 'こと' or koto['pos'][:1] != ['名詞']
                or naku['surface'] != 'なく' or naku['lemma'] != 'ない'
                or naku['pos'][:1] != ['形容詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, wo, verb, koto), (wo, verb, koto, naku)))):
            continue
        yield wo['span'][0], naku['span'][1]


RULE = GrammarRule(order=1, id='okaerimirukotonaku', expression=OKAERIMIRUKOTONAKU_EXPRESSION,
                   reading='をかえりみることなく', meaning='without regard for; disregarding',
                   match=match_okaerimirukotonaku)
RULES = (RULE,)
