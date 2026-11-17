"""Noun + を顧みず: without regard for."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_okaerimizu(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, verb, zu = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('顧み', '顧みる'), ('かえりみ', 'かえりみる'))
                or verb['pos'][:1] != ['動詞']
                or zu['surface'] != 'ず' or zu['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip((noun, wo, verb), (wo, verb, zu)))):
            continue
        end = zu
        if (i + 3 < len(tokens) and adjacent(zu, tokens[i + 3])
                and tokens[i + 3]['surface'] == 'に'
                and tokens[i + 3]['pos'][:1] == ['助詞']):
            end = tokens[i + 3]
        yield wo['span'][0], end['span'][1]


RULE = GrammarRule(order=1, id='okaerimizu', expression='〜を顧みず',
                   reading='をかえりみず', meaning='without regard for; disregarding',
                   match=match_okaerimizu)
RULES = (RULE,)
