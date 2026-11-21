"""Topic association with というと; といえば has an existing card."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toiuto_toieba(tokens):
    for topic, particle, verb, ending in zip(tokens, tokens[1:], tokens[2:], tokens[3:]):
        if (topic['pos'][:1] in (['名詞'], ['代名詞'], ['形容詞'], ['動詞'])
                and particle['surface'] == 'と' and particle['pos'][:1] == ['助詞']
                and verb['surface'] in ('いう', '言う')
                and verb['lemma'] in ('いう', '言う') and verb['pos'][:1] == ['動詞']
                and ending['surface'] == ending['lemma'] == 'と'
                and ending['pos'][:2] == ['助詞', '接続助詞']
                and all(adjacent(a, b) for a, b in
                        ((topic, particle), (particle, verb), (verb, ending)))):
            yield particle['span'][0], ending['span'][1]


RULE = GrammarRule(id='toiuto_toieba', expression='〜というと / 〜といえば',
                   reading='というと / といえば', meaning='speaking of; when it comes to',
                   match=match_toiuto_toieba, order=30)
RULES = (RULE,)
