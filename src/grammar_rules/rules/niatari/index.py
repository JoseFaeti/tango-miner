"""Formal occasion expressed by the continuative に当たり."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.niatatsute_nisaishite.index import LEXICAL_ATARU_NOUNS


def match_niatari(tokens):
    for i in range(1, len(tokens) - 1):
        before, ni, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or (before['pos'][:1] == ['動詞'] and before['surface'] != before['lemma'])
                or before['lemma'] in LEXICAL_ATARU_NOUNS
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('当たり', '当たる'), ('あたり', 'あたる'))
                or verb['pos'][:1] != ['動詞']
                or not adjacent(before, ni) or not adjacent(ni, verb)):
            continue
        following = tokens[i + 2] if i + 2 < len(tokens) else None
        if (following and adjacent(verb, following)
                and following['pos'][:1] in (['助動詞'], ['動詞'])):
            continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(id='niatari', expression='〜に当たり', reading='にあたり',
                   meaning='on the occasion of; when', match=match_niatari, order=11)
RULES = (RULE,)
