"""Conversational explanation or shared understanding with もんね."""
from src.GrammarConstants import MONNE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_monne(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, mon, ne = tokens[i - 1:i + 2]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or mon['surface'] != 'もん' or mon['lemma'] != 'もん'
                or mon['pos'][:1] != ['名詞'] and mon['pos'][:2] != ['助詞', '終助詞']
                or ne['surface'] != 'ね' or ne['lemma'] != 'ね'
                or ne['pos'][:2] != ['助詞', '終助詞']
                or not adjacent(predicate, mon) or not adjacent(mon, ne)):
            continue
        yield mon['span'][0], ne['span'][1]


RULE = GrammarRule(id='monne', expression=MONNE_EXPRESSION, reading='もんね',
                   meaning='because; after all (shared explanation or understanding)',
                   match=match_monne)
