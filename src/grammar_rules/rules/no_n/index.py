"""Bare utterance-final explanatory の and ん."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


NO_N_EXPRESSION = 'の / ん'


def match_no_n(tokens):
    for i in range(1, len(tokens)):
        predicate, marker = tokens[i - 1:i + 1]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or marker['surface'] not in ('の', 'ん')
                or marker['lemma'] != marker['surface']
                or marker['pos'][:2] not in (['助詞', '準体助詞'], ['助詞', '終助詞'])
                or not adjacent(predicate, marker)):
            continue
        if i + 1 < len(tokens) and tokens[i + 1]['surface'] not in ('。', '！', '!', '？', '?', '」', '』'):
            continue
        yield tuple(marker['span'])


RULE = GrammarRule(order=54, id='no_n', expression=NO_N_EXPRESSION,
                   reading='の / ん', meaning='explanatory question or ending', match=match_no_n)
RULES = (RULE,)
