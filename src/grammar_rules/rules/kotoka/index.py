"""Exclamations ending in ことか."""
KOTOKA_EXPRESSION = '〜ことか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kotoka(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, koto, ka = tokens[i - 1:i + 2]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith('連体形')
                or koto['surface'] not in ('こと', '事')
                or koto['pos'][:2] != ['名詞', '普通名詞']
                or ka['surface'] != 'か' or ka['lemma'] != 'か'
                or ka['pos'][:2] != ['助詞', '終助詞']
                or not adjacent(predicate, koto) or not adjacent(koto, ka)):
            continue
        # A following clause often makes this an embedded question, not an exclamation.
        if i + 2 < len(tokens) and tokens[i + 2]['pos'][:1] != ['補助記号']:
            continue
        yield koto['span'][0], ka['span'][1]


RULE = GrammarRule(order=68, id='kotoka', expression=KOTOKA_EXPRESSION,
                   reading='ことか', meaning='how; what an (exclamation)',
                   match=match_kotoka)

RULES = (RULE,)
