"""Strong denial with ものか or colloquial もんか."""
from src.GrammarConstants import MONOKA_MONKA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_monoka_monka(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, mono, ka = tokens[i - 1:i + 2]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or mono['surface'] not in ('もの', 'もん')
                or mono['lemma'] != mono['surface'] or mono['pos'][:1] != ['名詞']
                or ka['surface'] != 'か' or ka['lemma'] != 'か'
                or ka['pos'][:2] != ['助詞', '終助詞']
                or not adjacent(predicate, mono) or not adjacent(mono, ka)):
            continue
        yield mono['span'][0], ka['span'][1]


RULE = GrammarRule(id='monoka_monka', expression=MONOKA_MONKA_EXPRESSION,
                   reading='ものか / もんか', meaning='never; absolutely not',
                   match=match_monoka_monka)
