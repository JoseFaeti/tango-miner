"""Source-aligned exemplifying and emphatic particles."""
from src.GrammarConstants import NADO_NANKA_NANTE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nado_nanka_nante(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] not in ('など', 'なんか')
                or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)
                or previous['pos'][:1] == ['空白']):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(id='nado_nanka_nante', expression=NADO_NANKA_NANTE_EXPRESSION,
                   reading='など / なんか / なんて',
                   meaning='such as; things like; emphasis or dismissal',
                   match=match_nado_nanka_nante)
