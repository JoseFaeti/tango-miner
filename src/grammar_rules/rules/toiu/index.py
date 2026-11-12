"""Attributive quotation and naming with という or っていう."""
from src.GrammarConstants import TOIU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.toiukotoda.index import match_toiukotoda
from src.grammar_rules.rules.toiukotoha.index import match_toiukotoha
from src.grammar_rules.rules.toiunoha.index import match_toiunoha
from src.grammar_rules.rules.toiuyori.index import match_toiuyori


def match_toiu(tokens):
    longer_starts = {start for matcher in (match_toiukotoda, match_toiukotoha, match_toiunoha, match_toiuyori)
                     for start, _ in matcher(tokens)}
    for i in range(1, len(tokens) - 2):
        previous, particle, verb, noun = tokens[i - 1:i + 3]
        if (particle['surface'] not in ('と', 'って')
                or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う')
                or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞']
                or noun['pos'][:1] != ['名詞']
                or previous['pos'][:1] == ['補助記号']
                or not all(adjacent(a, b) for a, b in
                           ((previous, particle), (particle, verb), (verb, noun)))):
            continue
        if particle['span'][0] not in longer_starts:
            yield particle['span'][0], verb['span'][1]


RULE = GrammarRule(id='toiu', expression=TOIU_EXPRESSION, reading='という / っていう',
                   meaning='called; named; that is; saying that', match=match_toiu)
