"""Negative wondering that can express a wish or an indirect request."""
from src.GrammarConstants import NAIKANA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.kana_kashira.index import match_kana_kashira


def match_naikana(tokens):
    endings = {start: end for start, end in match_kana_kashira(tokens)}
    for i in range(1, len(tokens)):
        negative, marker = tokens[i - 1:i + 1]
        if (negative['surface'] == 'ない' and negative['lemma'] == 'ない'
                and negative['pos'][:1] in (['助動詞'], ['形容詞'])
                and marker['surface'] == 'か' and adjacent(negative, marker)
                and marker['span'][0] in endings):
            yield negative['span'][0], endings[marker['span'][0]]


RULE = GrammarRule(id='naikana', expression=NAIKANA_EXPRESSION,
                   reading='ないかな', meaning='I hope; I wonder if; an indirect request',
                   match=match_naikana)
