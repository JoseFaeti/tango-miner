"""Negative 当たる following に, including ambiguous lexical uses."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end


def match_niataranai(tokens):
    for i in range(1, len(tokens) - 1):
        before, ni, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] not in (['助詞'], ['助動詞'])
                or verb['lemma'] not in ('当たる', 'あたる')
                or not adjacent(before, ni) or not adjacent(ni, verb)):
            continue
        end = negative_predicate_end(tokens, i + 1, plain_lemma=verb['lemma'],
                                     plain_stems=('当たら', 'あたら'),
                                     polite_lemma=verb['lemma'], polite_stems=('当たり', 'あたり'),
                                     continuative=True, extended=True)
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='niataranai', expression='〜にあたらない', reading='にあたらない',
                   meaning='does not constitute; does not amount to', match=match_niataranai, order=30)
RULES = (RULE,)
