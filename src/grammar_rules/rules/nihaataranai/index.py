"""Negative 当たる following には, including ambiguous lexical uses."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end


def match_nihaataranai(tokens):
    for i in range(1, len(tokens) - 2):
        before, ni, wa, verb = tokens[i - 1:i + 3]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] not in (['助詞'], ['助動詞'])
                or wa['surface'] != 'は' or wa['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('当たる', 'あたる')
                or not all(adjacent(a, b) for a, b in ((before, ni), (ni, wa), (wa, verb)))):
            continue
        end = negative_predicate_end(tokens, i + 2, plain_lemma=verb['lemma'],
                                     plain_stems=('当たら', 'あたら'),
                                     polite_lemma=verb['lemma'], polite_stems=('当たり', 'あたり'),
                                     continuative=True, extended=True)
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nihaataranai', expression='〜には当たらない', reading='にはあたらない',
                   meaning='does not warrant; does not amount to', match=match_nihaataranai, order=30)
RULES = (RULE,)
