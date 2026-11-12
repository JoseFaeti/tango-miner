"""Informal questions ending in かい or だい."""
from src.GrammarConstants import KAI_DAI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kai_dai(tokens):
    for i in range(1, len(tokens) - 1):
        previous, marker, final = tokens[i - 1:i + 2]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞'],
                                        ['助動詞'], ['名詞'], ['副詞'], ['代名詞'], ['助詞'])
                or not adjacent(previous, marker) or not adjacent(marker, final)):
            continue
        if (final['surface'], final['lemma'], final['pos'][:2]) != ('い', 'い', ['助詞', '終助詞']):
            continue
        if (marker['surface'], marker['lemma'], marker['pos'][:1]) not in (
                ('か', 'か', ['助詞']), ('だ', 'だ', ['助動詞'])):
            continue
        if marker['surface'] == 'か' and marker['pos'][:2] != ['助詞', '終助詞']:
            continue
        if (i + 2 < len(tokens) and adjacent(final, tokens[i + 2])
                and tokens[i + 2]['surface'] in ('な', 'なあ')):
            continue
        yield marker['span'][0], final['span'][1]


RULE = GrammarRule(id='kai_dai', expression=KAI_DAI_EXPRESSION,
                   reading='かい / だい', meaning='informal question', match=match_kai_dai)
