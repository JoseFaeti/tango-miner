"""Wondering with the final particles かな and かしら."""
from src.GrammarConstants import KANA_KASHIRA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kana_kashira(tokens):
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞'],
                                       ['助動詞'], ['名詞'], ['副詞'], ['代名詞'])
                or not adjacent(previous, marker)):
            continue
        if (marker['surface'], marker['lemma'], marker['pos'][:2]) == ('かしら', 'かしら', ['助詞', '終助詞']):
            yield marker['span'][0], marker['span'][1]
        elif (marker['surface'], marker['lemma'], marker['pos'][:2]) == ('か', 'か', ['助詞', '終助詞']) and i + 1 < len(tokens):
            final = tokens[i + 1]
            if (final['surface'] in ('な', 'なあ') and final['lemma'] == final['surface']
                    and final['pos'][:2] == ['助詞', '終助詞'] and adjacent(marker, final)):
                yield marker['span'][0], final['span'][1]


RULE = GrammarRule(id='kana_kashira', expression=KANA_KASHIRA_EXPRESSION,
                   reading='かな / かしら', meaning='I wonder; wondering whether',
                   match=match_kana_kashira)
