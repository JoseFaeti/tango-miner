"""Negative condition followed by 限り."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_naikagiri(tokens):
    for i in range(len(tokens) - 1):
        negative, kagiri = tokens[i:i + 2]
        if not (negative['surface'] == 'ない' and negative['lemma'] == 'ない'
                and negative['pos'][:1] in (['助動詞'], ['形容詞'])
                and kagiri['surface'] in ('限り', 'かぎり')
                and kagiri['lemma'] in ('限り', '限る', 'かぎり', 'かぎる')
                and kagiri['pos'][:1] in (['名詞'], ['動詞'])
                and adjacent(negative, kagiri)):
            continue
        if (i + 2 < len(tokens) and adjacent(kagiri, tokens[i + 2])
                and tokens[i + 2]['surface'] in ('で', 'だ', 'です', 'の', 'に')):
            continue
        yield negative['span'][0], kagiri['span'][1]


RULE = GrammarRule(order=3, id='naikagiri', expression='〜ない限り',
                   reading='ないかぎり', meaning='unless; as long as not',
                   match=match_naikagiri)
RULES = (RULE,)
