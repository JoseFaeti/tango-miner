"""Possibility expressed by an attached かもしれない."""
from src.GrammarConstants import KAMOSHIRENAI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end


def match_kamoshirenai(tokens):
    for i in range(1, len(tokens) - 3):
        previous, ka, mo, shire = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞'], ['助動詞'], ['名詞'])
                or not all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 2))
                or (ka['surface'], ka['lemma'], ka['pos'][:2]) != ('か', 'か', ['助詞', '副助詞'])
                or (mo['surface'], mo['lemma'], mo['pos'][:2]) != ('も', 'も', ['助詞', '係助詞'])
                or shire['surface'] != 'しれ' or shire['lemma'] != 'しれる'
                or shire['pos'][:1] != ['動詞']):
            continue
        end = negative_predicate_end(tokens, i + 2, plain_lemma='しれる', plain_stems=('しれ',),
                                     polite_lemma='しれる', polite_stems=('しれ',))
        if end is not None:
            yield ka['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='kamoshirenai', expression=KAMOSHIRENAI_GRAMMAR_EXPRESSION,
                   reading='かもしれない', meaning='might; may; perhaps', match=match_kamoshirenai)
