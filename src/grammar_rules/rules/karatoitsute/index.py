"""Concessive reasoning with からといって."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_karatoitsute(tokens):
    for i in range(1, len(tokens) - 3):
        previous, kara, particle, verb, te = tokens[i - 1:i + 4]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or kara['surface'] != 'から' or kara['lemma'] != 'から'
                or kara['pos'][:2] != ['助詞', '接続助詞']
                or particle['surface'] != 'と' or particle['pos'][:2] != ['助詞', '格助詞']
                or verb['surface'] not in ('いっ', '言っ')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:2] != ['助詞', '接続助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, kara, particle, verb), (kara, particle, verb, te)))):
            continue
        yield kara['span'][0], te['span'][1]


RULE = GrammarRule(id='karatoitsute', expression='〜からといって',
                   reading='からといって', meaning='just because; it does not follow that',
                   match=match_karatoitsute, order=2)
RULES = (RULE,)
