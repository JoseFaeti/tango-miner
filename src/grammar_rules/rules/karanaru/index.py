"""Noun + からなる: consist of."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_karanaru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, kara, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'], ['接尾辞'])
                or kara['surface'] != 'から' or kara['lemma'] != 'から'
                or kara['pos'][:2] != ['助詞', '格助詞']
                or verb['lemma'] not in ('なる', '成る')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, kara) or not adjacent(kara, verb)):
            continue
        if verb['surface'] in ('なる', '成る'):
            yield kara['span'][0], verb['span'][1]
        elif verb['surface'] in ('なっ', '成っ') and i + 2 < len(tokens):
            tail = tokens[i + 2]
            if (adjacent(verb, tail)
                    and ((tail['surface'] == 'た' and tail['pos'][:1] == ['助動詞'])
                         or (tail['surface'] == 'て' and tail['pos'][:2] == ['助詞', '接続助詞']))):
                yield kara['span'][0], tail['span'][1]


RULE = GrammarRule(id='karanaru', expression='〜からなる', reading='からなる',
                   meaning='consist of', match=match_karanaru, order=55)
RULES = (RULE,)
