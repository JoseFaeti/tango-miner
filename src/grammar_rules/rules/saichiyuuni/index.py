"""In the middle of an ongoing action or event."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_saichiyuuni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, marker, particle = tokens[i - 1:i + 2]
        contracted = (previous['surface'] in ('てる', 'でる')
                      and previous['lemma'] in ('てる', 'でる')
                      and previous['pos'][:1] == ['助動詞'] and i >= 2
                      and tokens[i - 2]['pos'][:1] == ['動詞']
                      and adjacent(tokens[i - 2], previous))
        progressive = (previous['surface'] in ('いる', '居る')
                       and previous['lemma'] in ('いる', '居る')
                       and previous['pos'][:2] == ['動詞', '非自立可能'] and i >= 3
                       and tokens[i - 2]['surface'] in ('て', 'で')
                       and tokens[i - 2]['pos'][:2] == ['助詞', '接続助詞']
                       and tokens[i - 3]['pos'][:1] in (['動詞'], ['助動詞'])
                       and adjacent(tokens[i - 3], tokens[i - 2])
                       and adjacent(tokens[i - 2], previous))
        nominal = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'] and i >= 2
                   and tokens[i - 2]['pos'][:1] == ['名詞']
                   and adjacent(tokens[i - 2], previous))
        if ((progressive or contracted or nominal) and marker['surface'] in ('最中', 'さいちゅう')
                and marker['lemma'] in ('最中', 'さいちゅう')
                and marker['pos'][:1] == ['名詞']
                and particle['surface'] == 'に' and particle['lemma'] == 'に'
                and particle['pos'][:2] == ['助詞', '格助詞']
                and adjacent(previous, marker) and adjacent(marker, particle)):
            yield marker['span'][0], particle['span'][1]


RULE = GrammarRule(order=104, id='saichiyuuni', expression='〜最中に',
                   reading='さいちゅうに', meaning='in the middle of',
                   match=match_saichiyuuni)
RULES = (RULE,)
