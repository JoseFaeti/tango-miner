"""Considering a proposition expressed with と考えると."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_tokangaeruto(tokens):
    for i in range(1, len(tokens) - 2):
        previous, particle = tokens[i - 1:i + 1]
        pos = previous['pos']
        if not (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))
                and adjacent(previous, particle)
                and particle['surface'] == 'と' and particle['pos'][:2] == ['助詞', '格助詞']
                and next_matches(tokens, i, '考える', '動詞', '考える')
                and next_matches(tokens, i + 1, 'と', '助詞', 'と')
                and tokens[i + 2]['pos'][:2] in (['助詞', '接続助詞'], ['助詞', '格助詞'])):
            continue
        yield particle['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(id='tokangaeruto', expression='〜と考えると', reading='とかんがえると',
                   meaning='considering that; when one thinks that', match=match_tokangaeruto)
RULES = (RULE,)
