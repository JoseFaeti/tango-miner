"""Suggestions and instructions with verb-te + ごらん."""
from src.GrammarConstants import TE_GORAN_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_te_goran(tokens):
    for i in range(1, len(tokens) - 1):
        verb, te, goran = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or te['surface'] not in ('て', 'で')
                or te['pos'][:2] != ['助詞', '接続助詞']
                or goran['surface'] not in ('ごらん', 'ご覧', '御覧')
                or goran['lemma'] != goran['surface']
                or goran['pos'][:1] != ['名詞']
                or not adjacent(verb, te) or not adjacent(te, goran)):
            continue
        end = i + 1
        if end + 1 < len(tokens) and adjacent(goran, tokens[end + 1]):
            following = tokens[end + 1]
            if following['surface'] in ('なさい', 'ください', '下さい') and following['pos'][:1] == ['動詞']:
                end += 1
            elif following['surface'] in ('に', 'の'):
                continue
        yield te['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='te_goran', expression=TE_GORAN_EXPRESSION, reading='てごらん',
                   meaning='try doing; go ahead and do', match=match_te_goran)
