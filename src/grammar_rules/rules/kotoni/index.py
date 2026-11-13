"""Speaker-reaction expressions ending in ことに."""
KOTONI_EXPRESSION = '〜ことに'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


REACTIONS = {'嬉しい', 'うれしい', '悲しい', '哀しい', '悔しい', '恐ろしい',
             'ありがたい', '有り難い', '幸い', '残念', '不思議', '意外',
             '奇妙', '遺憾', '幸運', '不幸', '驚く', '困る'}


def match_kotoni(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, koto, ni = tokens[i - 1:i + 2]
        if (koto['surface'] not in ('こと', '事') or koto['lemma'] not in ('こと', '事')
                or koto['pos'][:2] != ['名詞', '普通名詞']
                or ni['surface'] != 'に' or ni['lemma'] != 'に'
                or ni['pos'][:1] != ['助詞']
                or not adjacent(predicate, koto) or not adjacent(koto, ni)):
            continue
        pos = predicate['pos']
        if (predicate['lemma'] in REACTIONS and pos[:1] == ['形容詞']
                and len(pos) >= 6 and pos[5].startswith('連体形')):
            pass
        elif (predicate['surface'] in ('な', 'た') and pos[:1] == ['助動詞']
              and len(pos) >= 6 and pos[5].startswith('連体形') and i >= 2
              and adjacent(tokens[i - 2], predicate)
              and tokens[i - 2]['lemma'] in REACTIONS
              and ((predicate['surface'] == 'な' and tokens[i - 2]['pos'][:1] == ['形状詞'])
                   or (predicate['surface'] == 'た' and tokens[i - 2]['pos'][:1] == ['動詞']))):
            pass
        else:
            continue
        yield koto['span'][0], ni['span'][1]


RULE = GrammarRule(order=67, id='kotoni', expression=KOTONI_EXPRESSION, reading='ことに',
                   meaning='to the speaker’s surprise, regret, delight, etc.', match=match_kotoni)

RULES = (RULE,)
