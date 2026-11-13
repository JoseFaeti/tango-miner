"""Explanatory reason with ものだから or colloquial もんだから."""
MONODAKARA_MONDAKARA_EXPRESSION = '〜ものだから / 〜もんだから'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_monodakara_mondakara(tokens):
    for i in range(1, len(tokens) - 2):
        predicate, mono, copula, kara = tokens[i - 1:i + 3]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or mono['surface'] not in ('もの', 'もん')
                or mono['lemma'] != mono['surface'] or mono['pos'][:1] != ['名詞']
                or (copula['surface'], copula['lemma']) != ('だ', 'だ')
                or copula['pos'][:1] != ['助動詞']
                or (kara['surface'], kara['lemma']) != ('から', 'から')
                or kara['pos'][:2] != ['助詞', '接続助詞']
                or not all(adjacent(a, b) for a, b in
                           ((predicate, mono), (mono, copula), (copula, kara)))):
            continue
        yield mono['span'][0], kara['span'][1]


RULE = GrammarRule(order=70, id='monodakara_mondakara', expression=MONODAKARA_MONDAKARA_EXPRESSION,
                   reading='ものだから / もんだから', meaning='because; since (explanation or excuse)',
                   match=match_monodakara_mondakara)

RULES = (RULE,)
