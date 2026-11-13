"""Informal negative benefactive requests."""
TE_KURENAI_TEMORAENAI_EXPRESSION = '〜てくれない / 〜てもらえない'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_te_kurenai_temoraenai(tokens):
    for i in range(1, len(tokens) - 2):
        verb, connective, auxiliary, negative = tokens[i - 1:i + 3]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or (auxiliary['lemma'], auxiliary['surface']) not in
                   (('くれる', 'くれ'), ('呉れる', '呉れ'),
                    ('もらえる', 'もらえ'), ('貰える', '貰え'))
                or auxiliary['pos'][:2] != ['動詞', '非自立可能']
                or negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in
                           ((verb, connective), (connective, auxiliary), (auxiliary, negative)))):
            continue
        end = i + 2
        if end + 1 < len(tokens) and adjacent(tokens[end], tokens[end + 1]):
            following = tokens[end + 1]
            if following['surface'] == 'か' and following['pos'][:2] == ['助詞', '終助詞']:
                end += 1
            elif following['pos'][:1] != ['補助記号']:
                continue
        if end + 1 < len(tokens) and tokens[end + 1]['pos'][:1] != ['補助記号']:
            continue
        yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=39, id='te_kurenai_temoraenai', expression=TE_KURENAI_TEMORAENAI_EXPRESSION,
                   reading='てくれない / てもらえない', meaning='could you do for me',
                   match=match_te_kurenai_temoraenai)

RULES = (RULE,)
