"""Polite requests using the potential of いただく."""
TE_ITADAKEMASENKA_EXPRESSION = '〜ていただけませんか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, polite_suffix_end


def match_te_itadakemasenka(tokens):
    for i in range(1, len(tokens) - 3):
        verb, connective, auxiliary = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or (auxiliary['lemma'], auxiliary['surface']) not in
                   (('いただける', 'いただけ'), ('頂ける', '頂け'))
                or auxiliary['pos'][:2] != ['動詞', '非自立可能']
                or not adjacent(verb, connective) or not adjacent(connective, auxiliary)
                or not adjacent(auxiliary, tokens[i + 2])):
            continue
        end = polite_suffix_end(tokens, i + 2, forms=('negative',))
        if end is None or end + 1 >= len(tokens):
            continue
        question = tokens[end + 1]
        if (question['surface'] == 'か' and question['pos'][:2] == ['助詞', '終助詞']
                and adjacent(tokens[end], question)):
            yield connective['span'][0], question['span'][1]


RULE = GrammarRule(order=38, id='te_itadakemasenka', expression=TE_ITADAKEMASENKA_EXPRESSION,
                   reading='ていただけませんか', meaning='could you please do',
                   match=match_te_itadakemasenka)

RULES = (RULE,)
