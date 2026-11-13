"""Informal requests with verb-te + ちょうだい."""
TE_CHOUDAI_EXPRESSION = '〜てちょうだい'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nai.index import match_nai


def match_te_choudai(tokens):
    negative_starts = None
    for i in range(1, len(tokens) - 1):
        verb, connective, request = tokens[i - 1:i + 2]
        if (connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or request['surface'] not in ('ちょうだい', '頂戴')
                or request['lemma'] != request['surface']
                or request['pos'][:1] != ['名詞']
                or not adjacent(verb, connective) or not adjacent(connective, request)):
            continue
        if (i + 2 < len(tokens) and adjacent(request, tokens[i + 2])
                and tokens[i + 2]['lemma'] == 'する'
                and tokens[i + 2]['pos'][:1] == ['動詞']):
            continue
        if verb['surface'] == 'ない' and connective['surface'] == 'で':
            if negative_starts is None:
                negative_starts = set(match_nai(tokens))
            if tuple(verb['span']) not in negative_starts:
                continue
            start = verb['span'][0]
        elif (verb['pos'][:1] in (['動詞'], ['助動詞'])
              and len(verb['pos']) >= 6 and verb['pos'][5].startswith('連用形')):
            start = connective['span'][0]
        else:
            continue
        yield start, request['span'][1]


RULE = GrammarRule(order=41, id='te_choudai', expression=TE_CHOUDAI_EXPRESSION,
                   reading='てちょうだい', meaning='please do (an informal request)',
                   match=match_te_choudai)

RULES = (RULE,)
