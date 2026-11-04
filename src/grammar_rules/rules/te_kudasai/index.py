"""Imperative requests following a verb's te-form."""
from src.GrammarConstants import TE_KUDASAI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_te_kudasai(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, request = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, connective) or not adjacent(connective, request)
                or request['pos'][:2] != ['動詞', '非自立可能']
                or len(request['pos']) < 6 or not request['pos'][5].startswith('命令形')):
            continue
        if ((request['lemma'], request['surface']) not in
                (('くださる', 'ください'), ('下さる', '下さい'),
                 ('くれる', 'くれ'), ('呉れる', '呉れ'))):
            continue
        yield connective['span'][0], request['span'][1]


RULE = GrammarRule(
    id='te_kudasai', expression=TE_KUDASAI_GRAMMAR_EXPRESSION,
    reading='てください / てくれ', meaning='please do; do (a request)',
    match=match_te_kudasai,
)
