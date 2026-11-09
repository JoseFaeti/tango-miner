"""Verbal negative requests and negative connective expressions."""
from src.GrammarConstants import NAIDE_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nai.index import match_nai


def match_naide(tokens):
    starts = None
    for i in range(len(tokens) - 1):
        negative, connective = tokens[i:i + 2]
        if (negative['surface'] != 'ない' or connective['surface'] != 'で'
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(negative, connective)):
            continue
        if starts is None:
            starts = {start for start, end in match_nai(tokens)
                      if any(tuple(t['span']) == (start, end) and t['surface'] == 'ない' for t in tokens)}
        if negative['span'][0] not in starts:
            continue
        end = connective['span'][1]
        if i + 2 < len(tokens):
            request = tokens[i + 2]
            if (adjacent(connective, request)
                    and request['pos'][:2] == ['動詞', '非自立可能']
                    and len(request['pos']) >= 6
                    and request['pos'][5].startswith('命令形')
                    and (request['lemma'], request['surface']) in
                    (('くださる', 'ください'), ('下さる', '下さい'),
                     ('くれる', 'くれ'), ('呉れる', '呉れ'))):
                end = request['span'][1]
        yield negative['span'][0], end


RULE = GrammarRule(
    id='naide', expression=NAIDE_GRAMMAR_EXPRESSION,
    reading='ないでください / ないで',
    meaning='please do not; without doing', match=match_naide,
)
