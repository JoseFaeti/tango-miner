"""Desire for someone to perform a benefactive action."""
from src.GrammarConstants import TEMORAITAI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.tai.index import match_tai


def match_temoraitai(tokens):
    endings = dict(match_tai(tokens))
    for i in range(1, len(tokens) - 2):
        verb, connective, auxiliary, desire = tokens[i - 1:i + 3]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or auxiliary['surface'] != 'もらい' or auxiliary['lemma'] != 'もらう'
                or auxiliary['pos'][:2] != ['動詞', '非自立可能']
                or not all(adjacent(a, b) for a, b in zip(
                    (verb, connective, auxiliary), (connective, auxiliary, desire)))):
            continue
        end = endings.get(desire['span'][0])
        if end is not None:
            yield connective['span'][0], end


RULE = GrammarRule(id='temoraitai', expression=TEMORAITAI_EXPRESSION,
                   reading='てもらいたい', meaning='want someone to do something for me',
                   match=match_temoraitai)
