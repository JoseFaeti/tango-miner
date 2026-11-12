"""Concessive nagara followed by the focusing particle mo."""
from src.GrammarConstants import NAGARAMO_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nagaramo(tokens):
    for i in range(1, len(tokens) - 2):
        previous, nagara, mo = tokens[i - 1:i + 2]
        pos = previous['pos']
        if (nagara['surface'] != 'ながら' or nagara['lemma'] != 'ながら'
                or nagara['pos'][:2] != ['助詞', '接続助詞']
                or mo['surface'] != 'も' or mo['lemma'] != 'も'
                or mo['pos'][:2] != ['助詞', '係助詞']
                or not adjacent(previous, nagara) or not adjacent(nagara, mo)):
            continue
        if pos[:1] == ['動詞']:
            if len(pos) < 6 or not pos[5].startswith('連用形'):
                continue
        elif pos[:1] not in (['名詞'], ['形状詞'], ['形容詞']):
            continue
        for token in tokens[i + 2:]:
            if token['surface'] in ('。', '！', '？', '!', '?'):
                break
            if token['pos'][:1] != ['補助記号']:
                yield (nagara['span'][0], mo['span'][1])
                break


RULE = GrammarRule(id='nagaramo', expression=NAGARAMO_EXPRESSION,
                   reading='ながらも', meaning='although; even though',
                   match=match_nagaramo)
