"""Conditional suggestions ending in どう or どうですか."""
TARADOU_EXPRESSION = '〜たらどう / 〜たらどうですか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.tara.index import match_tara


def match_taradou(tokens):
    conditionals = set(match_tara(tokens))
    for i in range(len(tokens) - 1):
        conditional, dou = tokens[i:i + 2]
        if (tuple(conditional['span']) not in conditionals or not adjacent(conditional, dou)
                or dou['surface'] != 'どう' or dou['lemma'] != 'どう'
                or dou['pos'][:1] != ['副詞']):
            continue
        end = i + 2
        if end < len(tokens) and tokens[end]['surface'] == 'です' and adjacent(dou, tokens[end]):
            if (tokens[end]['pos'][:1] != ['助動詞'] or end + 1 >= len(tokens)
                    or tokens[end + 1]['surface'] != 'か'
                    or tokens[end + 1]['pos'][:2] != ['助詞', '終助詞']
                    or not adjacent(tokens[end], tokens[end + 1])):
                continue
            end += 2
        if end < len(tokens) and tokens[end]['pos'][:1] != ['補助記号']:
            continue
        yield conditional['span'][0], tokens[end - 1]['span'][1]


RULE = GrammarRule(order=24, id='taradou', expression=TARADOU_EXPRESSION,
                   reading='たらどう / たらどうですか',
                   meaning='why not; how about doing', match=match_taradou)

RULES = (RULE,)
