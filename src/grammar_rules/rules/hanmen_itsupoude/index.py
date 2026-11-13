"""Contrasting clauses joined by hanmen or ippou de."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


EXPRESSION = '〜反面 / 〜一方で'


def match_hanmen_itsupoude(tokens):
    for i in range(1, len(tokens)):
        previous, word = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (word['surface'] not in ('反面', '一方')
                or word['lemma'] != word['surface']
                or word['pos'][:1] != ['名詞']
                or not adjacent(previous, word)
                or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6
                or not pos[5].startswith(('連体形', '終止形'))):
            continue
        end = i + 1
        if word['surface'] == '一方':
            if (end >= len(tokens) or tokens[end]['surface'] != 'で'
                    or tokens[end]['pos'][:2] != ['助詞', '格助詞']
                    or not adjacent(word, tokens[end])):
                continue
            end += 1
        for following in tokens[end:]:
            if following['surface'] in ('。', '！', '？', '!', '?'):
                break
            if following['pos'][:1] != ['補助記号']:
                yield (word['span'][0], tokens[end - 1]['span'][1])
                break


RULE = GrammarRule(id='hanmen_itsupoude', expression=EXPRESSION,
                   reading='はんめん / いっぽうで',
                   meaning='while; whereas; on the other hand',
                   match=match_hanmen_itsupoude)
RULES = (RULE,)
