"""Obligation expressed by inability to leave an action undone."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.wakenihaikanai.index import match_ending


def match_naiwakenihaikanai(tokens):
    for i in range(2, len(tokens)):
        verb, negative, wake = tokens[i - 2:i + 1]
        pos = verb['pos']
        if (len(pos) < 6 or not pos[5].startswith('未然形')
                or not (pos[:1] == ['動詞'] or
                        (pos[:1] == ['助動詞'] and verb['lemma'] in ('れる', 'られる', 'せる', 'させる')))
                or negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] != ['助動詞']
                or wake['surface'] not in ('わけ', '訳') or wake['lemma'] not in ('わけ', '訳')
                or wake['pos'][:2] != ['名詞', '普通名詞']
                or not adjacent(verb, negative) or not adjacent(negative, wake)):
            continue
        for _, end in match_ending(tokens, i):
            yield negative['span'][0], end


RULE = GrammarRule(order=32, id='naiwakenihaikanai', expression='〜ないわけにはいかない',
                   reading='ないわけにはいかない', meaning='cannot avoid doing; have to do',
                   match=match_naiwakenihaikanai)
RULES = (RULE,)
