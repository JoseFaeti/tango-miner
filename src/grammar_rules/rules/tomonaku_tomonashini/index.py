"""Verb + ともなく / ともなしに: without intending to."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tomonaku_tomonashini(tokens):
    for i in range(1, len(tokens)):
        verb = tokens[i - 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith('終止形')
                or not adjacent(verb, tokens[i])):
            continue
        j = i
        if tokens[j]['surface'] == 'とも' and tokens[j]['pos'][:1] == ['助詞']:
            j += 1
        elif (j + 1 < len(tokens) and tokens[j]['surface'] == 'と'
              and tokens[j]['pos'][:1] == ['助詞']
              and tokens[j + 1]['surface'] == 'も'
              and tokens[j + 1]['pos'][:1] == ['助詞']
              and adjacent(tokens[j], tokens[j + 1])):
            j += 2
        else:
            continue
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        tail = tokens[j]
        if tail['surface'] == 'なく' and tail['lemma'] == 'ない' and tail['pos'][:1] == ['形容詞']:
            yield tokens[i]['span'][0], tail['span'][1]
        elif (tail['surface'] == 'なし' and tail['pos'][:1] == ['名詞']
              and j + 1 < len(tokens) and adjacent(tail, tokens[j + 1])
              and tokens[j + 1]['surface'] == 'に' and tokens[j + 1]['pos'][:1] == ['助詞']):
            yield tokens[i]['span'][0], tokens[j + 1]['span'][1]


RULE = GrammarRule(id='tomonaku_tomonashini', expression='〜ともなく / 〜ともなしに',
                   reading='ともなく / ともなしに', meaning='without intending to',
                   match=match_tomonaku_tomonashini, order=1)
RULES = (RULE,)
