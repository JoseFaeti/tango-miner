"""Literary noun comparisons with のごとく."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_gotoku(tokens):
    for i in range(1, len(tokens) - 1):
        previous, no, gotoku = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['名詞'], ['代名詞'])
                and no['surface'] == no['lemma'] == 'の'
                and no['pos'][:2] == ['助詞', '格助詞']
                and (gotoku['surface'], gotoku['lemma']) in (
                    ('ごとく', 'ごとし'), ('如く', '如し'))
                and gotoku['pos'][:1] == ['助動詞']
                and adjacent(previous, no) and adjacent(no, gotoku)):
            yield no['span'][0], gotoku['span'][1]


RULE = GrammarRule(order=138, id='gotoku', expression='〜ごとく',
                   reading='ごとく', meaning='like; as if (literary)', match=match_gotoku)
RULES = (RULE,)
