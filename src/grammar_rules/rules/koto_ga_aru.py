"""Past experience expressed by a verb + たことがある."""
from src.GrammarConstants import KOTO_GA_ARU_EXPERIENCE
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


RULE = GrammarRule(
    id='koto_ga_aru_experience', expression=KOTO_GA_ARU_EXPERIENCE,
    reading='たことがある',
    meaning='Grammar: verb past form + ことがある describes having experienced an action.',
    match=lambda tokens: match_experience(tokens),
)


def match_experience(tokens):
    for i in range(2, len(tokens)):
        verb, past, koto = tokens[i - 2:i + 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith('連用形')
                or (past['surface'], past['lemma']) not in (('た', 'た'), ('だ', 'だ'))
                or past['pos'][:1] != ['助動詞']
                or not adjacent(verb, past) or not adjacent(past, koto)):
            continue
        if koto['surface'] == 'ことがある' and koto['lemma'] == 'ことがある':
            end = i
        elif (koto['surface'] == 'こと' and koto['pos'][:1] == ['名詞']
              and next_matches(tokens, i, 'が', '助詞', 'が')
              and next_matches(tokens, i + 1, 'ある', '動詞')):
            end = i + 2
            if tokens[end]['surface'] == 'あり':
                if not next_matches(tokens, end, 'ます', '助動詞', 'ます'):
                    continue
                end += 1
            elif tokens[end]['surface'] != 'ある':
                continue
        else:
            continue
        # The observed sentence means there is something left unsaid. Do not
        # generalize to every 忘れたこと: past experience is also possible.
        if (verb['lemma'] == '忘れる' and i >= 3
                and tokens[i - 3]['surface'] == '言い'
                and tokens[i - 3]['lemma'] == '言う'
                and adjacent(tokens[i - 3], verb)
                and end + 1 < len(tokens) and adjacent(tokens[end], tokens[end + 1])
                and tokens[end + 1]['surface'] == 'わ'
                and tokens[end + 1]['pos'][:1] == ['助詞']):
            continue
        # In the observed conditional, 困ったこと names a problem. Keep it
        # available to the lexical merger. A preceding に can instead mark
        # what caused trouble, supporting an experience reading.
        if (verb['lemma'] == '困る' and verb['surface'] == '困っ'
                and next_matches(tokens, end, 'だ', '助動詞', 'なら')
                and not (i >= 3 and tokens[i - 3]['surface'] == 'に'
                         and tokens[i - 3]['pos'][:1] == ['助詞']
                         and adjacent(tokens[i - 3], verb))):
            continue
        yield koto['span'][0], tokens[end]['span'][1]
