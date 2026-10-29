"""Past experience expressed by a verb + たことがある."""
from src.GrammarConstants import KOTO_GA_ARU_EXPERIENCE
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


RULE = GrammarRule(
    id='koto_ga_aru_experience', expression=KOTO_GA_ARU_EXPERIENCE,
    reading='たことがある',
    meaning=('Grammar: verb past form + ことがある describes having experienced an action; '
             'negative forms such as ことがない describe never having experienced it.'),
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
        if ((koto['surface'] == 'ことがある' and koto['lemma'] == 'ことがある')
                or (koto['surface'] == 'ことがない' and koto['lemma'] == 'ことがない'
                    and koto['pos'][:1] == ['形容詞'])):
            end = i
        elif (koto['surface'] == 'こと' and koto['pos'][:1] == ['名詞']
              and next_matches(tokens, i, 'が', '助詞', 'が')
              and i + 2 < len(tokens) and adjacent(tokens[i + 1], tokens[i + 2])):
            end = positive_verb_end(tokens, i + 2, lemma='ある',
                                    base_forms={'ある'}, past_stems={'あっ'},
                                    polite_stems={'あり'})
            if end is None:
                end = negative_predicate_end(tokens, i + 2, polite_lemma='ある',
                                             polite_stems={'あり'})
            if end is None:
                continue
        else:
            continue
        # In the observed source, this names what was realized after someone
        # disappeared. Other uses of わかる can describe an experience.
        if (verb['lemma'] == 'わかる' and i >= 5
                and [t['surface'] for t in tokens[i - 5:i - 2]] == ['なく', 'なっ', 'て']
                and all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 5, i - 2))
                and tokens[end]['surface'] == 'た'):
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
