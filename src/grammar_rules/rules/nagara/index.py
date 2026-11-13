"""Verb continuative + nagara, including its ambiguous concessive reading."""
NAGARA_GRAMMAR_EXPRESSION = '〜ながら'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nagara(tokens):
    for i in range(1, len(tokens) - 1):
        verb, particle, following = tokens[i - 1:i + 2]
        if particle['surface'] != 'ながら':
            continue
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith('連用形')
                or not adjacent(verb, particle)
                or particle['surface'] != 'ながら' or particle['lemma'] != 'ながら'
                or particle['pos'][:2] != ['助詞', '接続助詞']
                or following['surface'] == 'の'):
            continue
        if (following['surface'] == 'も' and following['lemma'] == 'も'
                and following['pos'][:2] == ['助詞', '係助詞']
                and adjacent(particle, following)):
            from src.grammar_rules.rules.nagaramo.index import match_nagaramo
            if any(start == particle['span'][0] for start, _ in match_nagaramo(tokens)):
                continue
        # Do not infer a following clause across sentence-ending punctuation.
        clause = []
        for token in tokens[i + 1:]:
            if token['surface'] in ('。', '！', '？', '!', '?'):
                break
            clause.append(token)
        if not any(t['pos'][:1] != ['補助記号'] for t in clause):
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=129,
    id='nagara', expression=NAGARA_GRAMMAR_EXPRESSION, reading='ながら',
    meaning='while doing; although (depending on context)', match=match_nagara,
)

RULES = (RULE,)
