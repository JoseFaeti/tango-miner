"""Strong denial with ものか or colloquial もんか."""
MONOKA_MONKA_EXPRESSION = '〜ものか / 〜もんか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_monoka_monka(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, mono, ka = tokens[i - 1:i + 2]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or mono['surface'] not in ('もの', 'もん')
                or mono['lemma'] != mono['surface'] or mono['pos'][:1] != ['名詞']
                or ka['surface'] != 'か' or ka['lemma'] != 'か'
                or ka['pos'][:2] != ['助詞', '終助詞']
                or not adjacent(predicate, mono) or not adjacent(mono, ka)):
            continue
        # An alternative introduced by あるいは and ending in an explicit
        # question about an ongoing state can be ordinary speculation.
        if (mono['surface'] == 'もの' and predicate['lemma'] == 'いる'
                and i + 2 < len(tokens) and tokens[i + 2]['surface'] in ('？', '?')):
            for token in reversed(tokens[:i - 1]):
                if token['surface'] in ('。', '！', '!', '？', '?'):
                    break
                if token['surface'] == 'あるいは':
                    break
            else:
                token = None
            if token is not None and token['surface'] == 'あるいは':
                continue
        yield mono['span'][0], ka['span'][1]


RULE = GrammarRule(order=71, id='monoka_monka', expression=MONOKA_MONKA_EXPRESSION,
                   reading='ものか / もんか', meaning='never; absolutely not',
                   match=match_monoka_monka)

RULES = (RULE,)
