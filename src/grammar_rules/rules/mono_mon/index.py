"""Sentence-final conversational explanation with もの or もん."""
MONO_MON_EXPRESSION = '〜もの / 〜もん'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_mono_mon(tokens):
    for i in range(1, len(tokens)):
        predicate, mono = tokens[i - 1:i + 1]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or mono['surface'] not in ('もの', 'もん')
                or mono['lemma'] != mono['surface']
                or mono['pos'][:1] != ['名詞'] and mono['pos'][:2] != ['助詞', '終助詞']
                or not adjacent(predicate, mono)):
            continue
        if i + 1 < len(tokens) and tokens[i + 1]['surface'] not in ('。', '！', '!', '？', '?', '」', '』'):
            continue
        yield mono['span'][0], mono['span'][1]


RULE = GrammarRule(order=73, id='mono_mon', expression=MONO_MON_EXPRESSION,
                   reading='もの / もん', meaning='because; since (conversational explanation or excuse)',
                   match=match_mono_mon)

RULES = (RULE,)
