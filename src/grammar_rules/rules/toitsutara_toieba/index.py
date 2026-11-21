"""Topic association or exclamation with といったら / といえば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toitsutara_toieba(tokens):
    for i in range(1, len(tokens) - 2):
        topic, particle, verb, ending = tokens[i - 1:i + 3]
        if (topic['pos'][:1] not in (['名詞'], ['代名詞'], ['形容詞'], ['動詞'])
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or not ((verb['surface'] in ('いっ', '言っ') and ending['surface'] == 'たら'
                         and ending['lemma'] == 'た')
                        or (verb['surface'] in ('いえ', '言え') and ending['surface'] == 'ば'
                            and ending['pos'][:1] == ['助詞']))
                or not all(adjacent(a, b) for a, b in zip((topic, particle, verb), (particle, verb, ending)))):
            continue
        # The longer extreme-degree construction has its own card.
        if (ending['surface'] == 'たら' and i + 3 < len(tokens)
                and adjacent(ending, tokens[i + 3])
                and tokens[i + 3]['lemma'] == 'ない'
                and tokens[i + 3]['surface'] in ('ない', 'なかっ')):
            continue
        yield particle['span'][0], ending['span'][1]


RULE = GrammarRule(id='toitsutara_toieba', expression='〜といったら / 〜といえば',
                   reading='といったら / といえば', meaning='speaking of; as for; how very',
                   match=match_toitsutara_toieba, order=30)
RULES = (RULE,)
