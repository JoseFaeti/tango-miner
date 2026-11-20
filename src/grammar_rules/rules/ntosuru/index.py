"""Literary verbal irrealis + んとする: intend or be about to."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_ntosuru(tokens):
    for i in range(len(tokens) - 2):
        head = tokens[i]
        pos = head['pos']
        if (head['surface'] == 'ん' and head['lemma'] in ('む', 'ぬ')
                and pos[:1] == ['助動詞'] and i > 0):
            verb = tokens[i - 1]
            vpos = verb['pos']
            if not (adjacent(verb, head) and len(vpos) >= 6
                    and (vpos[0] == '動詞' or (vpos[0] == '助動詞'
                         and verb['lemma'] in ('れる', 'られる', 'せる', 'させる')))
                    and (vpos[5].startswith('未然形') or
                         (vpos[5].startswith('連用形') and
                          (vpos[4].startswith(('上一段', '下一段')) or verb['lemma'] == '来る')))):
                continue
            start = head['span'][0]
        elif (len(pos) >= 6 and pos[0] == '動詞' and head['surface'].endswith('ん')
              and pos[5].startswith('連体形-撥音便')):
            start = head['span'][1] - 1
        else:
            continue
        if not (next_matches(tokens, i, 'と', '助詞', 'と')
                and next_matches(tokens, i + 1, 'する', '動詞')):
            continue
        j = i + 2
        end = positive_verb_end(tokens, j, lemma='する', base_forms={'する'},
                                past_stems={'し'}, polite_stems={'し'}, te_stems={'し'},
                                conditional_stems={'すれ'})
        if end is None:
            end = negative_predicate_end(tokens, j, plain_lemma='する', plain_stems={'し'},
                                         polite_lemma='する', polite_stems={'し'})
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(id='ntosuru', expression='〜んとする', reading='んとする',
                   meaning='intend to; be about to (literary)', match=match_ntosuru, order=95)
RULES = (RULE,)
