"""Indirect instructions with ように言う or 頼む."""
from src.GrammarConstants import YOU_NI_IU_TANOMU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_you_ni_iu_tanomu(tokens):
    for i in range(1, len(tokens)):
        predicate, you = tokens[i - 1:i + 1]
        pos = predicate['pos']
        if not adjacent(predicate, you) or len(pos) < 6:
            continue
        if pos[:1] == ['動詞'] and pos[5].startswith(('終止形', '連体形')):
            pass
        elif (predicate['surface'] == 'ない' and predicate['lemma'] == 'ない'
              and pos[:1] == ['助動詞'] and pos[5].startswith(('終止形', '連体形'))
              and i >= 2 and adjacent(tokens[i - 2], predicate)
              and tokens[i - 2]['pos'][:1] == ['動詞']
              and len(tokens[i - 2]['pos']) >= 6
              and tokens[i - 2]['pos'][5].startswith('未然形')):
            pass
        else:
            continue
        if you['surface'] == 'ように' and you['lemma'] == 'ように':
            ni = i
        elif (you['surface'] == 'よう' and you['lemma'] == 'よう'
              and you['pos'][:1] == ['形状詞']
              and next_matches(tokens, i, 'だ', '助動詞', 'に')):
            ni = i + 1
        else:
            continue
        if ni + 1 >= len(tokens) or not adjacent(tokens[ni], tokens[ni + 1]):
            continue
        verb = tokens[ni + 1]
        forms = {
            '言う': ({'言う', 'いう'}, {'言っ', 'いっ'}, {'言い', 'いい'}, {'言わ', 'いわ'}),
            '頼む': ({'頼む', 'たのむ'}, {'頼ん', 'たのん'}, {'頼み', 'たのみ'}, {'頼ま', 'たのま'}),
        }
        if verb['lemma'] not in forms or verb['pos'][:1] != ['動詞']:
            continue
        base, past, polite, negative = forms[verb['lemma']]
        end = positive_verb_end(tokens, ni + 1, lemma=verb['lemma'], base_forms=base,
                                past_stems=past, polite_stems=polite)
        if (end is None and verb['lemma'] == '頼む' and verb['surface'] in past
                and next_matches(tokens, ni + 1, 'だ', '助動詞', 'だ')):
            end = ni + 2
        if end is None:
            end = negative_predicate_end(tokens, ni + 1, plain_lemma=verb['lemma'],
                                         plain_stems=negative, polite_lemma=verb['lemma'],
                                         polite_stems=polite)
        if end is None and verb['surface'] in past:
            particle = 'で' if verb['lemma'] == '頼む' else 'て'
            if next_matches(tokens, ni + 1, particle, '助詞', particle):
                end = ni + 2
        if end is not None:
            yield you['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='you_ni_iu_tanomu', expression=YOU_NI_IU_TANOMU_EXPRESSION,
                   reading='ようにいう・たのむ', meaning='tell or ask someone to do',
                   match=match_you_ni_iu_tanomu)
