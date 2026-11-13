"""Selected colloquial predicate-attached concessive forms."""
from src.GrammarConstants import TATSUTE_DATSUTE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tatsute_datsute(tokens):
    for i in range(1, len(tokens)):
        anchor, suffix = tokens[i - 1:i + 1]
        pos = anchor['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith('連用形')
                or not adjacent(anchor, suffix)):
            continue
        end = i
        if suffix['surface'] == 'たって' and suffix['pos'][:2] == ['助詞', '接続助詞']:
            pass
        elif (suffix['surface'] == 'だ' and suffix['pos'][:1] == ['助動詞']
              and len(suffix['pos']) >= 5 and suffix['pos'][4] == '助動詞-タ'
              and i + 1 < len(tokens) and adjacent(suffix, tokens[i + 1])
              and tokens[i + 1]['surface'] == 'って'
              and tokens[i + 1]['pos'][:1] == ['助詞']):
            end += 1
        else:
            continue
        following = tokens[end + 1:]
        if not following or following[0]['pos'][:1] == ['補助記号']:
            continue
        # A past-tense quotation followed by a speech verb is not concession.
        if following[0]['lemma'] in ('言う', 'いう'):
            continue
        yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tatsute_datsute', expression=TATSUTE_DATSUTE_EXPRESSION,
                   reading='たって / だって', meaning='even if; even though',
                   match=match_tatsute_datsute)
