"""Conversational recognition or appeal with じゃん and じゃないか."""
from src.GrammarConstants import JIYAN_JIYANAIKA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_jiyan_jiyanaika(tokens):
    for i in range(1, len(tokens)):
        previous, suffix = tokens[i - 1:i + 1]
        pos = previous['pos']
        if not adjacent(previous, suffix):
            continue
        if pos[:1] in (['動詞'], ['形容詞'], ['助動詞']):
            if len(pos) < 6 or not pos[5].startswith(('終止形', '連体形')):
                continue
        elif pos[:1] not in (['名詞'], ['代名詞'], ['形状詞'], ['副詞']):
            continue
        if (suffix['surface'] == 'じゃん' and suffix['lemma'] == 'じゃん'
                and suffix['pos'][:2] == ['助詞', '終助詞']):
            end = i
        elif (suffix['surface'] == 'じゃ' and suffix['lemma'] == 'だ'
              and suffix['pos'][:1] == ['助動詞'] and i + 2 < len(tokens)):
            negative, question = tokens[i + 1:i + 3]
            if (not adjacent(suffix, negative) or not adjacent(negative, question)
                    or negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                    or negative['pos'][:1] != ['形容詞']
                    or question['surface'] != 'か' or question['lemma'] != 'か'
                    or question['pos'][:2] != ['助詞', '終助詞']):
                continue
            end = i + 2
        else:
            continue
        yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='jiyan_jiyanaika', expression=JIYAN_JIYANAIKA_EXPRESSION,
                   reading='じゃん / じゃないか',
                   meaning='recognition, emphasis, or appeal to shared understanding',
                   match=match_jiyan_jiyanaika)
