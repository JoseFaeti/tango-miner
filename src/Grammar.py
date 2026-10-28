"""One grammar stage that collects occurrences for registered construction rules."""
from dataclasses import dataclass, field

from src.GrammarConstants import GRAMMAR_EXPRESSIONS, TE_IRU_GRAMMAR_EXPRESSION
from src.GrammarShadow import aligned_tokens
from src.PipelineStep import PipelineStep
from src.WordStats import Sentence, WordStats
from src.steps.ProcessingStep import ProcessingStep
from src.steps.TokenizeStep import is_valid_sentence


def match_te_iru(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, iru = tokens[i - 1:i + 2]
        if (previous['span'][1] == particle['span'][0]
                and particle['span'][1] == iru['span'][0]
                and previous['pos'][:1] in (['動詞'], ['助動詞'])
                and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')
                and particle['surface'] in ('て', 'で')
                and particle['pos'][:2] == ['助詞', '接続助詞']
                and iru['surface'] == 'いる' and iru['lemma'] in ('いる', '居る')
                and iru['pos'][:2] == ['動詞', '非自立可能']):
            yield particle['span'][0], iru['span'][1]


@dataclass(frozen=True)
class GrammarRule:
    id: str
    expression: str
    reading: str
    meaning: str
    match: object


RULES = (GrammarRule(
    id='te_iru', expression=TE_IRU_GRAMMAR_EXPRESSION, reading='ている',
    meaning=('Grammar: verb て-form + いる. Can describe an ongoing action, '
             'a resulting state, or a habitual action, depending on context.'),
    match=match_te_iru,
),)

if frozenset(rule.expression for rule in RULES) != GRAMMAR_EXPRESSIONS:
    raise ValueError('Registered grammar identities and Anki protections must match')


@dataclass
class GrammarOccurrences:
    count: int = 0
    tags: set = field(default_factory=set)
    examples: list = field(default_factory=list)
    example_texts: set = field(default_factory=set)


class GrammarStep(PipelineStep):
    def __init__(self, max_examples=3, rules=RULES):
        self._processing_step = ProcessingStep.GRAMMAR_SHADOW
        self.start_time = None
        self.max_examples = max_examples
        self.rules = tuple(rules)
        if len({r.id for r in self.rules}) != len(self.rules) or len({r.expression for r in self.rules}) != len(self.rules):
            raise ValueError('Grammar rules must have unique identities and expressions')
        self.occurrences = {}

    def process(self, artifact):
        self.occurrences = {rule.id: GrammarOccurrences() for rule in self.rules}
        for sentence in artifact.data:
            tokens, error = aligned_tokens(sentence.text, sentence.tokens)
            if error:
                continue
            for rule in self.rules:
                group = self.occurrences[rule.id]
                for start, end in rule.match(tokens):
                    if not 0 <= start < end <= len(sentence.text):
                        raise ValueError(f'Invalid source span for grammar rule {rule.id}')
                    group.count += 1
                    if sentence.tag:
                        group.tags.add(sentence.tag)
                    if (len(group.examples) < self.max_examples and sentence.text not in group.example_texts
                            and is_valid_sentence(sentence.text)):
                        group.examples.append(Sentence(sentence.text, sentence.tag or '', str(sentence.origin or ''),
                                                       sentence.text[start:end], 0.0, (start, end)))
                        group.example_texts.add(sentence.text)
        self.done(f'{sum(g.count for g in self.occurrences.values())} source-supported grammar occurrences.')
        return artifact

    def add_cards(self, words):
        """Add collected cards after lexical scoring; fail before any collisions."""
        active = [(r, self.occurrences[r.id]) for r in self.rules if self.occurrences[r.id].examples]
        collisions = [r.expression for r, _ in active if r.expression in words]
        if collisions:
            raise ValueError(f'Grammar identity conflicts with an existing candidate: {collisions[0]}')
        for rule, group in active:
            words[rule.expression] = WordStats(
                index=0.0, index_count=0, frequency=group.count, score=0.0,
                reading=rule.reading, definition=rule.meaning,
                tags=group.tags | {'grammar'}, sentences=list(group.examples),
                lemma=rule.expression, pos=('grammar',), invalid=False,
            )
