"""Shared stages for collecting and scoring registered grammar rules."""
from dataclasses import dataclass, field
from bisect import bisect_right

from src.GrammarShadow import aligned_tokens
from src.TokenizedSentence import TokenizedSentence
from src.PipelineStep import PipelineStep
from src.WordStats import Sentence, WordStats
from src.steps.ProcessingStep import ProcessingStep
from src.steps.TokenizeStep import is_valid_sentence, is_useless
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.registry import RULES
# Preserve existing imports for callers of the former single-module API.
from src.grammar_rules.rules.te_iru import match_te_iru
from src.grammar_rules.rules.nda import match_nda
from src.grammar_rules.rules.tai import match_tai
from src.grammar_rules.rules.te_shimau import match_te_shimau
from src.grammar_rules.rules.monono import match_monono


@dataclass
class GrammarOccurrences:
    count: int = 0
    tags: set = field(default_factory=set)
    examples: list = field(default_factory=list)
    example_texts: set = field(default_factory=set)
    index: float = 0.0
    index_count: int = 0


class GrammarStep(PipelineStep):
    def __init__(self, max_examples=3, rules=RULES, dictionary_path=None, prefer_expressions=False, max_lookahead=10):
        self._processing_step = ProcessingStep.GRAMMAR_SHADOW
        self.start_time = None
        self.max_examples = max_examples
        self.dictionary_path = dictionary_path
        self.prefer_expressions = prefer_expressions
        self.max_lookahead = max_lookahead
        self.rules = tuple(rules)
        if len({r.id for r in self.rules}) != len(self.rules) or len({r.expression for r in self.rules}) != len(self.rules):
            raise ValueError('Grammar rules must have unique identities and expressions')
        self.occurrences = {}

    def process(self, artifact):
        self.occurrences = {rule.id: GrammarOccurrences() for rule in self.rules}
        self.matches = {}
        self.spans = {}
        total = len(artifact.data)
        interval = max(1, min(2000, total // 100))
        for sentence_index, sentence in enumerate(artifact.data):
            if sentence_index % interval == 0:
                self.progress(sentence_index, total)
            tokens, error = aligned_tokens(sentence.text, sentence.tokens)
            if error:
                continue
            proposals = []
            for rule in self.rules:
                for start, end in rule.match(tokens):
                    if not 0 <= start < end <= len(sentence.text):
                        raise ValueError(f'Invalid source span for grammar rule {rule.id}')
                    proposals.append((rule, start, end))
            expression_spans = []
            if proposals and self.prefer_expressions:
                # Use the established dictionary merger as evidence of a complete
                # expression, before deciding which analysis owns the occurrence.
                from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict
                merged = combine_tokens_jmdict(sentence.tokens, get_jmdict(self.dictionary_path),
                                               max_lookahead=self.max_lookahead)
                aligned, merge_error = aligned_tokens(sentence.text, merged)
                if not merge_error:
                    expression_spans = [(t['span'][0], t['span'][1],
                                         merged[t['index']].get('dictionary_expression', False))
                                        for t in aligned
                                        if merged[t['index']].get('analysis_rule') == 'jmdict_surface_merge']
            for rule, start, end in proposals:
                # A copular な immediately before explanatory んだ is part of
                # the predicate, not interrogative 何だ. An exact JMdict
                # spelling such as なんだって cannot override this analysis.
                copular_starts = {t['span'][0] for t in tokens
                                   if rule.id == 'explanatory_nda'
                                   and t['span'][1] == start and t['surface'] == 'な'
                                   and t['lemma'] == 'だ' and t['pos'][:1] == ['助動詞']}
                # ておく can overlap a longer dictionary expression beginning
                # at おく, such as おけばいい. Keep that complete expression
                # available to the lexical merger instead of reserving its stem.
                connective_ends = {t['span'][1] for t in tokens
                                   if rule.id == 'te_oku' and t['span'][0] == start
                                   and t['surface'] in ('て', 'で')}
                if any((a <= start and end <= b and (a < start or end < b)
                        and a not in copular_starts)
                       or (is_expression and a in connective_ends and a < end < b)
                       for a, b, is_expression in expression_spans):
                    continue
                group = self.occurrences[rule.id]
                group.count += 1
                self.matches.setdefault(sentence_index, []).append((rule.id, start))
                self.spans.setdefault(sentence_index, []).append((rule.id, start, end))
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
        """Add collected cards before scoring; fail before any collisions."""
        active = [(r, self.occurrences[r.id]) for r in self.rules if self.occurrences[r.id].examples]
        collisions = [r.expression for r, _ in active if r.expression in words]
        if collisions:
            raise ValueError(f'Grammar identity conflicts with an existing candidate: {collisions[0]}')
        for rule, group in active:
            words[rule.expression] = WordStats(
                index=group.index if group.index_count else 1.0, index_count=group.index_count, frequency=group.count, score=0.0,
                reading=rule.reading, definition=rule.meaning,
                tags=set(group.tags), sentences=list(group.examples),
                lemma=rule.expression, pos=('grammar',), invalid=False,
                scoring_tag_count=len(group.tags),
            )


class ExcludeGrammarTokensStep(PipelineStep):
    """Reserve complete matched tokens for active grammar cards before merging."""
    def __init__(self, grammar):
        self.grammar = grammar
        self._processing_step = ProcessingStep.GRAMMAR_SHADOW
        self.start_time = None

    def process(self, artifact):
        active = {rule.id for rule in self.grammar.rules
                  if self.grammar.occurrences[rule.id].examples}
        result = []
        count = 0
        for index, sentence in enumerate(artifact.data):
            spans = [(start, end) for rule_id, start, end in self.grammar.spans.get(index, ())
                     if rule_id in active]
            if not spans:
                result.append(sentence)
                continue
            aligned, error = aligned_tokens(sentence.text, sentence.tokens)
            if error:
                result.append(sentence)
                continue
            boundaries = {point for token in aligned for point in token['span']}
            spans = [(start, end) for start, end in spans if start in boundaries and end in boundaries]
            reserved = {token['index'] for token in aligned
                        if any(start <= token['span'][0] and token['span'][1] <= end
                               for start, end in spans)}
            if reserved:
                tokens = [{**token, 'grammar_reserved': True} if i in reserved else token
                          for i, token in enumerate(sentence.tokens)]
                count += len(reserved)
                result.append(TokenizedSentence(sentence.text, sentence.tag, sentence.origin, tokens))
            else:
                result.append(sentence)
        artifact.data = result
        self.done(f'{count} grammar tokens reserved from lexical processing.')
        return artifact


class GrammarPositionStep(PipelineStep):
    """Map raw grammar spans onto eligible merged-token positions per source."""
    def __init__(self, grammar):
        self.grammar = grammar
        self._processing_step = ProcessingStep.GRAMMAR_SHADOW
        self.start_time = None

    def process(self, artifact):
        sentinel = object()
        origin = sentinel
        total = 0
        first_positions = {}

        def finish_source():
            if not total:
                return
            for rule_id, position in first_positions.items():
                group = self.grammar.occurrences[rule_id]
                group.index = (group.index * group.index_count + min(position, total) / total) / (group.index_count + 1)
                group.index_count += 1

        for sentence_index, sentence in enumerate(artifact.data):
            if origin is not sentinel and sentence.origin != origin:
                finish_source()
                total = 0
                first_positions = {}
            origin = sentence.origin
            matches = self.grammar.matches.get(sentence_index, ())
            aligned, error = aligned_tokens(sentence.text, sentence.tokens) if matches else (None, None)
            starts = []
            if matches and not error:
                for token in aligned:
                    raw = sentence.tokens[token['index']]
                    if (raw['base_form'] or raw['lemma']) and raw['reading'] and not is_useless(raw):
                        starts.append(token['span'][0])
                for rule_id, start in matches:
                    if rule_id not in first_positions:
                        first_positions[rule_id] = total + max(1, bisect_right(starts, start))
            total += sum(1 for token in sentence.tokens
                         if (token['base_form'] or token['lemma']) and token['reading'] and not is_useless(token))
        finish_source()
        self.done('Calculated per-source grammar positions from merged eligible tokens.')
        return artifact


class AddGrammarCardsStep(PipelineStep):
    def __init__(self, grammar):
        self.grammar = grammar
        self._processing_step = ProcessingStep.GRAMMAR_SHADOW
        self.start_time = None

    def process(self, artifact):
        self.grammar.add_cards(artifact.data)
        rules = {rule.id: rule for rule in self.grammar.rules}
        for rule in self.grammar.rules:
            if rule.expression in artifact.data:
                artifact.data[rule.expression].sentences = []
        for sentence in artifact.sentences:
            for rule_id, start, end in self.grammar.spans.get(sentence.source_index, ()):
                expression = rules[rule_id].expression
                if expression not in artifact.data:
                    continue
                surface = sentence.text[start:end]
                sentence.lemma_surfaces[expression] = surface
                sentence.lemma_spans[expression] = (start, end)
                sentence.lemma_occurrences.setdefault(expression, []).append((surface, (start, end), None))
        self.done('Added source-supported grammar cards for scoring.')
        return artifact
