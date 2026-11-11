from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.SegmentedSentence import SegmentedSentence
from src.TokenizedSentence import TokenizedSentence
from src.TokenProvenance import token_components
from src.WordStats import WordStats
from src.steps.ProcessingStep.index import ProcessingStep
from src.steps.TokenizeStep.index import is_useless, kata_to_hira
from dataclasses import replace


PROGRESS_INTERVAL = 200


class PotentialFormResolver:
    """Resolve analyzer-supported potentials only when no independent entry exists."""

    def __init__(self, dictionary, tokenizer):
        self.dictionary = dictionary
        self.tokenizer = tokenizer
        self.cache = {}

    def resolve(self, token, lemma):
        if not token.get("pos") or token["pos"][0] != "\u52d5\u8a5e" or token.get("components"):
            return None
        if lemma not in self.cache:
            self.cache[lemma] = self._resolve(lemma)
        return self.cache[lemma]

    def _resolve(self, lemma):
        if not lemma.endswith("\u308b"):
            return None
        entries = self.dictionary.lookup_word(lemma)
        glosses = [gloss.lower() for entry in entries for sense in entry["senses"]
                   for gloss in sense["gloss"]]
        if entries and (not glosses or any(not gloss.startswith("to be able to ") for gloss in glosses)):
            return None
        from src.steps.TokenizeStep.index import TOKENIZER_MODE, get_lemma_reading
        nodes = self.tokenizer.tokenize(lemma, TOKENIZER_MODE)
        if len(nodes) != 1 or nodes[0].dictionary_form() != lemma:
            return None
        base = nodes[0].normalized_form()
        endings = {"\u3046": "\u3048\u308b", "\u304f": "\u3051\u308b", "\u3050": "\u3052\u308b", "\u3059": "\u305b\u308b", "\u3064": "\u3066\u308b", "\u306c": "\u306d\u308b", "\u3076": "\u3079\u308b", "\u3080": "\u3081\u308b", "\u308b": "\u308c\u308b"}
        if not base or base[-1] not in endings or base[:-1] + endings[base[-1]] != lemma:
            return None
        entries = self.dictionary.lookup_word(base)
        if not entries:
            return None
        return base, get_lemma_reading(base)


class BuildWordStatsStep(PipelineStep):
    """Build word statistics and sentence mappings from merged token streams."""

    def __init__(self, merge_potential_forms=False, dictionary_path=None):
        self.merge_potential_forms = merge_potential_forms
        self.dictionary_path = dictionary_path
        self._processing_step = ProcessingStep.WORD_COLLECTION

    def process(self, artifact: Artifact) -> Artifact:
        resolver = None
        if self.merge_potential_forms:
            from src.steps.MergeTokensStep.index import get_jmdict
            from src.steps.TokenizeStep.index import get_tokenizer
            resolver = PotentialFormResolver(get_jmdict(self.dictionary_path), get_tokenizer())
        word_data, segmented_sentences = build_word_stats(
            artifact.data,
            potential_resolver=resolver,
            progress_handler=self.progress,
            progress_interval=PROGRESS_INTERVAL,
        )
        self.done(
            f"{len(word_data)} tokens and {len(segmented_sentences)} sentences collected."
        )
        return Artifact(word_data, sentences=segmented_sentences)


def build_word_stats(
    tokenized_sentences: list[TokenizedSentence],
    progress_handler=None,
    word_data=None,
    segmented_sentences=None,
    progress_interval: int = 200,
    potential_resolver=None,
):
    """Aggregate merged tokens while retaining per-source index positions."""
    word_data = word_data or {}
    segmented_out = list(segmented_sentences) if segmented_sentences else []
    current_origin = None
    lemma_first_pos_in_file: dict[str, int] = {}
    token_index = 0
    reassigned_occurrences = {}
    usefulness_cache = {}

    def update_source_indexes():
        if token_index == 0:
            return
        for lemma, first_pos in lemma_first_pos_in_file.items():
            ws = word_data[lemma]
            file_norm_pos = first_pos / token_index
            ws.index = (ws.index * ws.index_count + file_norm_pos) / (ws.index_count + 1)
            ws.index_count += 1

    for index, tokenized_sentence in enumerate(tokenized_sentences, 1):
        if current_origin is not None and tokenized_sentence.origin != current_origin:
            update_source_indexes()
            lemma_first_pos_in_file = {}
            token_index = 0
        current_origin = tokenized_sentence.origin

        sentence_surfaces = {}
        sentence_spans = {}
        sentence_components = {}
        sentence_occurrences = {}
        cursor = 0
        for token_position, token in enumerate(tokenized_sentence.tokens):
            surface = token["surface"]
            # Align in order, allowing whitespace gaps but never guessing after failure.
            start = tokenized_sentence.text.find(surface, cursor) if cursor is not None and surface else -1
            if not surface:
                span = (-1, -1)
            elif start >= 0 and not tokenized_sentence.text[cursor:start].strip():
                span = (start, start + len(surface))
                cursor = span[1]
            else:
                span = (-1, -1)
                cursor = None
            previous_lemma = token.get("reassigned_from")
            if previous_lemma:
                count, tags = reassigned_occurrences.setdefault(previous_lemma, [0, set()])
                reassigned_occurrences[previous_lemma][0] = count + 1
                if tokenized_sentence.tag:
                    tags.add(tokenized_sentence.tag)
            lemma = token["base_form"] or token["lemma"]
            reading = token["reading"]
            if not lemma or not reading or token.get("grammar_reserved"):
                continue
            # Eligibility depends on these fields, not on source position or surface.
            key = (lemma, tuple(token["pos"]), token.get("analysis_rule"))
            useful = usefulness_cache.get(key)
            if useful is None:
                useful = not is_useless(token)
                usefulness_cache[key] = useful
            if not useful:
                continue
            if potential_resolver:
                resolved = potential_resolver.resolve(token, lemma)
                if resolved:
                    lemma, reading = resolved

            token_index += 1
            lemma_first_pos_in_file.setdefault(lemma, token_index)
            sentence_surfaces[lemma] = token["surface"]
            sentence_spans[lemma] = span

            ws = word_data.get(lemma)
            if ws:
                ws.frequency += 1
            else:
                ws = WordStats(
                    0.0, 0, 1, 0.0, kata_to_hira(reading), "", set(), [],
                    lemma, token["pos"], invalid=False,
                )
                word_data[lemma] = ws

            if token.get("dictionary_expression"):
                ws.dictionary_expression_occurrences += 1

            components = token_components(token)
            # A sentence-initial しかし followed by a comma is strong evidence
            # for the discourse conjunction. Keep this source cue separate
            # from the original Sudachi POS and do not infer it on failed spans.
            if (lemma == surface == 'しかし' and len(components) == 1
                    and components[0].pos and components[0].pos[0] == '接続詞'
                    and span[0] >= 0
                    and not tokenized_sentence.text[:span[0]].strip(' 「『（(\t')
                    and tokenized_sentence.text[span[1]:].startswith('、')):
                components = (replace(components[0], context='initial_shikashi_comma'),)
            # Initial んだ can be an interjection even when Sudachi splits
            # it into a nominalizer and copula; do not treat it as explanatory.
            if (lemma == surface == 'んだ' and len(components) == 2
                    and span[0] >= 0
                    and not tokenized_sentence.text[:span[0]].strip(' 「『（(\t')):
                components = (replace(components[0], context='initial_nda'), components[1])
            # Predicate + はず + copula or sentence ending supports expectation,
            # unlike nominal uses such as 矢のはず. Preserve this per occurrence.
            if (lemma == surface == 'はず' and len(components) == 1
                    and components[0].pos and components[0].pos[0] == '名詞'
                    and span[0] >= 0 and token_position > 0):
                previous = tokenized_sentence.tokens[token_position - 1]
                following = (tokenized_sentence.tokens[token_position + 1]
                             if token_position + 1 < len(tokenized_sentence.tokens) else None)
                previous_pos = previous.get('pos', ())
                next_pos = following.get('pos', ()) if following else ()
                predicate = (previous_pos and (previous_pos[0] in {'動詞', '形容詞'}
                             or (previous_pos[0] == '助動詞' and previous.get('lemma') in {'た', 'ない', 'だ'})))
                ending = (following is None or (next_pos and next_pos[0] == '補助記号')
                          or (next_pos and next_pos[0] == '助動詞'
                              and following.get('lemma') in {'だ', 'です', 'じゃ'}))
                previous_surface = previous.get('surface', '')
                following_surface = following.get('surface', '') if following else ''
                source_aligned = (bool(previous_surface)
                                  and tokenized_sentence.text[:span[0]].rstrip().endswith(previous_surface)
                                  and (not following or bool(following_surface) and
                                       tokenized_sentence.text[span[1]:].lstrip().startswith(following_surface)))
                if predicate and ending and source_aligned:
                    components = (replace(components[0], context='predicate_hazu_expectation'),)
            # A lexical できた before 人 has source support for its
            # prenominal "fine character" sense. Other noun heads can be
            # ordinary past predicates and must not share its examples.
            if (lemma in {'できた', '出来た'} and len(components) == 2
                    and span[0] >= 0 and token_position + 1 < len(tokenized_sentence.tokens)):
                following = tokenized_sentence.tokens[token_position + 1]
                if (following.get('surface') == '人' and following.get('pos', ())[0] == '名詞'
                        and tokenized_sentence.text[span[1]:].lstrip().startswith('人')):
                    components = (replace(components[0], context='dekita_person'), components[1])
                elif (following.get('surface') in {'！', '!'}
                      and not tokenized_sentence.text[:span[0]].strip(' 「『（(\t')
                      and tokenized_sentence.text[span[1]:].lstrip().startswith(following['surface'])):
                    components = (replace(components[0], context='dekita_exclamation'), components[1])
            sentence_components[lemma] = components
            sentence_occurrences.setdefault(lemma, []).append((surface, span, components))
            ws.component_counts[components] = ws.component_counts.get(components, 0) + 1

            if tokenized_sentence.tag:
                ws.tags.add(tokenized_sentence.tag)

        # Keep every source line for low-quality fallback. Attachment ranks
        # lines passing the normal sentence checks ahead of these alternatives.
        segmented_out.append(
            SegmentedSentence(
                text=tokenized_sentence.text,
                tag=tokenized_sentence.tag,
                origin=tokenized_sentence.origin,
                lemma_surfaces=sentence_surfaces,
                lemma_spans=sentence_spans,
                lemma_components=sentence_components,
                lemma_occurrences=sentence_occurrences,
                source_index=index - 1,
            )
        )

        if progress_handler and index % progress_interval == 0 and index < len(tokenized_sentences):
            progress_handler(index, len(tokenized_sentences))

    update_source_indexes()
    for lemma, (count, tags) in reassigned_occurrences.items():
        if lemma in word_data:
            word_data[lemma].reassigned_occurrences += count
            word_data[lemma].reassigned_tags.update(tags)
    return word_data, segmented_out
