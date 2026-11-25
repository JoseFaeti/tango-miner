"""Conservative recovery of dictionary candidates with an identical grammar gloss."""
import re


def _glosses(value):
    return frozenset(
        re.sub(r'^\d+\.\s*', '', part.strip()).casefold()
        for sense in value.split('<br>') for part in sense.split(';')
        if part.strip()
    )


def recover_dictionary_candidates(words, sentences, rules):
    equivalents = {}
    for rule in rules:
        for value in (rule.expression, rule.reading):
            for form in value.split(' / '):
                key = (form.strip().lstrip('〜～~'), _glosses(rule.meaning))
                equivalents.setdefault(key, {})[rule.id] = rule
    recovered = {}
    for word, stats in list(words.items()):
        if not stats.jmdict_definition or not stats.definition or stats.invalid:
            continue
        matches = equivalents.get((word, _glosses(stats.definition)), {})
        if len(matches) != 1:
            continue
        rule = next(iter(matches.values()))
        if word == rule.expression:
            continue
        target = words.get(rule.expression)
        if target is None:
            words[rule.expression] = stats
            stats.lemma = rule.expression
            stats.reading = rule.reading
            stats.definition = rule.meaning
            stats.pos = ('grammar',)
        else:
            count = target.index_count + stats.index_count
            if count:
                target.index = (target.index * target.index_count + stats.index * stats.index_count) / count
            target.index_count = count
            target.frequency += stats.frequency
            target.tags.update(stats.tags)
            target.scoring_tag_count = len(target.tags)
            for components, frequency in stats.component_counts.items():
                target.component_counts[components] = target.component_counts.get(components, 0) + frequency
        recovered[word] = rule.expression
        del words[word]
    for sentence in sentences:
        for word in recovered.keys() & sentence.lemma_surfaces.keys():
            expression = recovered[word]
            sentence.lemma_surfaces[expression] = sentence.lemma_surfaces.pop(word)
            for mapping in (sentence.lemma_spans, sentence.lemma_components):
                if word in mapping:
                    mapping[expression] = mapping.pop(word)
            occurrences = sentence.lemma_occurrences.pop(word, None)
            if occurrences is not None:
                sentence.lemma_occurrences.setdefault(expression, []).extend(occurrences)
