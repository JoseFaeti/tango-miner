"""Inspect grammar proposals, dictionary merges and reservations for source sentences."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.steps.MergeTokensStep.index import combine_tokens_jmdict, get_jmdict
from src.steps.TokenizeStep.index import tokenize_raw_sentences


def inspect(texts, dictionary_path=None):
    dictionary = get_jmdict(dictionary_path)
    sentences = tokenize_raw_sentences(texts, tag='inspection', source_path=Path('inspection.txt'))
    stage = GrammarStep(dictionary_path=dictionary_path, prefer_expressions=True)
    artifact = stage.process(Artifact(sentences))
    # Reservations depend on the existence of an eligible example for each card.
    reserved = ExcludeGrammarTokensStep(stage).process(artifact).data
    rows = []
    for index, (sentence, output) in enumerate(zip(sentences, reserved)):
        aligned, error = aligned_tokens(sentence.text, sentence.tokens)
        if error:
            rows.append({'text': sentence.text, 'alignment_error': error})
            continue
        proposals = [{'rule': rule.id, 'span': [start, end], 'surface': sentence.text[start:end]}
                     for rule in RULES for start, end in rule.match(aligned)]
        accepted = [{'rule': rule, 'span': [start, end], 'surface': sentence.text[start:end]}
                    for rule, start, end in stage.spans.get(index, ())]
        def compact(tokens):
            return [{'surface': token['surface'], 'lemma': token['lemma'],
                     'rule': token.get('analysis_rule'),
                     'reserved': bool(token.get('grammar_reserved'))}
                    for token in tokens]
        rows.append({'text': sentence.text, 'proposals': proposals, 'accepted': accepted,
                     'dictionary_before': compact(combine_tokens_jmdict(sentence.tokens, dictionary)),
                     'tokens_after_reservation': compact(output.tokens),
                     'dictionary_after': compact(combine_tokens_jmdict(output.tokens, dictionary))})
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path, help='UTF-8 text file, one source sentence per line')
    parser.add_argument('--limit', type=int, default=20, help='maximum sentences (default: 20)')
    parser.add_argument('--dictionary', type=Path, help='JMdict XML; defaults to configured dictionary')
    args = parser.parse_args()
    if args.limit < 1:
        parser.error('--limit must be positive')
    with args.input.open(encoding='utf-8') as stream:
        texts = []
        for line in stream:
            if line.strip():
                texts.append(line.strip())
                if len(texts) == args.limit:
                    break
    print(json.dumps(inspect(texts, args.dictionary), ensure_ascii=True, indent=2))


if __name__ == '__main__':
    main()
