"""Inspect JMdict and Sudachi coverage of UTF-8 expression candidates."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.steps.MergeTokensStep.index import get_jmdict
from src.steps.TokenizeStep.index import tokenize_raw_sentences
from src.Expressions import inventory


def inspect(expressions, dictionary_path=None):
    dictionary = get_jmdict(dictionary_path)
    sentences = tokenize_raw_sentences(expressions, tag='inspection',
                                       source_path=Path('inspection.txt'))
    return [
        {'expression': sentence.text,
         'jmdict': bool(dictionary.lookup_word(sentence.text)),
         'reading': ''.join(token['reading'] for token in sentence.tokens),
         'reading_in_jmdict': bool(dictionary.lookup_word(
             ''.join(token['reading'] for token in sentence.tokens))),
         'reading': ''.join(token['reading'] for token in sentence.tokens),
         'reading_in_jmdict': bool(dictionary.lookup_word(
             ''.join(token['reading'] for token in sentence.tokens))),
         'tokens': [{'surface': token['surface'], 'lemma': token['lemma'],
                     'pos': list(token['pos'])} for token in sentence.tokens]}
        for sentence in sentences
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path, nargs='?', help='UTF-8 file, one expression per line (default: inventory)')
    parser.add_argument('--dictionary', type=Path, help='JMdict XML (default: configured dictionary)')
    args = parser.parse_args()
    if args.input is None:
        expressions = [entry['expression'] for entry in inventory()]
    else:
        with args.input.open(encoding='utf-8') as stream:
            expressions = [line.strip() for line in stream if line.strip() and not line.lstrip().startswith('#')]
    print(json.dumps(inspect(expressions, args.dictionary), ensure_ascii=True, indent=2))


if __name__ == '__main__':
    main()
