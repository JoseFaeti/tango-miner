from collections import OrderedDict
from fugashi import Tagger
from pathlib import Path
import re
import unidic
import csv

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep

class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        if artifact.is_path:
            output_path = Path("-1.tokenized.tmp")
            tokenize(artifact.data, output_path, progress_handler=self.progress)
            return Artifact(output_path, is_path=True)
        return artifact


def tokenize(input_path, output_path, word_data=None, progress_handler=None):
    tag = None

    if input_path is not None:
        match = re.search(r"\[(.+?)\]", str(input_path))
        tag = match.group(1) if match else None

    tagger = Tagger(f"-d \"{Path(unidic.DICDIR)}\"")

    if word_data is None:
        word_data = OrderedDict()
        token_index = 0
    else:
        # continue token index from the last entry
        token_index = max((v[0] for v in word_data.values()), default=0)

    if input_path is not None:
        with open(input_path, encoding='utf-8') as f:
            text = f.read()
        
        for token in tagger(text):
            # print(token.feature)
            lemma = token.feature.orthBase or token.feature.lemma
            
            if not lemma:
                continue

            token_index += 1

            if lemma in word_data:
                word_data[lemma][1] += 1
            else:
                word_data[lemma] = [token_index, 1, set()]
                
            if tag:
                word_data[lemma][2].add(tag)

    if not output_path:
        return word_data
    else:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)

            processed = 0
            total_words = len(word_data.items())
            total_tokens = token_index

            for word, (index, frequency, tags) in word_data.items():
                index_normalized = 1 - (index / total_tokens)
                frequency_normalized = frequency / total_tokens
                score = round(index_normalized * frequency_normalized * 10_000, 10)

                writer.writerow([word, index, frequency, score, " ".join(sorted(tags))])
                processed += 1

                progress_handler(ProcessingStep.TOKENIZING, processed, total_words)#, f'Total tokens: {processed}')

    # print(f'total tokens: {token_index}')