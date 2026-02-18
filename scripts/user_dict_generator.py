import csv
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from sudachipy import dictionary, tokenizer

from .AddDictionaryEntries import AddDictionaryEntries
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.ProcessingStep import ProcessingStep
from src.WordStats import WordStats
from .WriteUserDictionaryFile import WriteUserDictionaryFile

input_file = Path("user_dict_candidates.txt")
output_file = Path("user_dict_sudachi.csv")


JMDICT_TO_SUDACHI_POS = {
    # ---- Nouns ----
    "noun (common) (futsuumeishi)": ("名詞", "普通名詞", "一般", "*"),
    "noun (proper)": ("名詞", "固有名詞", "一般", "*"),
    "noun (temporal) (jisoumeishi)": ("名詞", "普通名詞", "副詞可能", "*"),

    # ---- Verbs ----
    "Ichidan verb": ("動詞", "一般", "*", "*"),
    "Godan verb - -aru special class": ("動詞", "一般", "*", "*"),
    "Godan verb": ("動詞", "一般", "*", "*"),
    "transitive verb": ("動詞", "一般", "*", "*"),
    "intransitive verb": ("動詞", "一般", "*", "*"),

    # ---- Adjectives ----
    "adjective (keiyoushi)": ("形容詞", "一般", "*", "*"),
    "adjectival nouns or quasi-adjectives (keiyodoshi)": ("形状詞", "一般", "*", "*"),

    # ---- Adverbs ----
    "adverb (fukushi)": ("副詞", "一般", "*", "*"),
    "adverb taking the 'to' particle": ("副詞", "一般", "*", "*"),

    # ---- Conjunction ----
    "conjunction": ("接続詞", "*", "*", "*"),

    # ---- Interjection ----
    "interjection (kandoushi)": ("感動詞", "一般", "*", "*"),

    # ---- Expressions fallback ----
    "expressions (phrases, clauses, etc.)": ("名詞", "普通名詞", "一般", "*"),
}

POS_PRIORITY = [
    "動詞",
    "形容詞",
    "形状詞",
    "副詞",
    "接続詞",
    "感動詞",
    "名詞"
]



def main():
    valid_entries = []
    checked_sequences = []

    # Load input sequences
    with open(input_file, "r", encoding="utf-8") as f:
        sequences = [line.strip() for line in f if line.strip()]

    # convert candidate substrings to WordStats objects
    print_step_progress(ProcessingStep.FILTERING, 0, len(sequences))

    tokenizer_obj = dictionary.Dictionary().create()
    mode = tokenizer.Tokenizer.SplitMode.C  # or your preferred mode

    word_data = {}

    for i, s in enumerate(sequences):
        print_step_progress(ProcessingStep.FILTERING, i, len(sequences))

        tokens = tokenizer_obj.tokenize(s, mode)

        if len(tokens) == 1:
            continue

        # the sequence hasn't been recognized by the tokenizer as a single token,
        # so it might be an unrecognized word or expression
        word_data[s] = WordStats(
            0, 1, 0.0, "", "", set(), [], s, "", invalid=False
        )

    with (
        TemporaryDirectory() as tmpdir_str,
        open(output_file, "w", newline="", encoding="utf-8") as f
    ):
        tmpdir = Path(tmpdir_str)
        writer = csv.writer(f)

        # --- create a callback closure that has access to writer ---
        def on_entry_processed_callback(word, entry):
            if not entry:
                return

            # print(word, entry)

            cost = 0
            sense = entry['senses'][0]
            reading = entry['reading'][0]['form']
            pos = resolve_sudachi_pos(sense['pos'])

            row = [
                word,          # 0 見出し (TRIE用)
                0,             # 1 左連接ID
                0,             # 2 右連接ID
                cost,          # 3 コスト
                word,          # 4 表示見出し
                pos[0],        # 5 品詞1
                pos[1],        # 6 品詞2
                pos[2],        # 7 品詞3
                pos[3],        # 8 品詞4
                "*",           # 9 活用型
                "*",           # 10 活用形
                reading,       # 11 読み
                word,          # 12 正規化表記
                "*",            # 13 辞書形ID
                "C",           # 14 分割タイプ (C is safest default)
                "*",           # 15 A単位分割情報
                "*",           # 16 B単位分割情報
                "*"            # 17 未使用
            ]

            writer.writerow(row)

        steps = [
            AddDictionaryEntries(on_entry_processed=on_entry_processed_callback)
        ]

        pipeline = Pipeline(steps=steps, on_progress=print_step_progress)
        pipeline.run(Artifact(word_data, tmpdir=tmpdir))

    print('done')


def resolve_sudachi_pos(jmdict_pos_list):
    candidates = []

    for pos in jmdict_pos_list:
        mapped = JMDICT_TO_SUDACHI_POS.get(pos)
        if mapped:
            candidates.append(mapped)

    if not candidates:
        # fallback
        return ("名詞", "普通名詞", "一般", "*")

    # choose highest priority based on first element
    for priority in POS_PRIORITY:
        for candidate in candidates:
            if candidate[0] == priority:
                return candidate

    # safety fallback
    return candidates[0]


def print_step_progress(step, amount, total, additional_text=""):
    if step is None:
        print(additional_text)
        return

    step_text = {
        step.FILTERING: "Filtering definition candidates",
        step.DEFINITIONS: "Adding definitions"
    }

    if amount >= total:
        print(f"{step_text[step]}... done. {additional_text}", flush=True)
    else:
        percent = f"{amount / total:.1%}"
        print(f"{step_text[step]}... {percent} {additional_text}", end="\r", flush=True)


if __name__ == "__main__":
    main()
