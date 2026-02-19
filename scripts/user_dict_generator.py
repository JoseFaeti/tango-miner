import argparse
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

RE_ALL_HIRAGANA = re.compile(r"^[ぁ-んー]+$")

JMDICT_TO_SUDACHI_POS = {
    # ---- Nouns ----
    "noun (common) (futsuumeishi)": ("名詞", "普通名詞", "一般", "*"),
    "noun (proper)": ("名詞", "固有名詞", "一般", "*"),
    "noun (temporal) (jisoumeishi)": ("名詞", "普通名詞", "副詞可能", "*"),
    "noun, used as a prefix": ("名詞", "普通名詞", "接頭可能", "*"),
    "noun, used as a suffix": ("名詞", "普通名詞", "接尾可能", "*"),
    "numeric": ("名詞", "数詞", "*", "*"),

    # ---- Verbs ----
    "Ichidan verb": ("動詞", "一般", "*", "*"),
    "Godan verb": ("動詞", "一般", "*", "*"),
    "Godan verb - -aru special class": ("動詞", "一般", "*", "*"),
    "Kuru verb - special class": ("動詞", "一般", "*", "*"),
    "Suru verb": ("動詞", "一般", "*", "*"),
    "Suru verb - included": ("動詞", "一般", "*", "*"),
    "transitive verb": ("動詞", "一般", "*", "*"),
    "intransitive verb": ("動詞", "一般", "*", "*"),

    # ---- Adjectives ----
    "adjective (keiyoushi)": ("形容詞", "一般", "*", "*"),
    "adjectival nouns or quasi-adjectives (keiyodoshi)": ("形状詞", "一般", "*", "*"),
    "adjectival nouns or quasi-adjectives (keiyodoshi) taking the 'no' particle":
        ("形状詞", "一般", "*", "*"),
    "adjective (prenoun)": ("連体詞", "*", "*", "*"),
    "pre-noun adjectival (rentaishi)": ("連体詞", "*", "*", "*"),

    # ---- Adverbs ----
    "adverb (fukushi)": ("副詞", "一般", "*", "*"),
    "adverb taking the 'to' particle": ("副詞", "一般", "*", "*"),

    # ---- Particles ----
    "particle": ("助詞", "*", "*", "*"),

    # ---- Auxiliary ----
    "auxiliary verb": ("助動詞", "*", "*", "*"),
    "auxiliary adjective": ("助動詞", "*", "*", "*"),

    # ---- Conjunction ----
    "conjunction": ("接続詞", "*", "*", "*"),

    # ---- Interjection ----
    "interjection (kandoushi)": ("感動詞", "一般", "*", "*"),

    # ---- Prefix / Suffix ----
    "prefix": ("接頭辞", "*", "*", "*"),
    "suffix": ("接尾辞", "*", "*", "*"),

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



def count_lines_fast(path, chunk_size=1024 * 1024):
    count = 0
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            count += chunk.count(b"\n")
    return count


def main():
    parser = argparse.ArgumentParser(description="Process a Japanese text and extract word frequencies.")
    parser.add_argument('--input', '-i')
    parser.add_argument('--output', '-o')

    args = parser.parse_args()

    input_file = args.input
    output_file = args.output

    valid_entries = []
    checked_sequences = []

    tokenizer_obj = dictionary.Dictionary().create()
    mode = tokenizer.Tokenizer.SplitMode.C

    word_data = {}
    total_lines = count_lines_fast(input_file)

    with open(input_file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            s = line.strip()
            if not s:
                continue

            if i % 1000 == 0:
                print_step_progress(ProcessingStep.FILTERING, i, total_lines, f'{i} lines processed')

            tokens = tokenizer_obj.tokenize(s, mode)

            if len(tokens) == 1:
                continue

            word_data[s] = WordStats(
                0, 1, 0.0, "", "", set(), [], s, "", invalid=False
            )

    with (
        TemporaryDirectory() as tmpdir_str,
        open(output_file, "w", newline="", encoding="utf-8") as f
    ):
        tmpdir = Path(tmpdir_str)
        writer = csv.writer(f)
        invalid = 0

        # --- create a callback closure that has access to writer ---
        def on_entry_processed_callback(word, entry):
            nonlocal invalid

            if not entry:
                return

            # print(word, entry)

            sense = entry['senses'][0]
            reading = entry['reading'][0]['form']
            pos = resolve_sudachi_pos(sense['pos'])

            if not pos:
                invalid += 1
                return

            is_all_hiragana = RE_ALL_HIRAGANA.search(word)
            is_expr = is_expression(entry)
            word_length = len(word)

            # only keep kana words if they are expressions, to prevent garbage entries
            if is_all_hiragana and not is_expr and word_length < 3:
                invalid += 1
                return

            if is_exclusively_a_noun(entry) and is_all_hiragana:
                invalid += 1
                return

            cost = 0
            
            if is_expr:
                cost -= 10000 

            if is_all_hiragana:
                if word_length < 3:
                    cost += 1000
                else:
                    # probably a fragment disguised as noun or similar
                    cost += 500
            else:
                cost -= 500 * word_length

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

    print(f'User dictionary generated. {invalid} invalid entries skipped')


def is_exclusively_a_noun(jmdict_entry):
    pos_set = collect_all_pos(jmdict_entry)

    for p in pos_set:
        if not p.startswith("noun"):
            return False

    return True

def is_expression(jmdict_entry):
    pos_set = collect_all_pos(jmdict_entry)

    for p in pos_set:
        if p.startswith("expressions"):
            return True

    return False

def collect_all_pos(jmdict_entry):
    """
    Collect all POS tags from all senses of a JMDict entry.

    Args:
        jmdict_entry (dict): A single dictionary entry from JMdictLookup,
                             expected to have a 'senses' key.

    Returns:
        set: All POS tags found across senses.
    """
    all_pos = set()
    for sense in jmdict_entry.get("senses", []):
        for pos_tag in sense.get("pos", []):
            all_pos.add(pos_tag)
    return all_pos

# def is_most_certainly_an_expression(word: str, pos_list = []):
#     return contains_expression_pos(pos_list) and RE_ALL_HIRAGANA.search(word)


# def contains_expression_pos(jmdict_pos_list = []):
#     return "expressions (phrases, clauses, etc.)" in jmdict_pos_list


def resolve_sudachi_pos(jmdict_pos_list):
    candidates = []

    for pos in jmdict_pos_list:
        mapped = JMDICT_TO_SUDACHI_POS.get(pos)
        if mapped:
            candidates.append(mapped)

    if not candidates:
        return None

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
        print(f"{step_text[step]}... done. {additional_text}\n", flush=True)
    elif (amount % 5000) == 0:
        percent = f"{amount / total:.1%}"
        print(f"{step_text[step]}... {percent} {additional_text}", end="\r", flush=True)


if __name__ == "__main__":
    main()
