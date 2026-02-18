import argparse
from pathlib import Path
import unicodedata
import time


MIN_LEN = 2
MAX_LEN = 20
PROGRESS_EVERY = 100  # lines


def is_valid_japanese_char(ch: str) -> bool:
    name = unicodedata.name(ch, "")

    if "HIRAGANA" in name:
        return True
    if "KATAKANA" in name:
        return True
    if "CJK UNIFIED IDEOGRAPH" in name:
        return True
    if ch in {"ー", "々", "〆", "ヵ", "ヶ"}:
        return True

    return False


def extract_substrings_from_sentence(sentence: str, results: set):
    length = len(sentence)

    for start in range(length):
        if not is_valid_japanese_char(sentence[start]):
            continue

        for end in range(start + 1, min(start + MAX_LEN, length) + 1):
            ch = sentence[end - 1]

            if not is_valid_japanese_char(ch):
                break

            if end - start >= MIN_LEN:
                results.add(sentence[start:end])


def process_file(path: Path, results: set, stats: dict):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stats["lines"] += 1
            sentence = line.strip()

            if sentence:
                extract_substrings_from_sentence(sentence, results)

            if stats["lines"] % PROGRESS_EVERY == 0:
                elapsed = time.time() - stats["start_time"]
                print(
                    f"[{elapsed:.1f}s] "
                    f"Files: {stats['files']} | "
                    f"Lines: {stats['lines']} | "
                    f"Unique substrings: {len(results)}", end="\r", flush=True
                )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    results = set()

    stats = {
        "files": 0,
        "lines": 0,
        "start_time": time.time()
    }

    if input_path.is_dir():
        files = list(input_path.rglob("*.txt"))
    else:
        files = [input_path]

    for file in files:
        stats["files"] += 1
        print(f"Processing: {file}")
        process_file(file, results, stats)

    print("Writing output...")

    with output_path.open("w", encoding="utf-8") as f:
        for s in results:
            f.write(s + "\n")

    total_time = time.time() - stats["start_time"]
    print(
        f"Done in {total_time:.1f}s | "
        f"Files: {stats['files']} | "
        f"Lines: {stats['lines']} | "
        f"Total unique substrings: {len(results)}"
    )


if __name__ == "__main__":
    main()
