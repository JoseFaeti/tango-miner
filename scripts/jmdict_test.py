import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict

class JMdictLookup:
    def __init__(self, xml_path: str):
        # Resolve path relative to this script
        self.xml_path = Path(xml_path)
        self.tree = ET.parse(self.xml_path)
        self.root = self.tree.getroot()

    def lookup_word(self, word: str) -> List[Dict]:
        """
        Lookup a single word by kanji or reading.
        Returns a list of entries with 'keb', 'reb', 'senses'.
        """
        results = []
        for entry in self.root.findall("entry"):
            k_list = [k.text for k in entry.findall("k_ele/keb")]
            r_list = [r.text for r in entry.findall("r_ele/reb")]

            if word not in k_list and word not in r_list:
                continue

            senses = []
            for sense in entry.findall("sense"):
                glosses = [g.text for g in sense.findall("gloss")]
                pos_list = [p.text for p in sense.findall("pos")]
                senses.append({"gloss": glosses, "pos": pos_list})

            results.append({
                "keb": k_list,
                "reb": r_list,
                "senses": senses
            })
        return results

    def lookup_list(self, words: List[str]) -> Dict[str, List[Dict]]:
        """
        Lookup a list of words and return a dict mapping word -> entries.
        """
        output = {}
        for word in words:
            output[word] = self.lookup_word(word)
        return output

# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    from pathlib import Path
    import csv

    # --- JMdict Lookup setup ---
    xml_file = Path(__file__).parent / "JMdict_e.xml"
    jmdict = JMdictLookup(xml_file)

    # --- CSV input ---
    csv_file = "C:/Users/Jose/workshop/Japanese/ScriptsToMine/anki.dropped.csv"
    words = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                words.append(row[1])

    # --- Output file ---
    output_file = Path(__file__).parent / "jmdict_results.txt"

    with open(output_file, "w", encoding="utf-8") as f:
        for idx, word in enumerate(words, 1):
            entries = jmdict.lookup_word(word)
            
            if not entries:
                line = f"{word} -> No entries found"
                print(line)
                f.write(line + "\n")
                continue

            for e in entries:
                kanji_str = ';'.join(e["keb"])
                reading_str = ';'.join(e["reb"])
                gloss_str = ';'.join(g for s in e["senses"] for g in s["gloss"])
                line = f"Word: {word} | Kanji: {kanji_str} | Reading: {reading_str} -> Gloss: {gloss_str}"
                print(line)
                f.write(line + "\n")
            
            # Optional: progress feedback
            if idx % 100 == 0:
                print(f"Processed {idx}/{len(words)} words")
