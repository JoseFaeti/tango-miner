from pathlib import Path
from jmdict_test import JMdictLookup  # your SQL version

# Initialize JMdictLookup with the XML file
xml_file = Path(__file__).parent / "JMdict_e.xml"
jmdict = JMdictLookup(xml_file)

# Test a list of words
words_to_test = ["守護者", "今まで", "お世話"]

for word in words_to_test:
    entries = jmdict.lookup_word(word)  # query per word
    if not entries:
        print(f"{word} not found")
        continue

    for e in entries:
        kanji_str = ";".join(e["keb"])
        gloss_str = ";".join(gloss for s in e["senses"] for gloss in s["gloss"])
        print(f"Kanji: {kanji_str} -> Gloss: {gloss_str}")
