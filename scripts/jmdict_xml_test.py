from pathlib import Path
from src.JMDict import JMDict


def main():
    # Initialize JMdict with the XML file
    xml_file = Path.home() / "JMdict_e.xml"
    jmdict = JMDict(xml_file)

    # Test a list of words
    words_to_test = [
        "どう言う",
        # "もうじき",
        # "こと",
        # "ます",
        # "うな", # useless fragment which shouldn't be interpreted as 鰻,
        # "だい", # should be seen as a particle rather than a noun
        # "初めまして",
        # "守護者",
        # "今まで",
        # "お世話"
    ]

    for word in words_to_test:
        print(f'querying dict for word {word}')
        entries = jmdict.lookup_word(word)  # query per word
        if not entries:
            print(f"{word} not found")
            continue

        for i, e in enumerate(entries):
            print(f'\nENTRY #{i}', e)
            kanji_str = ";".join(form for k in e["kanji"] for form in k["form"])
            gloss_str = ";".join(gloss for s in e["senses"] for gloss in s["gloss"])
            pos_str = ';'.join(collect_all_pos(e))
            is_noun = is_exclusively_a_noun(e)
            is_expr = is_expression(e)
            print(f"Kanji: {kanji_str} -> Gloss: {gloss_str} | POS: {pos_str} | NOUN={is_noun} | EXPRESSION={is_expr}")


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


if __name__ == '__main__':
    main()