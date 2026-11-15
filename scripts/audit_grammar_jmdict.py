"""List exact JMdict matches for registered grammar forms for manual review."""
import argparse
import json
from pathlib import Path
import sys
import tomllib
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.grammar_rules.registry import RULES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rule', help='Limit output to one rule id')
    args = parser.parse_args()
    rules = [rule for rule in RULES if not args.rule or rule.id == args.rule]
    if not rules:
        parser.error('unknown rule id')
    forms = {rule.id: sorted({part.strip().lstrip('〜～')
                              for value in (rule.expression, rule.reading)
                              for part in value.split(' / ') if part.strip()})
             for rule in rules}
    wanted = {form for values in forms.values() for form in values}
    config = tomllib.loads((ROOT / 'config.toml').read_text(encoding='utf-8'))
    path = Path(config.get('dictionary', {}).get('jmdict_path', '~/JMdict_e.xml')).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    matches = {form: [] for form in wanted}
    for _, entry in ET.iterparse(path, events=('end',)):
        if entry.tag != 'entry':
            continue
        entry_forms = {element.text for element in entry.findall('./k_ele/keb')
                       + entry.findall('./r_ele/reb')}
        for form in wanted & entry_forms:
            matches[form].append({
                'entry': entry.findtext('ent_seq'),
                'forms': sorted(entry_forms),
                'senses': [{'gloss': [g.text for g in sense.findall('gloss')],
                            'pos': [p.text for p in sense.findall('pos')],
                            'stagk': [s.text for s in sense.findall('stagk')],
                            'stagr': [s.text for s in sense.findall('stagr')]}
                           for sense in entry.findall('sense')],
            })
        entry.clear()
    result = [{'id': rule.id, 'expression': rule.expression, 'meaning': rule.meaning,
               'matches': {form: matches[form] for form in forms[rule.id]}}
              for rule in rules]
    sys.stdout.reconfigure(encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
