import tempfile
import unittest
from pathlib import Path

from src.JMDict import JMDict
from src.TokenProvenance import TokenComponent
from unittest.mock import Mock


XML = """<?xml version="1.0" encoding="UTF-8"?>
<JMdict>
  <entry>
    <k_ele>
      <keb>猫</keb>
      <ke_pri>nf01</ke_pri>
    </k_ele>
    <r_ele>
      <reb>ねこ</reb>
      <re_pri>ichi1</re_pri>
    </r_ele>
    <sense>
      <pos>noun (common) (futsuumeishi)</pos>
      <gloss>cat</gloss>
    </sense>
    <sense>
      <pos>noun (common) (futsuumeishi)</pos>
      <gloss>feline</gloss>
    </sense>
  </entry>
  <entry>
    <k_ele>
      <keb>猫</keb>
      <ke_pri>nf48</ke_pri>
    </k_ele>
    <r_ele>
      <reb>ねこ</reb>
    </r_ele>
    <sense>
      <gloss>less common cat entry</gloss>
    </sense>
  </entry>
</JMdict>
"""


class JMDictTests(unittest.TestCase):
    def test_conjunction_evidence_selects_only_a_unique_matching_entry(self):
        xml = """<JMdict>
        <entry><r_ele><reb>しかし</reb></r_ele><sense><pos>conjunction</pos><gloss>however</gloss></sense></entry>
        <entry><r_ele><reb>しかし</reb></r_ele><sense><pos>noun (common)</pos><gloss>harlot</gloss></sense></entry>
        <entry><r_ele><reb>だが</reb></r_ele><sense><pos>conjunction</pos><gloss>but</gloss></sense></entry>
        <entry><r_ele><reb>だが</reb></r_ele><sense><pos>conjunction</pos><gloss>yet</gloss></sense></entry>
        </JMdict>"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'dict.xml'
            path.write_text(xml, encoding='utf-8')
            d = JMDict(path)
            conjunction = (TokenComponent('しかし', 'しかし', 'しかし', 'しかし', ('接続詞',)),)
            noun = (TokenComponent('しかし', 'しかし', 'しかし', 'しかし', ('名詞',)),)
            selection = d.select_contextual_entry('しかし', {conjunction: 3, noun: 1})
            self.assertEqual(selection[1], {conjunction})
            self.assertEqual(d.get_most_common_definition('しかし', component_counts={conjunction: 3, noun: 1}), 'however')
            self.assertEqual(d.get_most_common_definition('しかし', component_counts={noun: 1}), 'harlot')
            self.assertIsNone(d.get_most_common_definition('しかし', component_counts={conjunction: 1, noun: 1}))
            ambiguous = (TokenComponent('だが', 'だが', 'だが', 'だが', ('接続詞',)),)
            self.assertIsNone(d.get_most_common_definition('だが', component_counts={ambiguous: 1}))

    def test_nda_explanatory_selection_requires_split_particle_and_copula(self):
        xml = """<JMdict>
        <entry><r_ele><reb>んだ</reb></r_ele><sense><pos>interjection (kandoushi)</pos><gloss>yes</gloss></sense></entry>
        <entry><r_ele><reb>んだ</reb></r_ele><sense><pos>expressions (phrases, clauses, etc.)</pos><gloss>the reason is</gloss></sense></entry>
        </JMdict>"""
        particle = TokenComponent('ん', 'ん', 'ん', 'ん', ('助詞', '準体助詞'))
        copula = TokenComponent('だ', 'だ', 'だ', 'だ', ('助動詞', '*'))
        split = (particle, copula)
        interjection = (TokenComponent('んだ', 'んだ', 'んだ', 'んだ', ('感動詞', '一般')),)
        ambiguous = (TokenComponent('んだ', 'んだ', 'んだ', 'んだ', ('名詞', '普通名詞')),)
        wrong_particle = (TokenComponent('ん', 'ん', 'ん', 'ん', ('助詞', '格助詞')), copula)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'dict.xml'
            path.write_text(xml, encoding='utf-8')
            d = JMDict(path)
            selection = d.select_contextual_entry('んだ', {split: 5, ambiguous: 3, interjection: 1})
            self.assertEqual(selection[1], {split})
            self.assertEqual(d.get_most_common_definition('んだ', component_counts={split: 5, ambiguous: 3}), 'the reason is')
            for other in (ambiguous, interjection, wrong_particle):
                self.assertIsNone(d.get_most_common_definition('んだ', component_counts={other: 1}))
            self.assertIsNone(d.get_most_common_definition('んだ', component_counts={split: 0}))

    def test_unanimous_modifier_evidence_corrects_only_incompatible_entries(self):
        xml = """<JMdict>
        <entry><r_ele><reb>ふかい</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>discomfort</gloss></sense></entry>
        <entry><r_ele><reb>ふかい</reb></r_ele><sense><pos>adjective (keiyoushi)</pos><gloss>deep</gloss></sense><sense><gloss>profound</gloss></sense><sense><pos>noun (common)</pos><gloss>depth</gloss></sense></entry>
        <entry><r_ele><reb>どうこう</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>trend</gloss></sense></entry>
        <entry><r_ele><reb>どうこう</reb></r_ele><sense><pos>adverb (fukushi)</pos><gloss>this and that</gloss></sense></entry>
        <entry><r_ele><reb>まさか</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>noun</gloss></sense></entry>
        <entry><r_ele><reb>まさか</reb></r_ele><sense><pos>adverb (fukushi)</pos><gloss>first</gloss></sense></entry>
        <entry><r_ele><reb>まさか</reb></r_ele><sense><pos>adverb (fukushi)</pos><gloss>second</gloss></sense></entry>
        </JMdict>"""
        def evidence(word, pos, reading=None):
            return (TokenComponent(word, word, word, reading or word, (pos, '*')),)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'dict.xml'
            path.write_text(xml, encoding='utf-8')
            d = JMDict(path)
            adjective = evidence('ふかい', '形容詞')
            noun = evidence('ふかい', '名詞')
            self.assertEqual(d.get_most_common_definition('ふかい', component_counts={adjective: 4}), '1. deep<br>2. profound')
            self.assertEqual(d.get_most_common_definition('どうこう', component_counts={evidence('どうこう', '副詞'): 2}), 'this and that')
            self.assertEqual(d.get_most_common_definition('ふかい', component_counts={adjective: 4, noun: 1}), '1. deep<br>2. profound')
            self.assertIsNone(d.get_most_common_definition('ふかい', component_counts={evidence('ふかい', '形容詞', 'ちがう'): 1}))
            self.assertIsNone(d.get_most_common_definition('まさか', component_counts={evidence('まさか', '副詞'): 1}))
            merged = adjective + noun
            self.assertIsNone(d.get_most_common_definition('ふかい', component_counts={merged: 1}))

    def test_homophone_context_requires_consistent_unambiguous_evidence(self):
        xml = """<JMdict>
        <entry><r_ele><reb>ようやく</reb></r_ele><sense><pos>adverb (fukushi)</pos><gloss>finally</gloss></sense></entry>
        <entry><r_ele><reb>ようやく</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>summary</gloss></sense></entry>
        </JMdict>"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dict.xml"
            path.write_text(xml, encoding="utf-8")
            d = JMDict(path)
            adverb = (TokenComponent("ようやく", "ようやく", "ようやく", "ようやく", ("副詞",)),)
            noun = (TokenComponent("ようやく", "ようやく", "ようやく", "ようやく", ("名詞",)),)
            self.assertEqual(d.get_most_common_definition("ようやく", component_counts={adverb: 2}), "finally")
            self.assertEqual(d.get_most_common_definition("ようやく", component_counts={adverb: 2, noun: 1}), "finally")
            self.assertEqual(d.get_most_common_definition("ようやく", component_counts={noun: 1}), "summary")
            self.assertIsNone(d.get_most_common_definition("ようやく", component_counts={adverb: 1, noun: 1}))
            self.assertEqual(d.get_most_common_definition("ようやく"), "summary")

    def test_definitions_respect_known_spelling_and_reading_restrictions(self):
        xml = """<JMdict><entry>
        <k_ele><keb>表記</keb></k_ele><k_ele><keb>別記</keb></k_ele>
        <r_ele><reb>ひょうき</reb></r_ele><r_ele><reb>べっき</reb></r_ele>
        <sense><gloss>shared</gloss></sense>
        <sense><stagk>別記</stagk><gloss>other spelling</gloss></sense>
        <sense><stagr>べっき</stagr><gloss>other reading</gloss></sense>
        </entry><entry><k_ele><keb>限定</keb></k_ele><k_ele><keb>他</keb></k_ele>
        <sense><stagk>他</stagk><gloss>only other</gloss></sense></entry></JMdict>"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dict.xml"
            path.write_text(xml, encoding="utf-8")
            d = JMDict(path)
            self.assertEqual(d.get_most_common_definition("表記"), "1. shared<br>2. other reading")
            self.assertEqual(d.get_most_common_definition("ひょうき"), "1. shared<br>2. other spelling")
            self.assertEqual(d.get_most_common_definition("別記"), "1. shared<br>2. other spelling<br>3. other reading")
            self.assertIsNone(d.get_most_common_definition("限定"))

    def build_dict(self):
        tmp = tempfile.TemporaryDirectory()
        path = Path(tmp.name) / "JMdict_e.xml"
        path.write_text(XML, encoding="utf-8")
        return tmp, JMDict(path)

    def test_short_kana_context_uses_unanimous_pos_without_guessing_homophones(self):
        xml = """<JMdict>
        <entry><r_ele><reb>まつ</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>pine</gloss></sense></entry>
        <entry><r_ele><reb>まつ</reb></r_ele><sense><pos>Godan verb</pos><gloss>wait</gloss></sense></entry>
        <entry><r_ele><reb>そば</reb></r_ele><sense><pos>noun (common)</pos><gloss>noodles</gloss></sense></entry>
        <entry><r_ele><reb>そば</reb></r_ele><sense><pos>noun (common)</pos><gloss>nearby</gloss></sense></entry>
        <entry><r_ele><reb>たい</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>fish</gloss></sense></entry>
        <entry><r_ele><reb>たい</reb></r_ele><sense><pos>auxiliary adjective</pos><gloss>want to</gloss></sense><sense><gloss>wish to</gloss></sense><sense><pos>particle</pos><gloss>emphasis</gloss></sense></entry>
        </JMdict>"""
        def component(word, pos):
            return (TokenComponent(word, word, word, word, (pos, "*")),)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dict.xml"
            path.write_text(xml, encoding="utf-8")
            d = JMDict(path)
            self.assertEqual(d.get_most_common_definition("まつ", component_counts={component("まつ", "動詞"): 2}), "wait")
            self.assertEqual(d.get_most_common_definition("たい", component_counts={component("たい", "助動詞"): 2}), "1. want to<br>2. wish to")
            self.assertIsNone(d.get_most_common_definition("そば", component_counts={component("そば", "名詞"): 2}))
            self.assertEqual(d.get_most_common_definition("まつ", component_counts={component("まつ", "名詞"): 2}), "pine")
            self.assertEqual(d.get_most_common_definition("まつ", component_counts={component("まつ", "動詞"): 2, component("まつ", "名詞"): 1}), "wait")
            self.assertIsNone(d.get_most_common_definition("まつ"))

    def test_potential_fallback_checks_reading_morphology_and_evidence(self):
        xml = """<JMdict>
        <entry><k_ele><keb>倒す</keb></k_ele><r_ele><reb>たおす</reb></r_ele><sense><pos>transitive verb</pos><gloss>defeat</gloss></sense></entry>
        <entry><k_ele><keb>倒す</keb></k_ele><r_ele><reb>こかす</reb></r_ele><sense><pos>transitive verb</pos><gloss>knock over</gloss></sense></entry>
        </JMdict>"""
        node = Mock()
        node.dictionary_form.return_value = "倒せる"
        node.normalized_form.return_value = "倒す"
        tokenizer = Mock()
        tokenizer.tokenize.return_value = [node]
        def evidence(reading="たおせる", pos="動詞", lemma="倒せる"):
            return {(TokenComponent("倒せる", lemma, lemma, reading, (pos, "*")),): 1}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dict.xml"
            path.write_text(xml, encoding="utf-8")
            d = JMDict(path)
            self.assertEqual(d.get_potential_definition("倒せる", evidence(), tokenizer), "Potential form of 倒す: defeat")
            stem_only = {(TokenComponent("倒せ", "倒せる", "倒せる", "たおせる", ("動詞", "*")),): 1}
            self.assertIsNone(d.get_potential_definition("倒せる", stem_only, tokenizer))
            self.assertIsNone(d.get_potential_definition("倒せる", evidence("ふせる"), tokenizer))
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(pos="名詞"), tokenizer))
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(lemma="別"), tokenizer))
            node.normalized_form.return_value = "倒れる"
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(), tokenizer))
            tokenizer.tokenize.return_value = [node, node]
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(), tokenizer))

    def test_lookup_indexes_kanji_and_reading_forms(self):
        tmp, jmdict = self.build_dict()
        self.addCleanup(tmp.cleanup)

        self.assertEqual(len(jmdict.lookup_word("猫")), 2)
        self.assertEqual(len(jmdict.lookup_word("ねこ")), 2)
        self.assertEqual(jmdict.lookup_word("犬"), [])

    def test_get_best_entry_prefers_priority_tags(self):
        tmp, jmdict = self.build_dict()
        self.addCleanup(tmp.cleanup)

        best = jmdict.get_best_entry("猫")

        self.assertEqual(best["senses"][0]["gloss"], ["cat"])

    def test_get_most_common_definition_formats_all_senses(self):
        tmp, jmdict = self.build_dict()
        self.addCleanup(tmp.cleanup)

        definition = jmdict.get_most_common_definition("猫")

        self.assertEqual(definition, "1. cat<br>2. feline")
        self.assertIsNone(jmdict.get_most_common_definition("犬"))


if __name__ == "__main__":
    unittest.main()
