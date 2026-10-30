"""Recall-first regressions derived from the Anki-output audit.

These examples deliberately cover grammar, fixed expressions, colloquialisms,
and kana-only text. They protect the project's goal of retaining useful
material from the learner's reading corpus while ensuring a merge never
silently changes the displayed source form into an unrelated dictionary word.
"""

import unittest

from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict
from src.steps.FilterFrequencyStep import filter_useful_words
from src.steps.TokenizeStep import (
    TOKENIZER_MODE,
    get_tokenizer,
    sudachi_node_to_dict,
    tokenize,
)


def raw_tokens(text: str) -> list[dict]:
    return [
        sudachi_node_to_dict(node)
        for node in get_tokenizer().tokenize(text, TOKENIZER_MODE)
    ]


class AmbiguousOutputRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.jmdict = get_jmdict()

    def test_conjunction_pos_alone_cannot_distinguish_fragments(self):
        cases = [
            ('ないし', 'まだ全然自信ないし、それに倒れちゃったら困るから。', '月曜日ないし火曜日に会いましょう。'),
            ('ものの', 'そんなところもあなたがうっかりもののあかしです。', 'なんとかロマリアに着いたもののもう商売あがったりですよ。'),
            ('たら', '来月から週休２日になるからがんばってはたらけーっ！', 'いつも難しい顔をしてたら、部下たちも近寄りがたいぞ。'),
            ('んで', '最初の約束どおり魔法の鏡をもらえるといいんでがすがね。', 'んでもって、部屋がナント個室にうつったってわけよ。'),
        ]
        for word, fragment, legitimate in cases:
            with self.subTest(word=word):
                bad, _ = tokenize([fragment])
                good, _ = tokenize([legitimate])
                self.assertIn(word, bad)
                self.assertIsNone(self.jmdict.get_most_common_definition(word, component_counts=bad[word].component_counts))
                # With the system dictionary, たら in the positive example
                # is a conditional auxiliary, not a lexical conjunction.
                if word == 'たら':
                    self.assertNotIn(word, good)
                    continue
                self.assertIn(word, good)
                bad_components = set(bad[word].component_counts)
                good_components = set(good[word].component_counts)
                if word == 'んで':
                    # The fragment is split; the conjunction is a single token.
                    self.assertFalse(bad_components & good_components)
                else:
                    self.assertTrue(bad_components & good_components)
                self.assertIsNone(self.jmdict.get_most_common_definition(word, component_counts=good[word].component_counts))

    def test_sentence_initial_shikashi_uses_only_source_supported_examples(self):
        from src.steps.AddDefinitionsStep import add_and_filter_for_definitions
        from src.steps.AttachSentencesStep import attach_sentences

        supported = 'しかし、そういえば昔、悪い夢を見たことがあるな。'
        unsupported = 'しかしその塔からもすでに逃げてしまったかも知れんな。'
        words, sentences = tokenize([supported, unsupported])
        stats = words['しかし']
        self.assertEqual(stats.frequency, 2)
        self.assertEqual({c[0].context for c in stats.component_counts}, {'', 'initial_shikashi_comma'})
        add_and_filter_for_definitions({'しかし': stats}, lambda *args: None, None)
        self.assertIn('however', stats.definition)
        self.assertEqual(sum(stats.component_counts[c] for c in stats.definition_components), 1)
        stats.score = 100
        attach_sentences({'しかし': stats}, sentences)
        self.assertEqual([s.text for s in stats.sentences], [supported])

        alone, _ = tokenize([unsupported])
        self.assertIsNone(self.jmdict.get_most_common_definition('しかし', component_counts=alone['しかし'].component_counts))

    def test_keeps_grammar_and_fixed_expressions(self):
        cases = {
            "これが答えなんだ。": "なんだ",
            "これは大事なんです。": "んです",
            "ここでは戦えない。": "では",
            "行ってもいいよ。": "てもいい",
            "行かないでください。": "ないで",
            "行かないと間に合わない。": "ないと",
            "彼のために戦う。": "ために",
            "一人でもできる。": "一人で",
            "少なくとも歓迎できる事態ではありませんし。": "少なくとも",
        }

        for sentence, expected in cases.items():
            with self.subTest(sentence=sentence, expected=expected):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_keeps_kana_only_and_colloquial_learning_material(self):
        cases = {
            "長年探し続けて、ようやく見つけた。": "ようやく",
            "何をしようが自由だ。": "しよう",
            "さあ、さいだんへ行こう。": "さいだん",
            "お前の後についてくから適当にがんばってくれ。": "てく",
            "楽しいおもいでができた。": "おもいで",
            "うわ、かっけえ！": "かっけえ",
            "今日はだりぃ。": "だりぃ",
            "それはちげぇよ。": "ちげぇ",
        }

        for sentence, expected in cases.items():
            with self.subTest(sentence=sentence, expected=expected):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_boundary_repairs_preserve_original_components_and_controls(self):
        from src.steps.MergeTokensStep import _repair_chigee_boundary, _repair_warui_koto_boundary
        colloquial = raw_tokens('それはちげぇよ。')
        repaired = _repair_chigee_boundary(colloquial)
        candidate = next(t for t in repaired if t['lemma'] == 'ちげぇ')
        self.assertEqual([c.surface for c in candidate['components']], ['はち', 'げ', 'ぇ'])
        control = raw_tokens('それはちげです。')
        self.assertEqual(_repair_chigee_boundary(control), control)
        standalone = raw_tokens('悪いこと。')
        self.assertEqual(_repair_warui_koto_boundary(standalone), standalone)
        for text in ('それはちげぇよ、ちゃんと考えて。', '悪いことしても、仕方ねのかなとか思う。'):
            words, segments = tokenize([text])
            target = 'ちげぇ' if 'ちげぇ' in text else 'こと'
            start, end = segments[0].lemma_spans[target]
            self.assertEqual(text[start:end], target)
            self.assertTrue(words[target].component_counts)

    def test_dictionary_merges_keep_the_exact_displayed_source_form(self):
        sentences = [
            "おまもりをください。",
            "おまちください。",
            "これが答えなんだ。",
            "これは大事なんです。",
            "ここでは戦えない。",
            "行かないでください。",
            "彼のために戦う。",
            "うわ、かっけえ！",
            "今日はだりぃ。",
        ]

        for sentence in sentences:
            with self.subTest(sentence=sentence):
                merged = combine_tokens_jmdict(raw_tokens(sentence), self.jmdict)
                dictionary_merges = [token for token in merged if token["pos"][0] == "exp"]
                self.assertTrue(dictionary_merges)
                self.assertTrue(
                    all(token["surface"] == token["lemma"] for token in dictionary_merges)
                )

    def test_grammatical_boundaries_do_not_create_unrelated_words(self):
        cases = {
            "できないし困る。": ("ないし", "ない"),
            "そんなことしてみろ、命がいくつあっても足りないぜ。": ("ことし", "こと"),
        }
        for sentence, (unexpected, expected) in cases.items():
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertNotIn(unexpected, words)
                self.assertIn(expected, words)

    def test_actual_conjunction_and_year_remain_available(self):
        for sentence, expected in [("ないしは別の方法だ。", "ないし"),
                                   ("ことしは良い年になる。", "ことし")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_colloquial_kamoshirenai_is_not_split_into_shireru(self):
        for sentence in ["もしかしたらホントに優勝も夢じゃないかもしれねえな。",
                         "運のちからでラプソーンを楽に倒せるかもしれねえでげすぜ。",
                         "明日にはそこへ行くかもしれねぇ。"]:
            with self.subTest(sentence=sentence):
                words, segments = tokenize([sentence])
                self.assertIn("かもしれない", words)
                self.assertNotIn("しれる", words)
                self.assertNotIn("じゃないか", words)
                self.assertIn(segments[0].lemma_surfaces["かもしれない"], sentence)
                components, = words["かもしれない"].component_counts
                self.assertEqual(''.join(c.surface for c in components), segments[0].lemma_surfaces["かもしれない"])
        for sentence, expected in [("素性のしれない者を信じる。", "しれる"),
                                   ("行くかもしれない。", "かもしれない"),
                                   ("夢じゃないか。", "じゃないか")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_negative_kamoshirenai_variants_keep_the_source_and_components(self):
        for sentence, surface in [
            ("世界に平和をとりもどせるかもしれぬ。", "かもしれぬ"),
            ("何か良い情報が聞けるかもしれませぬ。", "かもしれませぬ"),
            ("明日にはそこへ行くかもしれん。", "かもしれん"),
        ]:
            with self.subTest(sentence=sentence):
                words, segments = tokenize([sentence])
                self.assertIn("かもしれない", words)
                self.assertNotIn("しれる", words)
                self.assertEqual(segments[0].lemma_surfaces["かもしれない"], surface)
                components, = words["かもしれない"].component_counts
                self.assertEqual(''.join(c.surface for c in components), surface)
        for sentence in ["素性のしれぬ者を信じる。", "何かがしれんとは言えない。"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("しれる", words)

    def test_occurrence_span_tracks_the_token_not_an_earlier_substring(self):
        sentence = "変わった後で、石をわったんだ。"
        words, segments = tokenize([sentence])
        self.assertIn("わる", words)
        start, end = segments[0].lemma_spans["わる"]
        self.assertEqual(sentence[start:end], "わっ")
        self.assertEqual(start, sentence.rindex("わっ"))

    def test_kigahen_does_not_steal_kanji_from_kawaru(self):
        sentence = "じゃあ気が変わったらまた声をかけてちょうだいね。"
        words, segments = tokenize([sentence])
        self.assertIn("変わる", words)
        self.assertIn("気", words)
        self.assertNotIn("わる", words)
        self.assertNotIn("気が変", words)
        self.assertEqual(segments[0].lemma_surfaces["変わる"], "変わっ")
        start, end = segments[0].lemma_spans["変わる"]
        self.assertEqual(sentence[start:end], "変わっ")
        components, = words["変わる"].component_counts
        self.assertIn([c.surface for c in components], [["気が変", "わっ"], ["変わっ"]])
        for text, expected in [("彼は気が変で困っている。", "気が変"),
                               ("彼は石をわってから帰った。", "わる"),
                               ("気が変わることもある。", "気が変わる")]:
            with self.subTest(text=text):
                retained, _ = tokenize([text])
                self.assertIn(expected, retained)

    def test_kigahen_repair_uses_reserved_conditional_as_context(self):
        from src.Artifact import Artifact
        from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
        from src.grammar_rules.rules.tara import RULE
        from tests.test_grammar_te_iru import sentence

        for text in ['じゃあ気が変わったらまた声をかけてちょうだいね。',
                     'やめとくうか…気が変わったらいつでも来る。']:
            with self.subTest(text=text):
                source = sentence(text, raw_tokens(text))
                stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
                artifact = stage.process(Artifact([source]))
                tokens = ExcludeGrammarTokensStep(stage).process(artifact).data[0].tokens
                merged = combine_tokens_jmdict(tokens, self.jmdict)
                self.assertEqual(stage.occurrences[RULE.id].count, 1)
                self.assertEqual([t['surface'] for t in merged if t.get('grammar_reserved')], ['たら'])
                self.assertIn('気', [t['lemma'] for t in merged])
                verb = next(t for t in merged if t['lemma'] == '変わる')
                self.assertEqual(verb['surface'], '変わっ')
                self.assertIn([c.surface for c in verb.get('components', ())], [[], ['気が変', 'わっ']])
                self.assertNotIn('気が変', [t['lemma'] for t in merged])
                self.assertNotIn('わる', [t['lemma'] for t in merged])

    def test_empty_sudachi_tokens_do_not_break_later_highlights(self):
        sentence = "やめとくうか…気が変わったらいつでも来る。"
        words, segments = tokenize([sentence])
        self.assertIn("変わる", words)
        start, end = segments[0].lemma_spans["変わる"]
        self.assertEqual(sentence[start:end], "変わっ")

    def test_kigakawatta_predicate_is_not_unusual_adjective(self):
        for sentence in ["やはり気が変わったか！",
                         "わ、わ、わ、わ、ねぇ気が変わったんでしょ。",
                         "わしは考えたが、気が変わった。",
                         "NAME:気が変わった…"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("変わる", words)
                self.assertNotIn("変わった", words)
                self.assertNotIn("わる", words)
        for sentence in ["変わった鉱物を材料に作れたら面白いんだがなぁ。",
                         "金持ちってのは変わった鏡を持ってるもんなんでがすねえ。",
                         "特に好きなのは、変わった習性を持つモンスター。"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("変わった", words)

    def test_prenominal_dictionary_entries_do_not_override_past_predicates(self):
        for sentence, base, false_target in [
            ("私はもう疲れた。", "疲れる", "疲れた"),
            ("お仕事にはもう慣れた？", "慣れる", "慣れた"),
            ("最終決戦の準備が整ったんだ。", "整う", "整った"),
            ("お前は落ち着いた。", "落ち着く", "落ち着いた"),
            ("彼女の性格が変わったんだ。", "変わる", "変わった"),
        ]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(base, words)
                self.assertNotIn(false_target, words)
        for sentence, target in [
            ("疲れた人がそこにいる。", "疲れた"),
            ("慣れた手つきで作業する。", "慣れた"),
            ("落ち着いた雰囲気が好きだ。", "落ち着いた"),
            ("それも優れた、天才的な発想力が必要だ！", "優れた"),
            ("まったく毎年ご苦労なこった。", "こった"),
        ]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(target, words)

    def test_negative_adjective_and_auxiliary_do_not_merge_as_naishi(self):
        cases = [
            "このあたりは他に町もないしここで身体を休めていってね。",
            "あ、私もこれ以上、ミヤとの将来は考えたくないし。",
            "できないし困る。",
        ]
        for sentence in cases:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("ない", words)
                self.assertNotIn("ないし", words)
        for sentence, expected in [("ないしは別の方法だ。", "ないし"),
                                   ("東京ないし大阪に行く。", "ないし"),
                                   ("行かないでください。", "ないで"),
                                   ("行かないと間に合わない。", "ないと")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_kotoshi_splits_only_with_following_suru_grammar(self):
        for sentence in ["でも先輩はそんなことしなくても、いっぱい貰えますよね。",
                         "悪いことしても、仕方ねのかなとか思う。",
                         "みんなを守るはずの兵士があんなことしちゃあ",
                         "なんでオレがこんなことしなくちゃならないんだよ！",
                         "そんなことしてみろ、命がいくつあっても足りないぜ。"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertNotIn("ことし", words)
                self.assertIn("こと", words)
                self.assertIn("する", words)
                if any(t["surface"] == "ことし" for t in raw_tokens(sentence)):
                    evidence = [c for seq in words["こと"].component_counts for c in seq]
                    self.assertTrue(any(c.surface == "ことし" for c in evidence))
        for sentence in ["ことしは良い年になる。", "ことしでも来年でもいいよ。"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("ことし", words)

    def test_imperatives_do_not_pollute_potential_forms(self):
        for sentence, base, potential in [
            ("フフフせいぜい祈れ。", "祈る", "祈れる"),
            ("祈れ、今こそ執行の時だ。", "祈る", "祈れる"),
            ("奈々ちゃんが代われって言った。", "代わる", "代われる"),
            ("さぁモンショウのありかを吐け！", "吐く", "吐ける"),
        ]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(base, words)
                self.assertNotIn(potential, words)
                components, = words[base].component_counts
                original = next(t for t in raw_tokens(sentence) if t["surface"] == components[0].surface)
                self.assertEqual(components[0].lemma, original["lemma"])
        for sentence, expected in [("あなたが祈れるようになった。", "祈れる"),
                                   ("私は神様に祈れないんだ。", "祈れる"),
                                   ("代われるものなら代わりたいよ！", "代われる"),
                                   ("間違えずに全部踊れたら大丈夫。", "踊れる")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_reassigned_imperatives_do_not_hide_a_real_low_frequency_potential(self):
        sentences = ["代われるものなら代わりたいよ。", "あなたと代われるなら嬉しい。",
                     "奈々ちゃんが代われって言った。", "あなたが代われって言った。"]
        words, segments = tokenize(sentences, tag="fixture")
        self.assertIn("代われる", words)
        self.assertEqual(words["代われる"].frequency, 2)
        self.assertEqual(words["代われる"].reassigned_occurrences, 2)
        kept = filter_useful_words(words, min_frequency=4, keep_percent=100, min_frequency_per_source=0)
        self.assertIn("代われる", kept)
        self.assertEqual(kept["代われる"].frequency, 2)
        for segment in segments:
            if "代われって" in segment.text:
                self.assertNotIn("代われる", segment.lemma_surfaces)
                self.assertIn("代わる", segment.lemma_surfaces)

    def test_imperatives_do_not_create_a_potential_candidate_by_themselves(self):
        words, _ = tokenize(["祈れ、今こそ執行の時だ。", "祈れ、願いは届く。"], tag="fixture")
        self.assertNotIn("祈れる", words)
        self.assertIn("祈る", words)

    def test_potential_form_fallback_is_source_grounded(self):
        for sentence, word, base in [
            ("彼の子孫たちにこうして会えるなんてすごくうれしいよ。", "会える", "会う"),
            ("果たしてもう一度戦ったとして勝てるかどうか。", "勝てる", "勝つ"),
            ("神を超える力を持った私を倒せるはずがなかろう。", "倒せる", "倒す"),
        ]:
            with self.subTest(word=word):
                words, _ = tokenize([sentence])
                stats = words[word]
                self.assertIsNone(self.jmdict.get_most_common_definition(word))
                result = self.jmdict.get_potential_definition(word, stats.component_counts, get_tokenizer())
                self.assertIsNotNone(result)
                self.assertTrue(result.startswith(f"Potential form of {base}:"))
        words, _ = tokenize(["森の中で猫を見れる。"])
        self.assertIsNone(self.jmdict.get_potential_definition("見れる", {}, get_tokenizer()))

    def test_adjective_pos_evidence_disambiguates_dictionary_homographs(self):
        for sentence, word, expected in [
            ("ふかい所ではいきが長持ちしませんが。", "ふかい", "deep"),
            ("むさい男ばっかのパーティーがイヤになった。", "むさい", "filthy"),
        ]:
            with self.subTest(word=word):
                words, _ = tokenize([sentence])
                result = self.jmdict.get_most_common_definition(word, component_counts=words[word].component_counts)
                self.assertIn(expected, result)

    def test_explanatory_nda_uses_only_supported_occurrences_as_examples(self):
        from src.steps.AddDefinitionsStep import add_and_filter_for_definitions
        from src.steps.AttachSentencesStep import attach_sentences
        sentences = ['ここに入ってたんだ。', 'あの人は昨日の夜に死んだ。', 'んだ、そうだと思うよ。']
        words, segments = tokenize(sentences, tag='fixture')
        self.assertIn('んだ', words)
        stats = words['んだ']
        add_and_filter_for_definitions({'んだ': stats}, lambda *args: None, None)
        self.assertFalse(stats.needs_review)
        self.assertIn('reason', stats.definition)
        self.assertIsNotNone(stats.definition_components)
        self.assertTrue(all(len(c) == 2 for c in stats.definition_components))
        self.assertGreater(sum(count for c, count in stats.component_counts.items()
                               if c not in stats.definition_components), 0)
        stats.score = 500
        attach_sentences({'んだ': stats}, segments)
        self.assertEqual([s.text for s in stats.sentences], ['ここに入ってたんだ。'])

    def test_high_frequency_homophone_definitions_use_token_context(self):
        for sentence, word, expected in [
            ("何をする。", "する", "to do"),
            ("猫がいる。", "いる", "to be"),
            ("長年探して、ようやく見つけた。", "ようやく", "finally"),
        ]:
            with self.subTest(word=word):
                words, _ = tokenize([sentence])
                result = self.jmdict.get_most_common_definition(word, component_counts=words[word].component_counts)
                self.assertIsNotNone(result)
                self.assertIn(expected, result)
        for sentence, word, expected in [
            ("紙に文字を刷る。", "刷る", "print"),
            ("道具が要る。", "要る", "needed"),
            ("文章を要約する。", "要約", "summary"),
        ]:
            with self.subTest(word=word):
                words, _ = tokenize([sentence])
                self.assertIn(word, words)
                self.assertIn(expected, self.jmdict.get_most_common_definition(word, component_counts=words[word].component_counts))

    def test_inflection_merges_keep_verb_targets_and_raw_evidence(self):
        for sentence, expected, false_target, definition in [
            ("よくもあんなところに閉じ込めおって！", "おる", "おって", "to be"),
            ("私はたまたま国を離れておってな。", "おる", "おって", "to be"),
            ("やつらはみな田舎者だと思っておってな。", "おる", "おって", "to be"),
            ("本を読んでおってくれ。", "おる", "おって", "to be"),
            ("だが黒いモヤに覆われた生き物。", "覆う", "覆われ", "to cover"),
            ("冬要塞は硬い壁に覆われている。", "覆う", "覆われ", "to cover"),
            ("彼は商人に雇われた。", "雇う", "雇われ", "to employ"),
            ("この子を神殿までつれていって。", "連れる", "つれて", "bring along"),
            ("彼を町までつれてきた。", "連れる", "つれて", "bring along"),
            ("何でワシを、こんな所につれてきたんじゃ！？", "連れる", "つれて", "bring along"),
        ]:
            with self.subTest(sentence=sentence):
                words, segments = tokenize([sentence])
                self.assertIn(expected, words)
                self.assertNotIn(false_target, words)
                result = self.jmdict.get_most_common_definition(expected, component_counts=words[expected].component_counts)
                self.assertIsNotNone(result)
                self.assertIn(definition, result)
                components, = words[expected].component_counts
                self.assertTrue(all(c.surface in sentence for c in components))
                start, end = segments[0].lemma_spans[expected]
                self.assertEqual(sentence[start:end], segments[0].lemma_surfaces[expected])

    def test_inflection_repairs_preserve_real_nouns_and_expressions(self):
        for sentence, target in [
            ("追手がすぐそこまで来た。", "追手"),
            ("おって連絡いたします。", "おって"),
            ("追手がおってきた。", "おって"),
            ("雪の覆われの美しい山だ。", "覆われ"),
            ("時代の変化につれて成長する。", "につれて"),
            ("子供たちをつれて遊ぶ。", "つれて"),
            ("あの子をつれてくる。", "つれてくる"),
            ("本を読んだ。つれていく。", "つれて"),
        ]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(target, words)

    def test_known_lemma_inventions_do_not_return(self):
        cases = {
            "おまちください。": "おまつ",
            "少なくとも歓迎できる事態ではありませんし。": "ぬし",
        }

        for sentence, unexpected in cases.items():
            with self.subTest(sentence=sentence, unexpected=unexpected):
                words, _ = tokenize([sentence])
                self.assertNotIn(unexpected, words)


if __name__ == "__main__":
    unittest.main()
