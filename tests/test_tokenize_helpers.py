import unittest

from src.TokenizeStep import (
    SENT_BOUNDARY,
    build_sentence_candidates,
    clean_sentence_text,
    contains_japanese_script,
    is_good_sentence_candidate,
    is_japanese_char,
    is_useless,
    iter_sudachi_chunks,
    kata_to_hira,
    replace_markup_with_placeholder,
    split_glued_dialogue_turns,
    strip_control_code_runs,
    looks_like_guide_header,
    split_text_by_utf8_bytes,
)


def token(lemma, pos=("名詞", "普通名詞", "一般", "*"), reading="ヨミ"):
    return {
        "base_form": lemma,
        "lemma": lemma,
        "reading": reading,
        "pos": pos,
    }


class TokenizeHelperTests(unittest.TestCase):
    def test_kata_to_hira_converts_katakana_reading(self):
        self.assertEqual(kata_to_hira("カタカナー"), "かたかなー")
        self.assertIsNone(kata_to_hira(""))

    def test_is_useless_filters_function_words_and_proper_nouns(self):
        self.assertTrue(is_useless(token("は", pos=("助詞", "係助詞", "*", "*"))))
        self.assertTrue(is_useless(token("東京", pos=("名詞", "固有名詞", "地名", "*"))))

    def test_is_useless_filters_noise_but_keeps_regular_japanese_words(self):
        self.assertTrue(is_useless(token("あ")))
        self.assertTrue(is_useless(token("スーパー")))
        self.assertTrue(is_useless(token("言っ")))
        self.assertFalse(is_useless(token("言葉")))

    def test_is_japanese_char_accepts_expected_sentence_characters(self):
        for ch in ["猫", "ね", "ネ", "A", "５", "。", "「"]:
            self.assertTrue(is_japanese_char(ch), ch)

        self.assertFalse(is_japanese_char("😀"))

    def test_split_text_by_utf8_bytes_keeps_chunks_under_limit(self):
        chunks = list(split_text_by_utf8_bytes("言葉abc", max_bytes=7))

        self.assertEqual("".join(chunks), "言葉abc")
        self.assertTrue(all(len(chunk.encode("utf-8")) <= 7 for chunk in chunks))

    def test_iter_sudachi_chunks_splits_oversized_sentence_parts(self):
        text = "言葉" * 10
        chunks = list(iter_sudachi_chunks(text, max_bytes=12))

        self.assertEqual("".join(chunks), text + SENT_BOUNDARY)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk.encode("utf-8")) <= 12 for chunk in chunks))

    def test_clean_sentence_text_strips_non_japanese_script_prefix(self):
        self.assertEqual(
            clean_sentence_text("F4　AD　01　01　10　01　01　01>美鶴は切なげに微笑んだ。"),
            "美鶴は切なげに微笑んだ。",
        )

    def test_clean_sentence_text_preserves_japanese_prefix_before_gt(self):
        self.assertEqual(
            clean_sentence_text("美鶴>切なげに微笑んだ。"),
            "美鶴>切なげに微笑んだ。",
        )

    def test_contains_japanese_script_ignores_ascii_metadata(self):
        self.assertFalse(contains_japanese_script("F4 AD 01"))
        self.assertTrue(contains_japanese_script("美鶴"))

    def test_replace_markup_with_placeholder_keeps_visible_edit_marker(self):
        self.assertEqual(
            replace_markup_with_placeholder("あなたも<COL　RE1>ヒョウガ</COL>を信じた。"),
            "あなたも[...]ヒョウガ[...]を信じた。",
        )

    def test_clean_sentence_text_replaces_markup_with_placeholder(self):
        self.assertEqual(
            clean_sentence_text("あんたも　<ティーダ、200ギルもらう>　がんばってくれよ"),
            "あんたも　[...]　がんばってくれよ",
        )

    def test_strip_control_code_runs_removes_hex_and_wait_commands(self):
        self.assertEqual(
            strip_control_code_runs("堂島　遼太郎　F5　86　0C　01　そんな事言ってないだろ。"),
            "堂島　遼太郎　　そんな事言ってないだろ。",
        )
        self.assertEqual(
            strip_control_code_runs("今からこわーい…#w(30)まさか。"),
            "今からこわーい…まさか。",
        )

    def test_clean_sentence_text_strips_control_codes(self):
        self.assertEqual(
            clean_sentence_text("F2　01　03　01見つけてあげられれば、きっと喜びそうだ"),
            "見つけてあげられれば、きっと喜びそうだ",
        )

    def test_split_glued_dialogue_turns_splits_new_speaker_after_sentence_end(self):
        self.assertEqual(
            split_glued_dialogue_turns("町の外で待ってるから呼んでね。ベロニカ「どうしたのかしら。"),
            ["町の外で待ってるから呼んでね。", "ベロニカ「どうしたのかしら。"],
        )

    def test_build_sentence_candidates_cleans_and_splits_text(self):
        self.assertEqual(
            build_sentence_candidates("F2　01　01　01町で待ってるよ。ベロニカ「どうしたの？"),
            ["町で待ってるよ。", "ベロニカ「どうしたの？"],
        )

    def test_looks_like_guide_header_detects_strategy_text(self):
        self.assertTrue(looks_like_guide_header("妖精の城の行き方とマップ　妖精の城に入ると魔物戦"))
        self.assertFalse(looks_like_guide_header("あなたが悪いニンゲンじゃないことは知ってる。"))

    def test_is_good_sentence_candidate_rejects_headers_and_markup_heavy_text(self):
        self.assertFalse(is_good_sentence_candidate("妖精の城の行き方とマップ　妖精の城に入ると魔物戦"))
        self.assertFalse(is_good_sentence_candidate("[...][...][...]　フィンに向かっているのです"))
        self.assertTrue(is_good_sentence_candidate("あなたが悪いニンゲンじゃないことは知ってる。"))


if __name__ == "__main__":
    unittest.main()
