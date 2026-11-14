import unittest

from src.steps.ExtractSentencesStep.index import normalize_sentence_boundaries


class SentenceDetectionTests(unittest.TestCase):
    def sentences(self, text):
        return normalize_sentence_boundaries(text)[0]

    def test_wrapped_prose_and_paragraphs(self):
        self.assertEqual(
            self.sentences("これは長い文章の\r\n　続きです。次です。\r\n\r\n別の段落です。"),
            ["これは長い文章の続きです。", "次です。", "別の段落です。"],
        )

    def test_indented_continuations_and_blank_lines(self):
        self.assertEqual(
            self.sentences("一行目\n　続き\n　さらに続き\n次の行\n　続き\n\n　別の段落"),
            ["一行目続きさらに続き", "次の行続き", "別の段落"],
        )

    def test_dialogue_lines_and_comma_continuation(self):
        self.assertEqual(
            self.sentences("こんにちは\nそうだね\nそれなら、\nまた明日\nありがとう\nさようなら"),
            ["こんにちは", "そうだね", "それなら、また明日", "ありがとう", "さようなら"],
        )

    def test_closing_quotes_remain_with_sentence(self):
        self.assertEqual(
            self.sentences("「本当！？」「そうだよ。」次です。"),
            ["「本当！？」", "「そうだよ。」", "次です。"],
        )


if __name__ == "__main__":
    unittest.main()
