from sudachipy import tokenizer, dictionary

tokenizer_obj = dictionary.Dictionary(dict="full").create()

input = [
	'(奈々)どこまでホントで　どこから　ウソなのか分からなくて',
	'誰一人として　父さんの悲しみを分かって　あげようとしない。'
]

with open('sudachipy_test_output.txt', 'w', encoding='utf-8') as f:
	for i in input:
		f.write(f'\n{i}\n')

		tokens = tokenizer_obj.tokenize(i)

		for m in tokens:
			f.write(f'{m.surface()} | {m.dictionary_form()} | {m.normalized_form()} | {m.reading_form()} | {m.part_of_speech()}\n')