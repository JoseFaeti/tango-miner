from src.Artifact import Artifact

class WriteUserDictionaryFile:
		def process(self, artifact: Artifact) -> Artifact:
				write_final_file(artifact.data, self.output_path, self.progress)
				return artifact

def write_final_file(input, output_file, progress_handler=None):
		with open(output_file, "w", newline="", encoding="utf-8") as f:
				writer = csv.writer(f)
				writer.writerow([
						"表層形","左文脈ID","右文脈ID","コスト",
						"品詞","品詞細分類1","品詞細分類2","品詞細分類3",
						"活用型","活用形","原形","読み","発音"
				])

				for i, word in enumerate(input):
						# Now build Sudachipy CSV row
						row = [
								seq, 0, 0, 0,
								"名詞","固有名詞","*","*",
								"*","*",
								seq, reading, reading
						]

						output_file.write(s + "\n")

				if progress_handler:
						progress_handler(None, 1, 1, f'Output written to {Path(output_file).resolve()}.')
