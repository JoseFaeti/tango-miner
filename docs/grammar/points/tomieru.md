# 〜と見える

Status: [~]

## Meaning

It seems that; apparently, based on an inference.

## Verified coverage

Matches a directly attached finite verb, adjective or auxiliary followed by
と見える, including past, polite, negative and て forms. Analyzer tests cover
these forms and separated, incomplete, different-verb and noun comparison
controls. The reserved span begins at と, leaving the preceding predicate
available as evidence.

## Remaining limitations

Other spellings, tokenizations and inflections are not covered. Bare noun
attachments are excluded because they can express visual comparison. Even a
predicate attachment can describe literal visibility; morphology alone cannot
resolve the intended sense. Precision and recall have not been independently
evaluated.
