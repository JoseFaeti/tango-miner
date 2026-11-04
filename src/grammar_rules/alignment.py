"""Align tokenizer output to its source text."""

def aligned_tokens(text, tokens):
    """Return offsets or an error; never infer offsets across missing text."""
    result, cursor = [], 0
    for index, token in enumerate(tokens):
        surface = token['surface']
        if not surface:
            continue
        start = text.find(surface, cursor)
        if start < 0 or text[cursor:start].strip():
            return None, f'cannot align token {index}'
        end = start + len(surface)
        result.append({'index': index, 'span': [start, end], 'surface': surface,
                       'lemma': token.get('lemma'), 'pos': list(token.get('pos', ()))})
        cursor = end
    if text[cursor:].strip():
        return None, 'token stream omits source text'
    return result, None
