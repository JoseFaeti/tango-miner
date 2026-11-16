# 〜を免れない

Status: [~]

## Meaning

An undesirable outcome cannot be avoided or escaped: 責任を免れない,
“cannot escape responsibility.” The expression can also describe a concrete
escape, so the matcher does not attempt to decide the meaning from context.

## Supported behavior

Matches an adjacent noun or pronoun, を, and the negative of 免れる; the
card's source span starts at を, preserving the preceding noun as lexical
evidence. Sudachi full-dictionary tests cover 免れない, 免れなかった,
免れなく, 免れなければ, 免れません, and まぬがれない. A following
て is outside the matched continuative span. Positive forms and separated
tokens do not match. The tests also check card identity and token reservation.

## Limitations

The matcher requires an immediately adjacent nominal token and を, so topic
particles and intervening material are outside its scope. Other spellings and
literary negative forms need source-aligned evidence and tests. Dictionary
expression precedence can suppress a match when a larger expression owns it.
