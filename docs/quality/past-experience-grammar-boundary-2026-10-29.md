# Past-experience grammar boundary (2026-10-29)

[Quality analysis index](../false-positive-analysis.md)


The first `〜たことがある` run assigned `困ったことがあるなら` to
the experience card and reduced lexical `困ったこと` from 21 to 20. The
rule now leaves this conditional problem-expression context to lexical
processing, while retaining an immediately preceding `に` context and other
experience uses. This is a narrow ambiguity heuristic, not a general semantic
disambiguation; a corpus comparison has not yet verified its effect.

The follow-up run removed that example from the grammar card but did not
restore `困ったこと`: Sudachi merged `ことがある` into one token. A
targeted boundary repair now exposes `こと` for the dictionary merger in
the observed conditional and retains the original token evidence. A replay
with the configured tokenizer and dictionary recovered `困ったこと`. The
newly selected `言い忘れたことがあるわ` also describes an unsaid matter;
the grammar rule leaves this observed form lexical. These narrow changes need
a full-corpus comparison and do not resolve general nominal ambiguity.

