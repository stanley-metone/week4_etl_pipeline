# Hackathon #1 Reflection

**Biggest technical hurdle:** Our team's largest obstacle was inconsistent
data formats across the sources we were merging — timestamps arrived in at
least three different formats, and one feed silently used empty strings
instead of true nulls. This caused our early join logic to drop rows without
any visible error, and we lost almost half a day before we noticed the row
counts didn't match.

**How we resolved it:** We stopped trying to patch the join code and instead
wrote a small standalone validation script first, run before any
transformation, that printed row counts, null counts, and dtype summaries at
each stage. That surfaced the silent drops immediately, and we normalized
timestamps and null representations right after extraction instead of deep
inside the transform logic.

**What I'd do differently on teamwork next time:** We split up by pipeline
stage (extract/transform/load) too early, before agreeing on a shared data
schema. That meant two of us made conflicting assumptions about column
names and types, and we had to redo integration work. Next time I'd push the
team to spend the first 30 minutes agreeing on a schema contract and sample
data before anyone writes stage-specific code, and I'd set up a shared
validation checkpoint (like the one in this project) from the very start
rather than bolting it on after something broke.
