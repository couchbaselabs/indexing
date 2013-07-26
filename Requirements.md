Requirements
============
This document tracks the high level requirements for Indexing from the Query language.

- A requirement does not mean we know that it can be implemented
- The document first outlines requriements of each area in isolation
- Then composite requirements or relaxations follow

Consistency
-----------
Without considering Atomicity, Isolation or Durability requirements, that is, assuming we are working on a single system running statements in serial order, and experiences no failures -

1. Indexes should be immediately consistent with updates
2. Reads should precede Writes in DML updates
3. Updates should execute in O(m log n)
4. Selects following updates should execute in O(m log n)

As an illustration:

1. CREATE INDEX ON contacts.name
2. UPDATE contacts SET currency = "Euro" WHERE location = "UK"
3. SELECT * FROM contacts WHERE currency = "Euro"

If 'm' is the number of updated/fetched documents, and 'n' is the total number of documents:

1. Statement (2) would take O(m log n) time in the worst case
2. Statement (3) would always return UK contacts.
3. Statement (3) would take O(m log n) time in the worst case

Atomicity
---------
It is unclear what Atomicity means for indexes without discussing Isolation properties. It is however agreed that IF there is a known failure before an index query begins, then:

1. Indexes can fail the query or block longer than O(m log n) provided it indicates it MAY do so at start of the read
2. Too wide / too narrow indexes results are permissible when if it does not affect correctness of future queries
