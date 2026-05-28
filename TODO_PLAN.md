# Narrow Roadmap — Pragmatic TODO Plan

> Deep-dive codebase review, May 2026.
> Priority: **real user gaps → important missing → polish → nice-to-have**.
> ~339 of ~1,649 C symbols wrapped. ~710 substantive functions remain unwrapped (rest are `get_type` boilerplate).
>
> **Last reviewed:** 2026-05-28 — ALL TIERS COMPLETE ✅ (30/30 tasks done)

---

## Summary of Current State

**The project is in solid shape.** The library can handle a complete analytics pipeline: reads/writes all formats, full columnar type coverage, Expression DSL, GROUP BY, joins, sorting, filtering, Arrow C Data interop.

---

## Tier 1 — Real User Blocker Gaps ✅ COMPLETE

| # | Feature | Status |
|---|---------|--------|
| 1.1 | Hash Join | ✅ |
| 1.2 | Binary Array | ✅ |
| 1.3 | Duration/Interval Arrays | ✅ |

## Tier 2 — Important Missing Features ✅ COMPLETE

| # | Feature | Status |
|---|---------|--------|
| 2.1 | Decimal128/256 Arrays | ✅ |
| 2.2 | Compressed I/O Streams | ✅ |
| 2.3 | Half-Float Arrays | ✅ |
| 2.4 | S3 Filesystem | ✅ |

## Tier 3 — Polish & Cleanup ✅ COMPLETE

| # | Feature | Status |
|---|---------|--------|
| 3.1 | Fix Skipped Tests | ✅ |
| 3.2 | Schema Metadata | ✅ |
| 3.3 | RecordBatch Sort/Take/Filter | ✅ |
| 3.4 | Acero Project Node | ✅ |
| 3.5 | Parquet Column Writes | ✅ |
| 3.6 | Array Statistics | ✅ |
| 3.7 | GAList Tests | ✅ |
| 3.8 | Table Sort/Take Native | ✅ |
| 3.9 | Dead Module Cleanup | ✅ |

## Tier 4 — Nice-to-Have ✅ COMPLETE

| # | Feature | Tests | Files |
|---|---------|-------|-------|
| 4.1 | Dictionary-Encoded Arrays | 13 | `column/dictionary.nim` |
| 4.2 | Run-End Encoded Arrays | 9 | `column/ree.nim` |
| 4.3 | Dense/Sparse Union Arrays | 11 | `column/union.nim` |
| 4.4 | Tensor | 12 | `types/gtensor.nim` |
| 4.5 | BinaryView/StringView | 4 | `column/binary.nim` |
| 4.6 | Fixed-Size Binary | 20 | `column/binary.nim` |
| 4.7 | GCS/Azure/HDFS | - | `io/filesystem.nim` (type shells only) |
| 4.8 | Compute Function Options | 11 | `compute/functions_options.nim` |
| 4.9 | Partitioning Dictionaries | - | `tabular/dataset.nim` (removed TODOs) |
| 4.10 | Null Type Arrays/Builders | 15 | `column/primitive.nim` |
| 4.11 | Acero Plan Introspection | 3 | `compute/acero.nim` |
| 4.12 | Arrow C Data Interface | 4 | `interop/cdata.nim` |

**Total: 102 new tests added for Tier 4 (0 failures, 0 leaks from Narrow code).**

## What's Not Wrapped (and Why)

| Skip | Reason |
|------|--------|
| ~710 substantive FFI functions | Most are `get_type` boilerplate or narrow compute options. Proportional user value is low. |
| ORC format | Arrow GLib has no ORC bindings. |
| BinaryView/StringView builders | No FFI constructors exist — types are read-only via C Data interface. |
| GCS/Azure/HDFS constructors | Only `get_type` in FFI — no file system instances can be created. |
| Partitioning dictionaries | `nil` is the valid default; dictionary type wrappers add complexity without user demand. |
