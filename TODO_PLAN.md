# Narrow Roadmap — Pragmatic TODO Plan

> Deep-dive codebase review, May 2026.
> Priority: **real user gaps → important missing → polish → nice-to-have**.
> ~339 of ~1,649 C symbols wrapped. ~710 substantive functions remain unwrapped (rest are `get_type` boilerplate).
>
> **Last reviewed:** 2026-05-27

---

## Summary of Current State

**The project is in solid shape.** The library can already handle a complete analytics pipeline:

- **Read/write**: Parquet, CSV, Feather, IPC, JSON — with row-group statistics, filter push-down, compression
- **Columnar types**: All primitives (bool, int8–64, uint8–64, float32/64), strings, nested (struct, list, map), temporal (date32/64, timestamp, time32/64), UUID, fixed-shape tensors
- **Schema/table**: Full `Schema`/`Field`/`ArrowTable`/`RecordBatch` with builders, slicing, concatenation, validation
- **Filtering**: Rich Expression DSL (`col()`, `==`, `!=`, `<`, `>`, `and`, `or`, `not`, `+`, `-`, `*`, `/`, `contains`, `startsWith`, `endsWith`, `matchSubstringRegex`, `toUpper`, `toLower`, `strLength`), filter parsing from strings, row-group statistics simplification
- **Aggregation**: Sum, mean, count, countValues across all numeric types, element-wise arithmetic/comparison
- **GROUP BY**: Acero-based aggregate node with fluent `table.groupBy("keys").aggregate(...)` API
- **Sort/Take/Cast**: `sortIndices`, `sortBy`, `take`, `castTo`/`viewAs` for arrays, chunked arrays, and tables
- **Compute function registry**: `call()`, `execute()`, `find()`, `listFunctions()` — generic access to all Arrow kernels
- **Dataset API**: Scanner, Hive/Directory partitioning, `writeDataset` with partitioning
- **Filesystem**: Local filesystem, URIs, streams (input/output/seekable), file info, selectors
- **Memory safety**: ARC/ORC integration via `arcGObject`/`arcRef` macros, comprehensive memory stress tests
- **CI**: Ubuntu (ASAN), macOS, Windows — 40 test files, ~3,370 tests

**What blocks real-world users right now**: Joins, binary data, duration/interval arrays, decimal types.

---

## Tier 1 — Real User Blocker Gaps

These are **substantial missing features** that block real analytics workflows. FFI exists, Nim wrappers don't.

### 1.1 Hash Join

- [ ] Wrap `garrow_hash_join_node_options_new`, `garrow_hash_join_node_options_set_left_outputs`, `garrow_hash_join_node_options_set_right_outputs`, `garrow_execute_plan_build_hash_join_node` (~7 FFI functions).
- [ ] Add `HashJoinNodeOptions` type, `buildHashJoinNode`, and high-level `joinTables()` convenience.
- [ ] Test: inner/left/right join on two tables by key column.
- **Effort**: Medium (~3 days) | **Impact**: Critical — joins are fundamental analytics primitive.
- **Files**: `src/narrow/compute/acero.nim`, `tests/test_acero_join.nim`

### 1.2 Binary Array Wrappers ✅ DONE (2026-05-27)

- **[x]** Wrap `garrow_binary_array_builder_*` (append, appendNull, appendValue, appendValues, new), `garrow_binary_array_*` (getValue, getBuffer, getOffsetsBuffer, new) — added `seq[byte]` branches to generic `Array[T]`/`ArrayBuilder[T]` in `primitive.nim`.
- **[x]** Test: 15 tests in `tests/test_binary.nim` covering creation, indexing, nulls, slicing, toSeq, iteration, memory stress.
- **Effort**: Medium (~2–3 days) | **Impact**: High — essential for serialized protobuf, image bytes, cryptographic hashes.
- **Files**: `src/narrow/column/primitive.nim`, `tests/test_binary.nim`

### 1.3 Duration / Interval Array Wrappers ✅ DONE (2026-05-27)

- **[x]** Duration arrays: `DurationArray` + `DurationArrayBuilder` with unit support, `append(Option[Duration])`, `appendValues`, `finish`, `[]`, `isNull`, `$`.
- **[x]** MonthInterval arrays: `MonthIntervalArray` + `MonthIntervalArrayBuilder`, `append(Option[MonthInterval])`, `appendValues`.
- **[x]** DayTimeInterval arrays: `DayTimeIntervalArray` + `DayTimeIntervalArrayBuilder`, value extraction via `g_object_get` (GObject property access).
- **[x]** MonthDayNanoInterval arrays: `MonthDayNanoIntervalArray` + `MonthDayNanoIntervalArrayBuilder`, value extraction via `g_object_get`.
- **[x]** Test: 70 tests total in `tests/test_temporal_extended.nim` covering all 4 new types.
- **Effort**: Medium (~2 days) | **Impact**: High — cannot create columns of Duration or interval arrays today. Type constructors exist in `gtemporal.nim` but no array/builder wrappers.
- **Files**: `src/narrow/types/gtemporal.nim`, `tests/test_temporal_extended.nim`

Note: LargeBinary/LargeString/LargeList remain unwrapped (~43 FFI functions). These need distinct Nim types since they use different GArrowLarge* handles.

---

## Tier 2 — Important Missing Features

These have fully available FFI and follow existing patterns. Each is 1–2 days.

### 2.1 Decimal128 / Decimal256 Arrays

- [ ] Wrap decimal128 and decimal256 array builders, arrays, data types, scalars — ~136 FFI functions across 4 widths.
- [ ] Follow the existing primitive array wrapper pattern in `column/primitive.nim`.
- [ ] Generic `DecimalArray[W]` type parameterized by width (or separate `Decimal128Array`/`Decimal256Array`).
- [ ] Arithmetic: `+`, `-`, `*`, `/`, `rescale`, `abs`, `negate`.
- [ ] Test: create decimal array, arithmetic, null handling, scale/precision.
- **Effort**: Low (~1–2 days) | **Impact**: Medium — financial / precision-critical use cases.
- **Files**: `src/narrow/types/gdecimal.nim` (new), `tests/test_decimal.nim`

### 2.2 Compressed Input/Output Streams

- [ ] Wrap `garrow_codec_new`, `garrow_codec_get_name`, `garrow_codec_get_compression_type`, `garrow_compressed_input_stream_new`, `garrow_compressed_output_stream_new` — ~7 FFI functions.
- [ ] Users can already write compressed Parquet via `WriterProperties`, but cannot decompress raw `.gz`/`.bz2`/`.lz4`/`.zst` CSV or other formats.
- [ ] Test: write and read compressed CSV round-trip.
- **Effort**: Low (~1 day) | **Impact**: Medium — cookbook has "Reading/Writing Compressed Data" recipes.
- **Files**: `src/narrow/io/compressed.nim` (new), `tests/test_compressed.nim`

### 2.3 Half-Float Array Support

- [ ] Wrap `garrow_half_float_array_builder_*`, `garrow_half_float_array_*`, `garrow_half_float_data_type_new`, `garrow_half_float_scalar_*` — ~13 FFI functions.
- [ ] Test: create half-float array, value extraction, null handling.
- **Effort**: Low (~1 day) | **Impact**: Medium — ML model weights, GPU interop.
- **Files**: `src/narrow/column/primitive.nim` (extend), `tests/test_garray.nim` (extend)

### 2.4 S3 Filesystem

- [ ] Wrap `garrow_s3_global_options_*`, `garrow_s3_is_enabled`, `garrow_s3_initialize`, `garrow_s3_finalize`.
- [ ] `GArrowS3FileSystem` is available via GType. Wire up credential/region handling.
- [ ] The cookbook S3 test is already skipped (`test_cookbook.nim:182`).
- [ ] Test: skipped in CI (no S3 endpoint available), but ensure compilation and smoke test with mock.
- **Effort**: Medium (~2–3 days) | **Impact**: Medium — cloud data access.
- **Files**: `src/narrow/io/filesystem.nim` (extend), `tests/test_filesystem.nim` (extend)

---

## Tier 3 — Polish & Cleanup

Low effort, medium impact. They fix rough edges, skipped tests, and incomplete implementations.

### 3.1 Fix Skipped Tests

- [ ] **Table filter by BooleanArray**: `test_filters.nim:169` — `skip()` but the `filter(table, BooleanArray)` proc already exists. Trivial to implement.
- [ ] **Parquet writeRecordBatch**: `test_parquet.nim:274` — `RecordBatchBuilder.columnBuilder` export should work. Verify and bring back.
- [ ] **Parquet newRowGroup**: `test_parquet.nim:277` — needs `writeColumnData` wrapper first (see 3.5 below).
- [ ] **Cookbook S3**: `test_cookbook.nim:182` — blocked by S3 filesystem (see 2.4).
- **Effort**: Trivial (~1 hr each for the first two) | **Files**: `tests/test_filters.nim`, `tests/test_parquet.nim`

### 3.2 Schema Metadata (Key-Value)

- [ ] Wrap `garrow_schema_get_metadata`, `garrow_schema_has_metadata`, `garrow_schema_to_string_metadata`, `garrow_schema_with_metadata`, `garrow_schema_add_field`, `garrow_schema_remove_field` — ~6 FFI functions.
- [ ] Users cannot attach or read custom key-value metadata on schemas.
- [ ] Test: add metadata to schema, read it back, schema equality with metadata.
- **Effort**: Low (~1 hr) | **Files**: `src/narrow/column/metadata.nim`, `tests/test_gschema.nim`

### 3.3 RecordBatch Sort / Take / Filter

- [ ] FFI exists: `garrow_record_batch_sort_indices`, `garrow_record_batch_take`, `garrow_record_batch_filter`, `garrow_record_batch_serialize`, `garrow_record_batch_export`.
- [ ] Only array and table versions are currently wrapped — `RecordBatch` is left out.
- [ ] Test: sort/take/filter record batch, verify consistency with table-level equivalents.
- **Effort**: Low (~2 hrs) | **Files**: `src/narrow/tabular/batch.nim`, `tests/test_recordbatch.nim`

### 3.4 Acero Project Node

- [ ] Wrap `garrow_project_node_options_new`, `garrow_project_node_options_get_expressions`, `garrow_execute_plan_build_project_node` — ~3 FFI functions.
- [ ] Column selection/projection is a basic operation in any execution plan.
- [ ] Add `ProjectNodeOptions` + `buildProjectNode`.
- [ ] Test: source → project → sink pipeline selecting a subset of columns.
- **Effort**: Low (~1 day) | **Files**: `src/narrow/compute/acero.nim`, `tests/test_acero.nim`

### 3.5 Parquet Column-Level Writing

- [ ] `FileWriter.newRowGroup()` exists but is unusable without a `writeColumnData` wrapper.
- [ ] Wrap `gparquet_column_chunk_writer_write_data` or the per-column write APIs to enable row-group construction from individual columns.
- [ ] Test: write a table column-by-column via row groups.
- **Effort**: Low (~1 day) | **Files**: `src/narrow/io/parquet.nim`, `tests/test_parquet.nim`

### 3.6 Array Statistics

- [ ] Wrap `garrow_array_get_statistics`, `garrow_array_statistics_get_n_nulls`, `garrow_array_statistics_get_distinct_count`, `garrow_array_statistics_is_exact` — ~12 FFI functions.
- [ ] Useful for data profiling: distinct counts, null counts with exact/approximate.
- [ ] Test: compute statistics on arrays with nulls and duplicates.
- **Effort**: Low (~1 hr) | **Files**: `src/narrow/compute/statistics.nim` (extend), `tests/test_parquet_statistics.nim` (extend)

### 3.7 GAList Tests

- [ ] `types/glist.nim` is the only wrapped module with zero test coverage.
- [ ] Test: `GAList[T]` creation, append/prepend, indexing, iteration, `toSeq`, memory management.
- [ ] Low risk (it's a GLib linked list utility used internally) but still needs coverage.
- **Effort**: Trivial | **Files**: `tests/test_glist.nim` (new)

### 3.8 Table Sort / Take via Native Arrow Functions

- [ ] `garrow_table_sort_indices` and `garrow_table_take` exist separately from the compute-kernel versions.
- [ ] Compare performance against current compute-kernel-based sort/take. Wrap whichever is faster.
- [ ] Test: table sort/take with null handling, multi-key sort.
- **Effort**: Low (~2 hrs) | **Files**: `src/narrow/compute/sorting.nim` (extend), `tests/test_sorting.nim` (extend)

### 3.9 Dead Module Cleanup

- [ ] `core/back_generated.nim`: 30,572 lines. Appears to be an old backup of `generated.nim`. Remove if unused.
- [ ] Empty legacy files (`src/grecordbatch.nim`, `src/gtypes.nim`): 0 bytes each. Remove.
- **Effort**: Trivial | **Files**: as above

---

## Tier 4 — Nice-to-Have / Longer Tail

Available in FFI but lower user demand or higher implementation complexity.

### 4.1 Dictionary-Encoded Arrays

- [ ] Wrap `garrow_dictionary_array_*`, `garrow_dictionary_data_type_*`, `garrow_dictionary_encode_options_*` — ~13 FFI functions.
- [ ] Useful for low-cardinality string compression.
- **Files**: `src/narrow/column/dictionary.nim` (new)

### 4.2 Run-End Encoded Arrays

- [ ] Wrap `garrow_run_end_encoded_array_*`, `garrow_run_end_encoded_data_type_*` — ~15 FFI functions.
- [ ] Specialized Arrow compression format for repeated values.
- **Files**: `src/narrow/column/ree.nim` (new)

### 4.3 Dense / Sparse Union Arrays

- [ ] Wrap `garrow_dense_union_*`, `garrow_sparse_union_*`, `garrow_union_scalar_*` — ~19 FFI functions.
- [ ] Niche Arrow feature. Rarely needed in analytical workloads.
- **Files**: `src/narrow/column/union.nim` (new)

### 4.4 Tensor

- [ ] Wrap `garrow_tensor_*` — ~15 FFI functions.
- [ ] For ML/DL workflows. Shape, strides, contiguity, dimension names.
- **Files**: `src/narrow/types/gtensor.nim` (new)

### 4.5 BinaryView / StringView

- [ ] Wrap `garrow_binary_view_*`, `garrow_string_view_*` — ~8 FFI functions.
- [ ] New Arrow format for zero-copy string operations. Forward-looking.
- **Files**: `src/narrow/column/binary.nim` (extend)

### 4.6 Fixed-Size Binary

- [ ] Wrap `garrow_fixed_size_binary_*` — ~16 FFI functions.
- [ ] Specialized binary format with fixed-width records.
- **Files**: `src/narrow/column/binary.nim` (extend)

### 4.7 GCS / Azure / HDFS Filesystems

- [ ] GType wrappers exist in `generated.nim`: `garrow_gcs_file_system_get_type`, `garrow_azure_file_system_get_type`, `garrow_hdfs_file_system_get_type`.
- [ ] Like S3 but for other cloud providers. Lower demand.
- **Files**: `src/narrow/io/filesystem.nim` (extend)

### 4.8 Typed Compute Function Options

- [ ] ~130+ options types available in FFI: SortOptions, RankOptions, QuantileOptions, TDigestOptions, SelectKOptions, RoundOptions, PadOptions, TrimOptions, ReplaceOptions, ExtractRegexOptions, SplitOptions, StrftimeOptions, StrptimeOptions, UTF8NormalizeOptions, CumulativeOptions, PairwiseOptions, ModeOptions, VarianceOptions, IndexOptions, SetLookupOptions, SkewOptions, WinsorizeOptions, etc.
- [ ] Generic `FunctionOptions()` already works with defaults. Typed wrappers add discoverability and compile-time validation.
- [ ] Add on-demand as users request specific functions.
- **Files**: `src/narrow/compute/functions_options.nim` (new)

### 4.9 Partitioning Dictionary Support

- [ ] 4 TODO comments in `dataset.nim`: `# TODO: implement dictionaries for partitioning`.
- [ ] `newDirectoryPartitioning` and `newHivePartitioning` always pass `nil` for dictionaries parameter.
- [ ] Partitioning works correctly without it — this is a feature gap, not a bug.
- **Files**: `src/narrow/tabular/dataset.nim`

### 4.10 Null Type Arrays / Builders

- [ ] Wrap `garrow_null_array_builder_*`, `garrow_null_array_*`, `garrow_null_data_type_new` — ~11 FFI functions.
- [ ] For columns that are entirely null (schema placeholders, etc.).
- **Files**: `src/narrow/column/primitive.nim` (extend)

### 4.11 Acero Plan Introspection

- [ ] Wrap `garrow_execute_plan_get_nodes`, `garrow_execute_node_get_kind_name`, `garrow_execute_node_options_get_type` — ~3 FFI functions.
- [ ] Nice for debugging execution plans, but low user-facing value.
- **Files**: `src/narrow/compute/acero.nim` (extend)

### 4.12 Arrow C Data Interface (Export/Import)

- [ ] Wrap `garrow_schema_export`, `garrow_record_batch_export`, `garrow_record_batch_reader_export`/`_import`.
- [ ] Zero-copy data sharing across process/language boundaries.
- [ ] Low user demand unless interop with other Arrow bindings is needed.
- **Files**: `src/narrow/interop/` (new)

---

## Recommended Execution Order

```
Week 1: Binary Array + Duration/Interval arrays (tier 1, #2-3)
        → Core type coverage. Users can work with binary and temporal data.

Week 2: Hash Join (tier 1, #1)
        → THE biggest missing feature. Unlocks combined-dataset workflows.

Week 3: Decimal types + Half-float + Compressed streams (tier 2, #1-3)
        → Financial + ML + compressed data use cases.

Week 4: S3 filesystem + Acero Project node + Schema metadata (tier 2 #4, tier 3 #2, #4)
        → Cloud access, plan projection, schema enrichment.

Week 5+: Fix skipped tests, RecordBatch sort/take/filter, array statistics,
         GAList, dead module cleanup (tier 3, #1, #3, #5, #6, #7, #9)
        → Code quality, consistency, coverage.

On-demand: Tier 4 items (dictionary, REE, union, tensor, view types, compute options).
```

---

## What NOT to Prioritize (and Why)

| Skip | Reason |
|------|--------|
| 100% FFI coverage (~710 substantive functions) | Most are `get_type` boilerplate or narrow compute options. Would bloat the library without proportional user value. |
| Union / Tensor / REE types | Niche Arrow features. Wrap when a user asks. |
| ORC format | Arrow GLib has no ORC bindings in `generated.nim`. Can't do it. |
| Acero introspection (`get_nodes`, `get_kind_name`) | Nice for debugging but low user-facing value. |
| GCS / Azure / HDFS filesystems | Lower demand than S3. |

---

## Gain / Effort Matrix

| # | Feature | Effort | Impact | Tier |
|---|---------|--------|--------|------|
| 1.1 | Hash Join | Medium | Critical | 1 |
| 1.2 | Binary Array | Medium | High | 1 |
| 1.3 | Duration/Interval Arrays | Medium | High | 1 |
| 2.1 | Decimal Arrays | Low | Medium | 2 |
| 2.2 | Compressed Streams | Low | Medium | 2 |
| 2.3 | Half-Float Arrays | Low | Medium | 2 |
| 2.4 | S3 Filesystem | Medium | Medium | 2 |
| 3.1 | Fix Skipped Tests | Trivial | Low-Med | 3 |
| 3.2 | Schema Metadata | Low | Medium | 3 |
| 3.3 | RecordBatch Sort/Take/Filter | Low | Medium | 3 |
| 3.4 | Acero Project Node | Low | Medium | 3 |
| 3.5 | Parquet Column Writes | Low | Medium | 3 |
| 3.6 | Array Statistics | Low | Medium | 3 |
| 3.7 | GAList Tests | Trivial | Low | 3 |
| 3.8 | Table Sort/Take Native | Low | Low | 3 |
| 3.9 | Dead Module Cleanup | Trivial | Low | 3 |
| 4.1–12 | Dictionary, REE, Union, etc. | Medium | Low | 4 |
