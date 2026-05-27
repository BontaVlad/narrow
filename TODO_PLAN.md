# Narrow Roadmap — Pragmatic TODO Plan

> Deep-dive codebase review, May 2026.
> Priority: **real user gaps → important missing → polish → nice-to-have**.
> ~339 of ~1,649 C symbols wrapped. ~710 substantive functions remain unwrapped (rest are `get_type` boilerplate).
>
> **Last reviewed:** 2026-05-27 (2.2-2.4, 3.1-3.5 done)

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

### 1.1 Hash Join ✅ DONE (2026-05-27)

- **[x]** Wrap `garrow_hash_join_node_options_new`, `garrow_hash_join_node_options_set_left_outputs`, `garrow_hash_join_node_options_set_right_outputs`, `garrow_execute_plan_build_hash_join_node`.
- **[x]** Add `HashJoinNodeOptions` type, `buildHashJoinNode`, and high-level `joinTables()` convenience with `JoinType` enum.
- **[x]** Test: 9 tests in `tests/test_acero_join.nim` covering inner/outer/full/semi/anti joins, multi-key, and empty results.
- **Effort**: Medium (~3 days) | **Impact**: Critical — joins are fundamental analytics primitive.
- **Files**: `src/narrow/compute/acero.nim` (+95 lines), `tests/test_acero_join.nim` (new, 202 lines)

Note: Arrow GLib 24.0.0 does not deduplicate key columns by default — both left and right key columns appear in results. Use `setLeftOutputs`/`setRightOutputs` for column projection.

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

### 2.1 Decimal128 / Decimal256 Arrays ✅ DONE (2026-05-27)

- **[x]** Wrap Decimal128 and Decimal256 value types, data types, arrays, array builders — ~156 FFI functions.
- **[x]** `Decimal128` and `Decimal256` value types with string/int64 constructors, `$`, `toBytes`, `toInt` (Decimal128 only), comparisons (`==`, `<`, `<=`, `>`, `>=`), arithmetic (`+`, `-`, `*`, `/`, `abs`, `negate`), `rescale`.
- **[x]** `Decimal128Array` / `Decimal256Array` + builders with precision/scale metadata, `append` (string/int64/Decimal128), `appendNull`, `finish`, `[]`, `len`, `isNull`, `formatValue`.
- **[x]** Test: 37 tests in `tests/test_gdecimal.nim` covering all value type ops, array building, nulls, indexing, formatValue, rescale.
- **Effort**: Low (~1 day) | **Impact**: Medium — financial / precision-critical use cases.
- **Files**: `src/narrow/types/gdecimal.nim` (new, 328 lines), `tests/test_gdecimal.nim` (new, 238 lines)
- **Note**: `$` on standalone Decimal128/256 returns raw integer without scale (Arrow design). Use `arr.formatValue(i)` for scale-formatted output. Decimal256 is missing `minus`, `toInt` from the C FFI.

### 2.2 Compressed Input/Output Streams ✅ DONE (2026-05-27)

- **[x]** `Codec` type (arcGObject) + `newCodec`, `name`, `compressionType`, `level`, `codecFromExtension` mapping.
- **[x]** `newCompressedInputStream`/`newCompressedOutputStream` return regular `InputStream`/`OutputStream` (cast-based, no new stream types). All existing stream methods work for free.
- **[x]** FileSystem convenience procs: `openCompressedInputStream`/`OutputStream` with auto-detection from file extension (`.gz`→gzip, `.zst`→zstd, `.bz2`→bz2) or explicit `Codec` parameter.
- **[x]** Test: 18 tests in `tests/test_compressed.nim` covering codec creation, extension inference, gzip/zstd/bz2 round-trips, snappy/lz4 streaming limitation, empty data, large data, multi-write.
- **Effort**: Low (~1 day) | **Impact**: Medium — cookbook has "Reading/Writing Compressed Data" recipes.
- **Files**: `src/narrow/io/compressed.nim` (new, 61 lines), `tests/test_compressed.nim` (new, 186 lines)
- **Note**: Only gzip, zstd, bz2 support streaming compression. Snappy, lz4, brotli, lzo — codec creation works but `garrow_compressed_output_stream_new` returns "NotImplemented".

### 2.3 Half-Float Array Support ✅ DONE (2026-05-27)

- **[x]** `HalfFloat = distinct uint16` value type with borrowed `==`, `<`, `<=` comparisons.
- **[x]** `HGFloatArray` and `HGFloatArrayBuilder` types (`arcGObject`) + `HGFloatScalar`.
- **[x]** Builder: `newHalfFloatArrayBuilder`, `append(HalfFloat)`, `appendNull` (via generic `garrow_array_builder_append_null`), `appendValues`, `finish`.
- **[x]** Array: `newHalfFloatArray(seq)`/`(seq, mask)`, `[]`/`len`/`isNull`/`toSeq`/`@`/`items`/`$`.
- **[x]** Scalar: `newHalfFloatScalar(HalfFloat)`, `getValue`.
- **[x]** Data type: `newHalfFloatGType()` returns `GADType`.
- **[x]** Test: 19 tests in `tests/test_half_float.nim`.
- **Effort**: Low (~1 day) | **Impact**: Medium — ML model weights, GPU interop, memory-constrained edge devices.
- **Files**: `src/narrow/types/gtypes.nim` (+7 lines), `src/narrow/column/primitive.nim` (+105 lines), `tests/test_half_float.nim` (new, 134 lines)
- **Note**: No native Nim float16. Values are raw IEEE 754 half-precision bit patterns stored as `uint16`. Conversion to/from float32 requires external library or manual bit manipulation. `garrow_half_float_array_builder_append_null` does not exist — use generic `garrow_array_builder_append_null` with cast.

### 2.4 S3 Filesystem ✅ DONE (2026-05-27)

- **[x]** `S3GlobalOptions` type (arcGObject) + `newS3GlobalOptions`, `isS3Enabled`, `initializeS3()` (default)/`initializeS3(options)`, `finalizeS3`.
- **[x]** No per-property setters/getters in FFI — use GObject property system (`g_object_set`/`g_object_get`) for advanced config.
- **[x]** Test: 2 tests in `tests/test_filesystem.nim`.
- **Effort**: Medium (~2–3 days) | **Impact**: Medium — cloud data access.
- **Files**: `src/narrow/io/filesystem.nim` (+27 lines), `tests/test_filesystem.nim` (+9 lines)
- **Note**: S3 may not be compiled into Arrow GLib or credentials may be missing. Tests only verify type creation and `isS3Enabled` doesn't crash.

---

## Tier 3 — Polish & Cleanup

Low effort, medium impact. They fix rough edges, skipped tests, and incomplete implementations.

### 3.1 Fix Skipped Tests ✅ DONE (2026-05-27)

- **[x] Table filter by BooleanArray**: `test_filters.nim:166` — unskipped, real test with table + BooleanArray mask.
- **[x] Parquet writeRecordBatch**: `test_parquet.nim:272` — unskipped, uses `RecordBatchBuilder.columnBuilder[int32]` + `flush`.
- **[x] Parquet newRowGroup**: `test_parquet.nim:277` — wrapped `writeChunkedArray` in parquet.nim, unskipped test.
- **Effort**: Trivial | **Files**: `tests/test_filters.nim`, `tests/test_parquet.nim`, `src/narrow/io/parquet.nim`

### 3.2 Schema Metadata (Key-Value) ✅ DONE (2026-05-27)

- **[x]** `hasMetadata`, `getMetadata` (returns `Table[string, string]`), `getMetadataValue(key)` (returns `Option[string]`), `withMetadata(openArray[(string, string)])`, `toString(schema, showMetadata)`.
- **[x]** `addField(i, field)`, `removeField(i)` — return new schemas (immutable).
- **[x]** Test: 12 tests in `tests/test_gschema.nim` (8 metadata + 4 field editing).
- **Effort**: Low | **Files**: `src/narrow/column/metadata.nim` (+57 lines), `tests/test_gschema.nim` (+79 lines)
- **Note**: Empty metadata array → `hasMetadata = false` (Arrow convention). `g_str_hash`/`g_str_equal` for GHashTable string keys. GHashTable is created/destroyed in `withMetadata`; `getMetadata` reads from borrowed hash table.

### 3.3 RecordBatch Sort / Take / Filter ✅ DONE (2026-05-27)

- **[x]** `sortIndices(rb, keys)`, `take(rb, indices)`, `sortBy(rb, keys)`, `filter(rb, mask, options)`, `filter(rb, mask)`.
- **[x]** Requires `ensureComputeInitialized()` — RecordBatch FFI (`garrow_record_batch_sort_indices` etc.) dispatches through Arrow's compute function registry.
- **[x]** Test: 8 tests in `tests/test_recordbatch.nim` (6 sort/take + 2 filter).
- **Effort**: Low (~2 hrs) | **Files**: `src/narrow/compute/sorting.nim` (+17 lines), `src/narrow/compute/filters.nim` (+12 lines), `tests/test_recordbatch.nim` (+77 lines)

### 3.5 Parquet Column-Level Writing ✅ DONE (2026-05-27)

- **[x]** `newBufferedRowGroup(fw)` — wraps `gparquet_arrow_file_writer_new_buffered_row_group` (Arrow-managed memory for column-by-column writes).
- **[x]** `writeRecordBatch(fw, rb)` — direct RecordBatch write to existing FileWriter.
- **[x]** Combined with existing `newRowGroup` + `writeChunkedArray`, gives full column-level Parquet writing.
- **Effort**: Low (~1 day) | **Files**: `src/narrow/io/parquet.nim` (+7 lines)

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
         → Decimal ✅, Compressed ✅, Half-float ✅

Week 4: S3 filesystem + Acero Project node + Schema metadata (tier 2 #4, tier 3 #2, #4)
        → S3 ✅, Project node ✅, Schema metadata ✅

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
| 2.2 | Compressed Streams ✅ | Low | Medium | 2 |
| 2.3 | Half-Float Arrays ✅ | Low | Medium | 2 |
| 2.4 | S3 Filesystem ✅ | Medium | Medium | 2 |
| 3.1 | Fix Skipped Tests ✅ | Trivial | Low-Med | 3 |
| 3.2 | Schema Metadata ✅ | Low | Medium | 3 |
| 3.3 | RecordBatch Sort/Take/Filter ✅ | Low | Medium | 3 |
| 3.4 | Acero Project Node ✅ | Low | Medium | 3 |
| 3.5 | Parquet Column Writes ✅ | Low | Medium | 3 |
| 3.6 | Array Statistics | Low | Medium | 3 |
| 3.7 | GAList Tests | Trivial | Low | 3 |
| 3.8 | Table Sort/Take Native | Low | Low | 3 |
| 3.9 | Dead Module Cleanup | Trivial | Low | 3 |
| 4.1–12 | Dictionary, REE, Union, etc. | Medium | Low | 4 |
