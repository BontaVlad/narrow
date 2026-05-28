import ../core/[ffi, error, utils]

# ============================================================================
# Compute Function Options
# ============================================================================
# CountOptions → aggregations.nim
# FilterOptions → filters.nim
# TakeOptions, SortKey, SortOptions → sorting.nim

arcGObject:
  type
    ArraySortOptions* = object
      handle*: ptr GArrowArraySortOptions

    SetLookupOptions* = object
      handle*: ptr GArrowSetLookupOptions

    VarianceOptions* = object
      handle*: ptr GArrowVarianceOptions

    RoundOptions* = object
      handle*: ptr GArrowRoundOptions

    IndexOptions* = object
      handle*: ptr GArrowIndexOptions

    JoinOptions* = object
      handle*: ptr GArrowJoinOptions

    WinsorizeOptions* = object
      handle*: ptr GArrowWinsorizeOptions

    ScalarAggregateOptions* = object
      handle*: ptr GArrowScalarAggregateOptions

    EqualOptions* = object
      handle*: ptr GArrowEqualOptions

proc newArraySortOptions*(order: GArrowSortOrder): ArraySortOptions =
  result.handle = garrow_array_sort_options_new(order)

proc newSetLookupOptions*(valueSet: ptr GArrowDatum): SetLookupOptions =
  result.handle = garrow_set_lookup_options_new(valueSet)

proc newVarianceOptions*(): VarianceOptions =
  result.handle = garrow_variance_options_new()

proc newRoundOptions*(): RoundOptions =
  result.handle = garrow_round_options_new()

proc newIndexOptions*(): IndexOptions =
  result.handle = garrow_index_options_new()

proc newJoinOptions*(): JoinOptions =
  result.handle = garrow_join_options_new()

proc newWinsorizeOptions*(): WinsorizeOptions =
  result.handle = garrow_winsorize_options_new()

proc newScalarAggregateOptions*(): ScalarAggregateOptions =
  result.handle = garrow_scalar_aggregate_options_new()

proc newEqualOptions*(): EqualOptions =
  result.handle = garrow_equal_options_new()
