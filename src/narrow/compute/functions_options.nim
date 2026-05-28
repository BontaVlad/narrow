import ../core/[ffi, utils]
import ./sorting

# ============================================================================
# RoundMode Enum
# ============================================================================

type RoundMode* = enum
  rmDown = GARROW_ROUND_DOWN.int
  rmUp = GARROW_ROUND_UP.int
  rmTowardsZero = GARROW_ROUND_TOWARDS_ZERO.int
  rmTowardsInfinity = GARROW_ROUND_TOWARDS_INFINITY.int
  rmHalfDown = GARROW_ROUND_HALF_DOWN.int
  rmHalfUp = GARROW_ROUND_HALF_UP.int
  rmHalfTowardsZero = GARROW_ROUND_HALF_TOWARDS_ZERO.int
  rmHalfTowardsInfinity = GARROW_ROUND_HALF_TOWARDS_INFINITY.int
  rmHalfToEven = GARROW_ROUND_HALF_TO_EVEN.int
  rmHalfToOdd = GARROW_ROUND_HALF_TO_ODD.int

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

    StrftimeOptions* = object
      handle*: ptr GArrowStrftimeOptions

    PadOptions* = object
      handle*: ptr GArrowPadOptions

    ModeOptions* = object
      handle*: ptr GArrowModeOptions

    QuantileOptions* = object
      handle*: ptr GArrowQuantileOptions

    TDigestOptions* = object
      handle*: ptr GArrowTDigestOptions

    SelectKOptions* = object
      handle*: ptr GArrowSelectKOptions

    TrimOptions* = object
      handle*: ptr GArrowTrimOptions

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

proc newStrftimeOptions*(): StrftimeOptions =
  result.handle = garrow_strftime_options_new()

proc newPadOptions*(): PadOptions =
  result.handle = garrow_pad_options_new()

proc newModeOptions*(): ModeOptions =
  result.handle = garrow_mode_options_new()

proc newQuantileOptions*(): QuantileOptions =
  result.handle = garrow_quantile_options_new()

proc getQs*(opts: QuantileOptions): seq[float64] =
  var nq: gsize = 0
  let raw = garrow_quantile_options_get_qs(opts.handle, addr nq)
  result = newSeq[float64](nq.int)
  if nq > 0:
    let arr = cast[ptr UncheckedArray[gdouble]](raw)
    for i in 0 ..< nq.int:
      result[i] = arr[i]

proc setQ*(opts: var QuantileOptions, q: float64) =
  garrow_quantile_options_set_q(opts.handle, q.cdouble)

proc newTDigestOptions*(): TDigestOptions =
  result.handle = garrow_tdigest_options_new()

proc getQs*(opts: TDigestOptions): seq[float64] =
  var nq: gsize = 0
  let raw = garrow_tdigest_options_get_qs(opts.handle, addr nq)
  result = newSeq[float64](nq.int)
  if nq > 0:
    let arr = cast[ptr UncheckedArray[gdouble]](raw)
    for i in 0 ..< nq.int:
      result[i] = arr[i]

proc setQ*(opts: var TDigestOptions, q: float64) =
  garrow_tdigest_options_set_q(opts.handle, q.cdouble)

proc newSelectKOptions*(): SelectKOptions =
  result.handle = garrow_select_k_options_new()

proc getSortKeys*(opts: SelectKOptions): ptr GList =
  garrow_select_k_options_get_sort_keys(opts.handle)

proc addSortKey*(opts: var SelectKOptions, key: SortKey) =
  garrow_select_k_options_add_sort_key(opts.handle, key.handle)

proc newTrimOptions*(): TrimOptions =
  result.handle = garrow_trim_options_new()
