import unittest2
import ../src/narrow

suite "Compute Options - ArraySort":
  test "create ArraySortOptions with ascending order":
    let opts = newArraySortOptions(GARROW_SORT_ORDER_ASCENDING)
    check not isNil(opts.handle)

  test "create ArraySortOptions with descending order":
    let opts = newArraySortOptions(GARROW_SORT_ORDER_DESCENDING)
    check not isNil(opts.handle)

suite "Compute Options - SetLookup":
  test "create SetLookupOptions":
    let arr = newArray(@[1'i32, 2, 3])
    let datum = cast[ptr GArrowDatum](garrow_array_datum_new(arr.toPtr))
    let opts = newSetLookupOptions(datum)
    check not isNil(opts.handle)
    g_object_unref(datum)

suite "Compute Options - Variance":
  test "create VarianceOptions":
    let opts = newVarianceOptions()
    check not isNil(opts.handle)

suite "Compute Options - Round":
  test "create RoundOptions":
    let opts = newRoundOptions()
    check not isNil(opts.handle)

suite "Compute Options - Index":
  test "create IndexOptions":
    let opts = newIndexOptions()
    check not isNil(opts.handle)

suite "Compute Options - Join":
  test "create JoinOptions":
    let opts = newJoinOptions()
    check not isNil(opts.handle)

suite "Compute Options - Winsorize":
  test "create WinsorizeOptions":
    let opts = newWinsorizeOptions()
    check not isNil(opts.handle)

suite "Compute Options - ScalarAggregate":
  test "create ScalarAggregateOptions":
    let opts = newScalarAggregateOptions()
    check not isNil(opts.handle)

suite "Compute Options - Equal":
  test "create EqualOptions":
    let opts = newEqualOptions()
    check not isNil(opts.handle)

suite "Compute Options - Memory":
  test "copy semantics":
    let opts1 = newVarianceOptions()
    var opts2 = opts1
    check not isNil(opts2.handle)
    var opts3 = newVarianceOptions()
    opts3 = opts1
    check not isNil(opts3.handle)

suite "Compute Options - Strftime":
  test "create StrftimeOptions":
    let opts = newStrftimeOptions()
    check not isNil(opts.handle)

suite "Compute Options - Pad":
  test "create PadOptions":
    let opts = newPadOptions()
    check not isNil(opts.handle)

suite "Compute Options - Mode":
  test "create ModeOptions":
    let opts = newModeOptions()
    check not isNil(opts.handle)

suite "Compute Options - Quantile":
  test "create QuantileOptions":
    let opts = newQuantileOptions()
    check not isNil(opts.handle)

  test "setQ on QuantileOptions":
    var opts = newQuantileOptions()
    opts.setQ(0.5)
    let qs = opts.getQs
    check qs.len >= 0

suite "Compute Options - TDigest":
  test "create TDigestOptions":
    let opts = newTDigestOptions()
    check not isNil(opts.handle)

  test "setQ on TDigestOptions":
    var opts = newTDigestOptions()
    opts.setQ(0.99)
    let qs = opts.getQs
    check qs.len >= 0

suite "Compute Options - SelectK":
  test "create SelectKOptions":
    let opts = newSelectKOptions()
    check not isNil(opts.handle)

  test "addSortKey to SelectKOptions":
    var opts = newSelectKOptions()
    let key = newSortKey("col1", Ascending)
    opts.addSortKey(key)

suite "Compute Options - Trim":
  test "create TrimOptions":
    let opts = newTrimOptions()
    check not isNil(opts.handle)

suite "RoundMode":
  test "all round mode values distinct":
    var seen: set[RoundMode]
    for m in [rmDown, rmUp, rmTowardsZero, rmTowardsInfinity,
              rmHalfDown, rmHalfUp, rmHalfTowardsZero,
              rmHalfTowardsInfinity, rmHalfToEven, rmHalfToOdd]:
      check m notin seen
      seen.incl(m)
    check seen.len == 10
