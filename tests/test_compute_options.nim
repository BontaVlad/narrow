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
