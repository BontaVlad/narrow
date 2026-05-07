import criterion
import ../src/narrow
import ../src/narrow/compute/match_substring_options
import ./config

let cfg = narrowConfig()

# Local helper to avoid a circular module dependency in the library.
proc toFunctionOptions(options: MatchSubstringOptions): FunctionOptions =
  result.handle = cast[ptr GArrowFunctionOptions](options.handle)
  if not isNil(options.handle):
    discard g_object_ref(options.handle)

# ---------------------------------------------------------------------------
# Compute kernel benchmarks — inspired by arrow/cpp/src/arrow/compute/kernels/
# ---------------------------------------------------------------------------

proc makeInt64Array(n: int, offset: int64 = 0): Array[int64] =
  var values = newSeq[int64](n)
  for i in 0 ..< n:
    values[i] = offset + i.int64
  newArray(values)

proc makeBoolArray(n: int): Array[bool] =
  var values = newSeq[bool](n)
  for i in 0 ..< n:
    values[i] = i mod 2 == 0
  newArray(values)

proc makeStringArray(n: int): Array[string] =
  var values = newSeq[string](n)
  for i in 0 ..< n:
    values[i] = "str_" & $i
  newArray(values)

# Pre-build all input data at module scope so benchmarks measure
# kernel execution, not array allocation.
let boolA = makeBoolArray(1_000_000)
let boolB = makeBoolArray(1_000_000)
let strArr = makeStringArray(100_000)
let int64A = makeInt64Array(1_000_000)
let int64B = makeInt64Array(1_000_000, 1)

benchmark cfg:

  # ----- Boolean kernels (scalar_boolean_benchmark.cc inspired) -----
  proc benchBooleanAnd1M {.measure.} =
    let result = call("and", [newDatum(boolA), newDatum(boolB)])
    blackBox(result.toArray())

  # ----- String kernels (scalar_string_benchmark.cc inspired) -----
  proc benchStringContains100K {.measure.} =
    let opts = newMatchSubstringOptions("str_")
    let result = call("match_substring", [newDatum(strArr)], options = opts.toFunctionOptions())
    blackBox(result.toArray())

  proc benchStringStartsWith100K {.measure.} =
    let opts = newMatchSubstringOptions("str")
    let result = call("starts_with", [newDatum(strArr)], options = opts.toFunctionOptions())
    blackBox(result.toArray())

  proc benchStringEndsWith100K {.measure.} =
    let opts = newMatchSubstringOptions("0")
    let result = call("ends_with", [newDatum(strArr)], options = opts.toFunctionOptions())
    blackBox(result.toArray())

  # ----- Cast kernels (scalar_cast_benchmark.cc inspired) -----
  proc benchCastInt64ToFloat64_1M {.measure.} =
    var opts = newCastOptions()
    opts.toDataType = newGType(float64)
    let result = call("cast", [newDatum(int64A)], options = opts.toFunctionOptions())
    blackBox(result.toArray())

  # ----- Function execution (function_benchmark.cc inspired) -----
  proc benchExecuteAddInt64_1M {.measure.} =
    let result = call("add", [newDatum(int64A), newDatum(int64B)])
    blackBox(result.toArray())
