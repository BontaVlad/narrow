import criterion
import ../src/narrow
import ./config

let cfg = narrowConfig()

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

benchmark cfg:

  # ----- Boolean kernels (scalar_boolean_benchmark.cc inspired) -----
  proc benchBooleanAnd1M {.measure.} =
    let a = makeBoolArray(1_000_000)
    let b = makeBoolArray(1_000_000)
    let expr = newCallExpression("and", [col("a"), col("b")])
    discard expr

  # ----- String kernels (scalar_string_benchmark.cc inspired) -----
  proc benchStringContains100K {.measure.} =
    let arr = makeStringArray(100_000)
    let expr = strContains(col("s"), "str_")
    discard expr

  proc benchStringStartsWith100K {.measure.} =
    let arr = makeStringArray(100_000)
    let expr = startsWith(col("s"), "str")
    discard expr

  proc benchStringEndsWith100K {.measure.} =
    let arr = makeStringArray(100_000)
    let expr = endsWith(col("s"), "0")
    discard expr

  # ----- Cast kernels (scalar_cast_benchmark.cc inspired) -----
  proc benchCastInt64ToFloat64_1M {.measure.} =
    let arr = makeInt64Array(1_000_000)
    let expr = newCallExpression("cast", [col("x")])
    discard expr

  # ----- Function execution (function_benchmark.cc inspired) -----
  proc benchExecuteAddInt64_1M {.measure.} =
    let a = makeInt64Array(1_000_000)
    let b = makeInt64Array(1_000_000, 1)
    let expr = add(col("a"), col("b"))
    discard expr
